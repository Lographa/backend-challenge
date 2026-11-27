import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import { finished } from 'node:stream';
import {
  type BeforeApplicationShutdown,
  Global,
  type INestApplicationContext,
  Injectable,
  Logger,
  Module,
  type OnModuleInit,
  Optional,
} from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import { AppConfig } from '../config/env';
import { INSTANCE_ID_HEADER } from '../instance/instance-identity';
import { MetricsRegistry } from '../observability/metrics';
import { assignCorrelationId } from './correlation';
import { SERVICE_UNAVAILABLE_CODE } from './domain-exception.filter';

const MAX_TEARDOWN_RESERVE_MS = 250;
const POST_TIMEOUT_EXIT_GRACE_MS = 50;
const REQUEST_TRACKED_SYMBOL = Symbol('ShutdownService.requestTracked');
const FORCED_DRAIN_ABORT_SYMBOL = Symbol('ShutdownService.forcedDrainAbort');
const ADAPTER_HOOK_INSTALLED_SYMBOL = Symbol('ShutdownService.adapterHookInstalled');

export type MonotonicClock = () => number;

export type CriticalResource = 'postgres' | 'sqs';

export type CriticalOperationKind =
  | 'worker'
  | 'http_financial'
  | 'outbox_publish'
  | 'pending_reference'
  | 'health_probe';

export type CriticalOperationState =
  | 'active'
  | 'exceeded_budget_active'
  | 'completed'
  | 'cancelled'
  | 'failed';

export interface CriticalOperationDescriptor {
  readonly kind: CriticalOperationKind;
  readonly name: string;
  readonly resources: readonly CriticalResource[];
  /** Diagnostic identifiers only (e.g. walletId, correlationId, dependency). Never include payloads or secrets. */
  readonly context?: Readonly<Record<string, string | undefined>>;
  readonly signal?: AbortSignal;
}

export interface CriticalOperationSnapshot {
  readonly kind: CriticalOperationKind;
  readonly name: string;
  readonly state: CriticalOperationState;
  readonly resources: readonly CriticalResource[];
  readonly context?: Readonly<Record<string, string>>;
  readonly durationMs: number;
  readonly errorReason?: string;
}

export interface ShutdownTelemetrySummary {
  readonly startedAt: string | null;
  readonly completedAt: string | null;
  readonly trigger: string | null;
  readonly inFlightHttpAtStart: number;
  readonly drainedHttpRequests: number;
  readonly abortedHttpRequests: number;
  readonly rejectedHttpRequests: number;
  readonly inFlightOperationsAtStart: number;
  readonly drainedCriticalOperations: number;
  readonly abortedCriticalOperations: number;
  readonly elapsedMs: number;
}

export type BoundedCallBudget =
  | { readonly exhausted: true }
  | { readonly exhausted: false; readonly budgetMs: number; readonly signal: AbortSignal; readonly dispose: () => void };

export class CooperativeBudgetExhaustedError extends Error {
  constructor(operation: string) {
    super(`Cooperative shutdown budget exhausted for ${operation}`);
    this.name = 'CooperativeBudgetExhaustedError';
  }
}

interface InternalTrackedOperation {
  readonly id: symbol;
  readonly kind: CriticalOperationKind;
  readonly name: string;
  readonly resources: ReadonlySet<CriticalResource>;
  readonly context?: Readonly<Record<string, string>>;
  readonly startedAtMono: number;
  settledAtMono?: number;
  state: CriticalOperationState;
  errorReason?: string;
  readonly promise: Promise<void>;
}

type CloseableHttpServer = Server & {
  closeIdleConnections?: () => void;
  closeAllConnections?: () => void;
};

type HookableHttpAdapter = {
  getHttpServer?: () => unknown;
  onRequestHook?: (req: IncomingMessage, res: ServerResponse, done: (err?: unknown) => void) => unknown;
  setOnRequestHook?: (
    hook: (req: IncomingMessage, res: ServerResponse, done: (err?: unknown) => void) => void,
  ) => void;
  [ADAPTER_HOOK_INSTALLED_SYMBOL]?: boolean;
};

@Injectable()
export class ShutdownService implements BeforeApplicationShutdown {
  private readonly logger = new Logger(ShutdownService.name);
  private readonly activeRequests = new Map<ServerResponse, IncomingMessage>();
  private readonly drainWaiters = new Set<() => void>();
  private readonly activeOperations = new Map<symbol, InternalTrackedOperation>();
  private readonly completedOperationsHistory: CriticalOperationSnapshot[] = [];
  private readonly operationWaiters = new Set<() => void>();
  private readonly reasons: string[] = [];
  private readonly cooperativeAbortController = new AbortController();

  private clock: MonotonicClock = () => performance.now();
  private shuttingDown = false;
  private httpDrainCompleted = false;
  private timedOut = false;
  private teardownFailed = false;
  private startedAtMono = 0;
  private cooperativeDeadlineMono = 0;
  private hardDeadlineMono = 0;
  private reserveMs = 0;
  private triggerReason: string | undefined;
  private bootstrapBarrier: Promise<unknown> | undefined;
  private closePromise: Promise<void> | undefined;
  private startedAtIso: string | null = null;
  private completedAtIso: string | null = null;
  private inFlightHttpAtStart = 0;
  private drainedHttpRequests = 0;
  private abortedHttpRequests = 0;
  private rejectedHttpRequests = 0;
  private inFlightOperationsAtStart = 0;
  private drainedCriticalOperations = 0;
  private abortedCriticalOperations = 0;

  constructor(
    private readonly config: AppConfig,
    @Optional() private readonly httpAdapterHost?: HttpAdapterHost,
    @Optional() private readonly metrics?: MetricsRegistry,
  ) {}

  /** Overrides the monotonic clock in unit/integration tests without relying on wall-clock sleeps. */
  setClockForTesting(clock: MonotonicClock): void {
    this.clock = clock;
  }

  get shutdownTimeoutMs(): number {
    return this.config.shutdownTimeoutMs;
  }

  get instanceId(): string | undefined {
    return this.config.instanceId;
  }

  get teardownReserveMs(): number {
    return this.shuttingDown ? this.reserveMs : computeTeardownReserveMs(this.config.shutdownTimeoutMs);
  }

  get cooperativeBudgetMs(): number {
    return this.config.shutdownTimeoutMs - this.teardownReserveMs;
  }

  get trigger(): string | undefined {
    return this.triggerReason;
  }

  /** Signal aborted as soon as shutdown starts, used to interrupt idle long-polls immediately. */
  get shutdownSignal(): AbortSignal {
    return this.cooperativeAbortController.signal;
  }

  reason(): string | undefined {
    return this.triggerReason;
  }

  isShuttingDown(): boolean {
    return this.shuttingDown;
  }

  /** Monotonically becomes `true` once the HTTP drain window in `beforeApplicationShutdown()` finishes. */
  isHttpDrainCompleted(): boolean {
    return this.httpDrainCompleted;
  }

  didTimeout(): boolean {
    return this.timedOut;
  }

  hasTeardownError(): boolean {
    return this.teardownFailed;
  }

  hasFailed(): boolean {
    return this.timedOut || this.teardownFailed;
  }

  failureReasons(): readonly string[] {
    return this.reasons;
  }

  inFlightHttpCount(): number {
    return this.activeRequests.size;
  }

  isRequestAdmitted(request: IncomingMessage, response: ServerResponse): boolean {
    const taggedRequest = request as IncomingMessage & { [REQUEST_TRACKED_SYMBOL]?: boolean };
    return taggedRequest[REQUEST_TRACKED_SYMBOL] === true || this.activeRequests.has(response);
  }

  activeCriticalOperationCount(resource?: CriticalResource): number {
    if (resource === undefined) {
      return this.activeOperations.size;
    }
    let count = 0;
    for (const op of this.activeOperations.values()) {
      if (op.resources.has(resource)) {
        count += 1;
      }
    }
    return count;
  }

  activeOperationSnapshots(): CriticalOperationSnapshot[] {
    const now = this.clock();
    const snapshots: CriticalOperationSnapshot[] = [];
    for (const op of this.activeOperations.values()) {
      snapshots.push(toSnapshot(op, now));
    }
    return snapshots;
  }

  recentSettledOperationSnapshots(): readonly CriticalOperationSnapshot[] {
    return this.completedOperationsHistory;
  }

  elapsedMs(): number {
    if (!this.shuttingDown) {
      return 0;
    }
    return Math.max(0, Math.round(this.clock() - this.startedAtMono));
  }

  remainingCooperativeMs(): number {
    if (!this.shuttingDown) {
      return this.config.shutdownTimeoutMs - computeTeardownReserveMs(this.config.shutdownTimeoutMs);
    }
    return Math.max(0, Math.floor(this.cooperativeDeadlineMono - this.clock()));
  }

  remainingHardMs(): number {
    if (!this.shuttingDown) {
      return this.config.shutdownTimeoutMs;
    }
    return Math.max(0, Math.ceil(this.hardDeadlineMono - this.clock()));
  }

  /**
   * Registers an in-flight bootstrap/listen Promise so any signal arriving during `app.init()` or
   * `app.listen()` waits for that startup phase to settle before invoking `app.close()`.
   */
  setBootstrapBarrier(barrier: Promise<unknown> | undefined): void {
    this.bootstrapBarrier = barrier;
  }

  /**
   * Idempotently starts the global shutdown clock. Subsequent calls preserve the original
   * monotonic deadlines so worker teardown and HTTP draining never sum timeouts.
   */
  beginShutdown(reason = 'shutdown'): boolean {
    if (this.shuttingDown) {
      return false;
    }

    const now = this.clock();
    this.shuttingDown = true;
    this.triggerReason = reason;
    this.startedAtIso = new Date().toISOString();
    this.inFlightHttpAtStart = this.activeRequests.size;
    this.inFlightOperationsAtStart = this.activeOperations.size;
    this.reserveMs = computeTeardownReserveMs(this.config.shutdownTimeoutMs);
    this.startedAtMono = now;
    this.hardDeadlineMono = now + this.config.shutdownTimeoutMs;
    this.cooperativeDeadlineMono = this.hardDeadlineMono - this.reserveMs;

    if (!this.cooperativeAbortController.signal.aborted) {
      this.cooperativeAbortController.abort(new CooperativeBudgetExhaustedError(reason));
    }

    for (const [res] of this.activeRequests) {
      markResponseConnectionClose(res);
    }
    this.closeIdleHttpConnections();
    try {
      this.metrics?.stopPeriodicSnapshot();
    } catch {
      // Stopping the snapshot timer must not change shutdown deadlines or in-flight counters.
    }
    return true;
  }

  /** Last in-memory sample after the drain. Does not query the database and does not reset counters. */
  emitFinalMetricsSnapshot(): void {
    try {
      this.metrics?.emitFinalSnapshot();
    } catch {
      // The final snapshot must not change the shutdown exit path.
    }
  }

  recordRejectedHttpRequest(): void {
    if (this.shuttingDown) {
      this.rejectedHttpRequests += 1;
    }
  }

  markShutdownCompleted(): void {
    if (this.completedAtIso === null) {
      this.completedAtIso = new Date().toISOString();
    }
  }

  shutdownSummary(): ShutdownTelemetrySummary {
    return {
      startedAt: this.startedAtIso,
      completedAt: this.completedAtIso,
      trigger: this.triggerReason ?? null,
      inFlightHttpAtStart: this.inFlightHttpAtStart,
      drainedHttpRequests: this.drainedHttpRequests,
      abortedHttpRequests: this.abortedHttpRequests,
      rejectedHttpRequests: this.rejectedHttpRequests,
      inFlightOperationsAtStart: this.inFlightOperationsAtStart,
      drainedCriticalOperations: this.drainedCriticalOperations,
      abortedCriticalOperations: this.abortedCriticalOperations,
      elapsedMs: this.elapsedMs(),
    };
  }

  /**
   * Shares a single `app.close()` Promise across concurrent signals or programmatic callers
   * while preserving `beginShutdown()` idempotency and waiting for any active bootstrap barrier.
   */
  closeApplication(app: Pick<INestApplicationContext, 'close'>, reason = 'shutdown'): Promise<void> {
    this.beginShutdown(reason);
    if (this.closePromise === undefined) {
      const barrier = this.bootstrapBarrier;
      this.closePromise = (async () => {
        if (barrier !== undefined) {
          await barrier.catch(() => undefined);
        }
        await app.close();
      })().catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        this.markTeardownError(`Application close failed: ${message}`);
        throw error;
      });
    }
    return this.closePromise;
  }

  markTimedOut(reason: string): void {
    this.timedOut = true;
    this.reasons.push(reason);
  }

  markTeardownError(reason: string): void {
    this.teardownFailed = true;
    this.reasons.push(reason);
  }

  /**
   * Creates an `AbortSignal` for auxiliary SQS calls bounded by `defaultTimeoutMs` in normal
   * operation and strictly capped by `remainingCooperativeMs()` during shutdown. Never creates
   * a 0ms signal when the cooperative budget is already exhausted.
   */
  acquireSqsCallBudget(defaultTimeoutMs: number, operation: string): BoundedCallBudget {
    if (!this.shuttingDown) {
      return {
        exhausted: false,
        budgetMs: defaultTimeoutMs,
        signal: AbortSignal.timeout(defaultTimeoutMs),
        dispose: () => undefined,
      };
    }

    const budgetMs = Math.min(defaultTimeoutMs, this.remainingCooperativeMs());
    if (budgetMs <= 0) {
      return { exhausted: true };
    }

    const controller = new AbortController();
    const timer = setTimeout(() => {
      controller.abort(new CooperativeBudgetExhaustedError(operation));
    }, budgetMs);
    return {
      exhausted: false,
      budgetMs,
      signal: controller.signal,
      dispose: () => {
        clearTimeout(timer);
      },
    };
  }

  /**
   * Tracks the real Promise of a critical operation (`postgres` and/or `sqs`) until it genuinely
   * settles. The entry is removed ONLY when `work()` resolves or rejects, preventing resource
   * teardown (`orm.close()` / `sqs.destroy()`) from running underneath active operations.
   */
  trackCriticalOperation<T>(descriptor: CriticalOperationDescriptor, work: () => Promise<T>): Promise<T> {
    const id = Symbol(descriptor.name);
    const sanitizedContext = sanitizeDiagnosticContext(descriptor.context);
    const startedAtMono = this.clock();

    let settleTracking!: (outcome: 'completed' | 'cancelled' | 'failed', errorReason?: string) => void;
    const settlementPromise = new Promise<void>((resolve) => {
      let settledOnce = false;
      settleTracking = (outcome, errorReason) => {
        if (settledOnce) {
          return;
        }
        settledOnce = true;
        const finishedAt = this.clock();
        entry.settledAtMono = finishedAt;
        entry.state = outcome;
        if (errorReason !== undefined) {
          entry.errorReason = errorReason;
        }
        if (this.shuttingDown) {
          if (outcome === 'completed') {
            this.drainedCriticalOperations += 1;
          } else {
            this.abortedCriticalOperations += 1;
          }
        }
        this.activeOperations.delete(id);
        this.recordSettledSnapshot(toSnapshot(entry, finishedAt));
        resolve();
        this.notifyOperationWaiters();
      };
    });

    const entry: InternalTrackedOperation = {
      id,
      kind: descriptor.kind,
      name: descriptor.name,
      resources: new Set(descriptor.resources),
      ...(sanitizedContext !== undefined ? { context: sanitizedContext } : {}),
      startedAtMono,
      state: 'active',
      promise: settlementPromise,
    };

    this.activeOperations.set(id, entry);

    return Promise.resolve()
      .then(() => work())
      .then(
        (value) => {
          settleTracking('completed');
          return value;
        },
        (error: unknown) => {
          const reason = error instanceof Error ? error.message : String(error);
          const wasCancelled =
            error instanceof CooperativeBudgetExhaustedError ||
            isAbortError(error) ||
            (descriptor.signal?.aborted === true && error === descriptor.signal.reason);
          settleTracking(wasCancelled ? 'cancelled' : 'failed', reason);
          throw error;
        },
      );
  }

  /**
   * Resolves only when zero tracked critical operations currently depend on `resource`
   * (or all resources when omitted). Never uses `Promise.race` to abandon in-flight work.
   */
  async waitForResourceIdle(resource?: CriticalResource): Promise<void> {
    while (this.activeCriticalOperationCount(resource) > 0) {
      await new Promise<void>((resolve) => {
        this.operationWaiters.add(resolve);
      });
    }
  }

  /**
   * Executes a worker stop routine within the shared cooperative deadline while keeping the real
   * `stopWork` Promise registered in `activeOperations` if it exceeds the cooperative budget,
   * so `beforeApplicationShutdown()` / resource hooks will NOT close `orm` or `sqs` underneath it.
   */
  async runWithinCooperativeBudget(
    component: string,
    stopWork: () => Promise<void>,
    resources: readonly CriticalResource[] = ['postgres', 'sqs'],
  ): Promise<void> {
    this.beginShutdown(`onModuleDestroy:${component}`);

    let settled = false;
    const trackedWork = this.trackCriticalOperation(
      {
        kind: 'worker',
        name: `${component}.stop`,
        resources,
        context: { component },
      },
      async () => {
        try {
          await stopWork();
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          this.markTeardownError(`${component} stop failed: ${message}`);
          throw error;
        } finally {
          settled = true;
        }
      },
    );

    const budgetMs = this.remainingCooperativeMs();
    if (budgetMs <= 0) {
      // Yield one microtask in case stopWork() settles immediately (e.g. idle worker).
      await Promise.resolve();
      await Promise.resolve();
      if (settled) {
        await trackedWork;
        return;
      }
      trackedWork.catch(() => undefined);
      this.markRemainingOperationsExceeded();
      const reason = `${component} had 0ms remaining in cooperative shutdown budget`;
      this.markTimedOut(reason);
      this.logger.error({
        message: 'Worker stop exceeded cooperative budget (0ms left); deferring resource teardown until settlement or hard deadline',
        component,
        elapsedMs: this.elapsedMs(),
      });
      return;
    }

    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeoutOutcome = new Promise<'timed_out'>((resolve) => {
      timer = setTimeout(() => resolve('timed_out'), budgetMs);
    });

    try {
      const winner = await Promise.race([trackedWork.then(() => 'completed' as const), timeoutOutcome]);
      if (winner === 'timed_out' && !settled) {
        trackedWork.catch(() => undefined);
        this.markRemainingOperationsExceeded();
        const reason = `${component} exceeded cooperative shutdown budget (${budgetMs}ms)`;
        this.markTimedOut(reason);
        this.logger.error({
          message: 'Worker stop exceeded cooperative shutdown budget; deferring resource teardown until settlement or hard deadline',
          component,
          budgetMs,
          elapsedMs: this.elapsedMs(),
        });
      }
    } finally {
      clearTimeout(timer);
    }
  }

  trackRequest(request: IncomingMessage, response: ServerResponse): void {
    const taggedRequest = request as IncomingMessage & { [REQUEST_TRACKED_SYMBOL]?: boolean };
    if (taggedRequest[REQUEST_TRACKED_SYMBOL] === true || this.activeRequests.has(response)) {
      return;
    }

    if (
      response.writableFinished ||
      response.writableEnded ||
      response.destroyed ||
      request.socket?.destroyed === true
    ) {
      return;
    }

    taggedRequest[REQUEST_TRACKED_SYMBOL] = true;

    if (this.shuttingDown) {
      markResponseConnectionClose(response);
    }

    this.activeRequests.set(response, request);

    let settled = false;
    const socket = request.socket;
    const settle = (err?: Error | null): void => {
      if (settled) {
        return;
      }
      settled = true;
      disposeFinished();
      if (socket !== undefined && typeof socket.removeListener === 'function') {
        socket.removeListener('close', onSocketClose);
      }
      if (this.shuttingDown) {
        const forcedAbort =
          (response as ServerResponse & { [FORCED_DRAIN_ABORT_SYMBOL]?: boolean })[FORCED_DRAIN_ABORT_SYMBOL] ===
          true;
        if (!forcedAbort && (err === undefined || err === null) && response.writableFinished) {
          this.drainedHttpRequests += 1;
        } else {
          this.abortedHttpRequests += 1;
        }
      }
      this.activeRequests.delete(response);
      if (this.activeRequests.size === 0) {
        this.notifyDrained();
      }
    };

    const onSocketClose = (): void => {
      settle();
    };

    const disposeFinished = finished(response, (err) => settle(err));
    if (socket !== undefined && typeof socket.once === 'function') {
      if (socket.destroyed) {
        settle();
        return;
      }
      socket.once('close', onSocketClose);
    }

    if (response.writableFinished || response.destroyed) {
      settle();
    }
  }

  async beforeApplicationShutdown(): Promise<void> {
    this.beginShutdown('beforeApplicationShutdown');
    this.closeIdleHttpConnections();

    if (this.activeRequests.size > 0) {
      const httpBudgetMs = this.remainingCooperativeMs();
      if (httpBudgetMs > 0) {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const deadlineReached = new Promise<void>((resolve) => {
          timer = setTimeout(resolve, httpBudgetMs);
        });
        try {
          await Promise.race([this.whenHttpDrained(), deadlineReached]);
        } finally {
          clearTimeout(timer);
        }
      }
    }

    // F4: Once the HTTP drain phase finishes, prohibit any late /health/ready from starting new probes.
    this.httpDrainCompleted = true;

    if (this.activeRequests.size === 0) {
      this.closeIdleHttpConnections();
    } else {
      const remainingHttpCount = this.activeRequests.size;
      this.markTimedOut(`HTTP drain timed out with ${remainingHttpCount} active request(s)`);
      this.logger.error({
        message: 'HTTP drain exceeded cooperative shutdown budget; destroying remaining connections',
        inFlightHttp: remainingHttpCount,
        elapsedMs: this.elapsedMs(),
      });
      this.destroyRemainingRequests();
      this.forceCloseAllHttpConnections();
    }

    // F1 & F3: Wait for any in-flight critical operations (workers, HTTP financial TXs after client
    // disconnect/timeout, outbox batches, pending-reference batches, PostgreSQL/SQS probes).
    if (this.activeOperations.size > 0) {
      const opCooperativeBudgetMs = this.remainingCooperativeMs();
      if (opCooperativeBudgetMs > 0) {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const deadlineReached = new Promise<void>((resolve) => {
          timer = setTimeout(resolve, opCooperativeBudgetMs);
        });
        try {
          await Promise.race([this.waitForResourceIdle(), deadlineReached]);
        } finally {
          clearTimeout(timer);
        }
      }

      if (this.activeOperations.size > 0) {
        this.markRemainingOperationsExceeded();
        const activeSnapshots = this.activeOperationSnapshots();
        this.markTimedOut(
          `${activeSnapshots.length} critical operation(s) still active past cooperative shutdown deadline: ` +
            activeSnapshots.map((op) => `${op.name}[${op.kind}]`).join(', '),
        );
        this.logger.error({
          message:
            'Critical operations remain active past cooperative shutdown budget; holding resource teardown until settlement or hard deadline',
          activeOperations: activeSnapshots,
          elapsedMs: this.elapsedMs(),
        });

        // Intentionally NO Promise.race here: never release NestJS into sqs.destroy() / orm.close()
        // while critical operations still depend on those resources. If they never settle, the active
        // ref'd hard-deadline watchdog terminates the process with exit code 1.
        await this.waitForResourceIdle();
      }
    }
  }

  /**
   * F5: Installs correlation + drain + request tracking at slot #0 of ExpressAdapter (`onRequestHook`),
   * strictly ahead of `express.json()` and `MikroOrmMiddleware` (`RequestContext.create`).
   */
  installAdapterRequestHook(): void {
    const adapter = this.httpAdapterHost?.httpAdapter as HookableHttpAdapter | null | undefined;
    if (adapter === undefined || adapter === null || typeof adapter.setOnRequestHook !== 'function') {
      return;
    }
    if (adapter[ADAPTER_HOOK_INSTALLED_SYMBOL] === true) {
      return;
    }
    adapter[ADAPTER_HOOK_INSTALLED_SYMBOL] = true;

    const previousHook = adapter.onRequestHook;
    const drainMiddleware = createHttpDrainMiddleware(this);

    adapter.setOnRequestHook((request, response, done) => {
      assignCorrelationId(request, response);
      assignInstanceIdHeader(response, this.config.instanceId);
      drainMiddleware(request, response, () => {
        if (typeof previousHook === 'function') {
          previousHook.call(adapter, request, response, done);
          return;
        }
        done();
      });
    });
  }

  private markRemainingOperationsExceeded(): void {
    for (const op of this.activeOperations.values()) {
      if (op.state === 'active') {
        op.state = 'exceeded_budget_active';
      }
    }
  }

  private recordSettledSnapshot(snapshot: CriticalOperationSnapshot): void {
    this.completedOperationsHistory.push(snapshot);
    if (this.completedOperationsHistory.length > 64) {
      this.completedOperationsHistory.shift();
    }
  }

  private whenHttpDrained(): Promise<void> {
    if (this.activeRequests.size === 0) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.drainWaiters.add(resolve);
    });
  }

  private notifyDrained(): void {
    if (this.drainWaiters.size === 0) {
      return;
    }
    const waiters = [...this.drainWaiters];
    this.drainWaiters.clear();
    for (const wake of waiters) {
      wake();
    }
  }

  private notifyOperationWaiters(): void {
    if (this.operationWaiters.size === 0) {
      return;
    }
    const waiters = [...this.operationWaiters];
    this.operationWaiters.clear();
    for (const wake of waiters) {
      wake();
    }
  }

  private destroyRemainingRequests(): void {
    for (const [response, request] of [...this.activeRequests.entries()]) {
      (response as ServerResponse & { [FORCED_DRAIN_ABORT_SYMBOL]?: boolean })[FORCED_DRAIN_ABORT_SYMBOL] =
        true;

      if (!response.headersSent && !response.writableEnded && !response.destroyed) {
        try {
          const correlationId = assignCorrelationId(request, response);
          response.statusCode = 503;
          markResponseConnectionClose(response);
          response.setHeader('Content-Type', 'application/json; charset=utf-8');
          const socket = request.socket as (IncomingMessage['socket'] & { destroySoon?: () => void }) | undefined;
          if (socket !== undefined) {
            response.once('finish', () => {
              if (typeof socket.destroySoon === 'function') {
                socket.destroySoon();
              } else {
                socket.destroy();
              }
            });
          }
          response.end(
            JSON.stringify({
              code: SERVICE_UNAVAILABLE_CODE,
              message: 'Service is shutting down; retry later',
              correlationId,
            }),
          );
          this.activeRequests.delete(response);
          continue;
        } catch {
          // Fall through to hard socket destruction if writing 503 fails.
        }
      }

      try {
        request.socket?.destroy();
      } catch {
        // Ignore destruction errors on already-closed sockets.
      }
      try {
        response.destroy();
      } catch {
        // Ignore destruction errors on half-closed streams.
      }
      this.activeRequests.delete(response);
    }
    if (this.activeRequests.size === 0) {
      this.notifyDrained();
    }
  }

  private closeIdleHttpConnections(): void {
    const server = this.resolveHttpServer();
    server?.closeIdleConnections?.();
  }

  private forceCloseAllHttpConnections(): void {
    const server = this.resolveHttpServer();
    server?.closeAllConnections?.();
  }

  private resolveHttpServer(): CloseableHttpServer | undefined {
    const candidate = this.httpAdapterHost?.httpAdapter?.getHttpServer?.() as
      | CloseableHttpServer
      | null
      | undefined;
    if (candidate !== undefined && candidate !== null && typeof candidate === 'object') {
      return candidate;
    }
    return undefined;
  }
}

export function createHttpDrainMiddleware(shutdown: ShutdownService) {
  return (request: IncomingMessage, response: ServerResponse, next: () => void): void => {
    assignInstanceIdHeader(response, shutdown.instanceId);
    const rawUrl = (request as IncomingMessage & { originalUrl?: string }).originalUrl ?? request.url;
    const path = normalizeRequestPath(rawUrl);
    if (isHealthEndpointPath(path)) {
      if (shutdown.isShuttingDown()) {
        markResponseConnectionClose(response);
      }
      shutdown.trackRequest(request, response);
      next();
      return;
    }

    if (shutdown.isRequestAdmitted(request, response)) {
      if (shutdown.isShuttingDown()) {
        markResponseConnectionClose(response);
      }
      next();
      return;
    }

    if (shutdown.isShuttingDown()) {
      shutdown.recordRejectedHttpRequest();
      const correlationId = assignCorrelationId(request, response);
      if (response.writableEnded || response.destroyed || request.socket?.destroyed === true) {
        return;
      }
      if (!response.headersSent) {
        response.statusCode = 503;
        markResponseConnectionClose(response);
        response.setHeader('Content-Type', 'application/json; charset=utf-8');
      }
      // If the client was streaming an unread request body (e.g. slow POST upload during shutdown),
      // close the write side cleanly once the 503 payload flushes instead of waiting for body upload.
      const socket = request.socket as (IncomingMessage['socket'] & { destroySoon?: () => void }) | undefined;
      if (request.complete !== true && socket !== undefined) {
        response.once('finish', () => {
          if (typeof socket.destroySoon === 'function') {
            socket.destroySoon();
          } else {
            socket.destroy();
          }
        });
      }
      response.end(
        JSON.stringify({
          code: SERVICE_UNAVAILABLE_CODE,
          message: 'Service is shutting down; retry later',
          correlationId,
        }),
      );
      return;
    }

    shutdown.trackRequest(request, response);
    next();
  };
}

export interface ProcessShutdownHandlerOptions {
  readonly app: Pick<INestApplicationContext, 'close' | 'flushLogs'>;
  readonly shutdown: ShutdownService;
  readonly logger: Logger;
  readonly onExit?: (code: number) => void;
}

export interface ProcessShutdownController {
  readonly detach: () => void;
  readonly triggerSignal: (signal: 'SIGTERM' | 'SIGINT') => Promise<void>;
  readonly shutdownAfterBootstrapFailure: (error: unknown) => Promise<void>;
}

/**
 * Installs SIGTERM/SIGINT handlers backed by a single shared close Promise and an active (ref'd)
 * watchdog timer. Duplicate signals are absorbed while draining so OS default signal handlers
 * never kill an in-flight financial commit, and the normal path lets the event loop exit
 * naturally without calling `process.exit(0)`.
 */
export function createProcessShutdownController(options: ProcessShutdownHandlerOptions): ProcessShutdownController {
  const { app, shutdown, logger } = options;
  const exitProcess = options.onExit ?? ((code: number) => process.exit(code));
  let activeShutdown: Promise<void> | undefined;
  let watchdog: ReturnType<typeof setTimeout> | undefined;

  const ensureWatchdog = (triggerLabel: string): void => {
    if (watchdog !== undefined) {
      return;
    }
    const watchdogDelayMs = Math.max(1, shutdown.remainingHardMs());
    // Intentionally NOT .unref()'d: keeps the event loop alive across httpServer.close() -> orm.close()
    // and guarantees non-zero exit if teardown or critical operations hang past the hard deadline.
    watchdog = setTimeout(() => {
      shutdown.markTimedOut(`Hard shutdown timeout (${shutdown.shutdownTimeoutMs}ms) exceeded`);
      shutdown.markShutdownCompleted();
      if (options.onExit === undefined) {
        process.exitCode = 1;
      }
      logger.error({
        message: 'Graceful shutdown timed out; forcing exit',
        trigger: triggerLabel,
        timeoutMs: shutdown.shutdownTimeoutMs,
        inFlightHttp: shutdown.inFlightHttpCount(),
        activeOperations: shutdown.activeOperationSnapshots(),
        shutdownSummary: shutdown.shutdownSummary(),
      });
      app.flushLogs();
      // Hard timeout exits before the coordinated finally, so there is no final metrics snapshot.
      exitProcess(1);
    }, watchdogDelayMs);
  };

  const executeCoordinatedShutdown = (triggerLabel: string): Promise<void> => {
    if (activeShutdown !== undefined) {
      return activeShutdown;
    }

    shutdown.beginShutdown(triggerLabel);
    ensureWatchdog(triggerLabel);

    activeShutdown = shutdown
      .closeApplication(app, triggerLabel)
      .catch((error: unknown) => {
        const reason = error instanceof Error ? error.message : String(error);
        shutdown.markTeardownError(`Unhandled shutdown error: ${reason}`);
        if (options.onExit === undefined) {
          process.exitCode = 1;
        }
        logger.error({
          message: 'Error during graceful shutdown',
          trigger: triggerLabel,
          reason,
        });
      })
      .finally(() => {
        if (watchdog !== undefined) {
          clearTimeout(watchdog);
          watchdog = undefined;
        }
        detachListeners();
        shutdown.markShutdownCompleted();
        shutdown.emitFinalMetricsSnapshot();

        if (shutdown.hasFailed()) {
          if (options.onExit === undefined) {
            process.exitCode = 1;
          }
          logger.error({
            message: 'Shutdown completed with timeout or teardown failure',
            trigger: triggerLabel,
            reasons: shutdown.failureReasons(),
            elapsedMs: shutdown.elapsedMs(),
            shutdownSummary: shutdown.shutdownSummary(),
          });
          app.flushLogs();
          if (options.onExit !== undefined) {
            options.onExit(1);
          } else {
            // F6: Referenced (NOT .unref()'d) so post-failure exit is deterministic after log flush.
            setTimeout(() => {
              process.exit(1);
            }, POST_TIMEOUT_EXIT_GRACE_MS);
          }
          return;
        }

        logger.log({
          message: 'Graceful shutdown completed',
          signal: triggerLabel,
          elapsedMs: shutdown.elapsedMs(),
          shutdownSummary: shutdown.shutdownSummary(),
        });
        app.flushLogs();
      });

    return activeShutdown;
  };

  const handleSignal = (signal: 'SIGTERM' | 'SIGINT'): Promise<void> => {
    if (activeShutdown !== undefined) {
      logger.warn({
        message: 'Duplicate shutdown signal received while shutdown is already in progress',
        signal,
        elapsedMs: shutdown.elapsedMs(),
      });
      return activeShutdown;
    }

    logger.log({
      message: 'Shutdown signal received',
      signal,
      timeoutMs: shutdown.shutdownTimeoutMs,
      inFlightHttp: shutdown.inFlightHttpCount(),
      inFlightOperations: shutdown.activeCriticalOperationCount(),
      startedAt: new Date().toISOString(),
    });
    return executeCoordinatedShutdown(signal);
  };

  const shutdownAfterBootstrapFailure = (error: unknown): Promise<void> => {
    const reason = error instanceof Error ? error.message : String(error);
    const code = (error as { code?: unknown })?.code;
    shutdown.markTeardownError(`Bootstrap failure: ${reason}`);
    if (options.onExit === undefined) {
      process.exitCode = 1;
    }
    logger.error({
      message: 'Application bootstrap failed; tearing down initialized resources',
      reason,
      ...(typeof code === 'string' ? { code } : {}),
    });
    return executeCoordinatedShutdown('bootstrap_failure');
  };

  const onSigterm = (): void => {
    void handleSignal('SIGTERM');
  };
  const onSigint = (): void => {
    void handleSignal('SIGINT');
  };

  const detachListeners = (): void => {
    process.off('SIGTERM', onSigterm);
    process.off('SIGINT', onSigint);
  };

  process.on('SIGTERM', onSigterm);
  process.on('SIGINT', onSigint);

  return {
    detach: detachListeners,
    triggerSignal: handleSignal,
    shutdownAfterBootstrapFailure,
  };
}

export function installProcessShutdownHandlers(options: ProcessShutdownHandlerOptions): () => void {
  return createProcessShutdownController(options).detach;
}

@Global()
@Module({
  providers: [ShutdownService],
  exports: [ShutdownService],
})
export class ShutdownModule implements OnModuleInit {
  constructor(private readonly shutdown: ShutdownService) {}

  onModuleInit(): void {
    this.shutdown.installAdapterRequestHook();
  }
}

function computeTeardownReserveMs(shutdownTimeoutMs: number): number {
  return Math.min(MAX_TEARDOWN_RESERVE_MS, Math.floor(shutdownTimeoutMs * 0.1));
}

function normalizeRequestPath(rawUrl: string | undefined): string {
  const withoutQuery = (rawUrl ?? '/').split('?')[0] ?? '/';
  if (withoutQuery.length > 1 && withoutQuery.endsWith('/')) {
    return withoutQuery.slice(0, -1);
  }
  return withoutQuery;
}

function isHealthEndpointPath(path: string): boolean {
  return path === '/health/live' || path === '/health/ready';
}

function markResponseConnectionClose(response: ServerResponse): void {
  if (!response.headersSent && !response.destroyed) {
    response.setHeader('Connection', 'close');
  }
}

function assignInstanceIdHeader(response: ServerResponse, instanceId: string | undefined): void {
  if (instanceId !== undefined && instanceId.length > 0 && !response.headersSent && !response.destroyed) {
    response.setHeader(INSTANCE_ID_HEADER, instanceId);
  }
}

function sanitizeDiagnosticContext(
  raw: Readonly<Record<string, string | undefined>> | undefined,
): Readonly<Record<string, string>> | undefined {
  if (raw === undefined) {
    return undefined;
  }
  const clean: Record<string, string> = {};
  let hasEntry = false;
  for (const [key, value] of Object.entries(raw)) {
    if (typeof value === 'string' && value.length > 0) {
      clean[key] = value.slice(0, 128);
      hasEntry = true;
    }
  }
  return hasEntry ? Object.freeze(clean) : undefined;
}

function toSnapshot(op: InternalTrackedOperation, nowMono: number): CriticalOperationSnapshot {
  const endMono = op.settledAtMono ?? nowMono;
  return {
    kind: op.kind,
    name: op.name,
    state: op.state,
    resources: [...op.resources],
    ...(op.context !== undefined ? { context: op.context } : {}),
    durationMs: Math.max(0, Math.round(endMono - op.startedAtMono)),
    ...(op.errorReason !== undefined ? { errorReason: op.errorReason } : {}),
  };
}

function isAbortError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  return error.name === 'AbortError' || error.name === 'TimeoutError';
}
