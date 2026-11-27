import 'reflect-metadata';
import { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Socket } from 'node:net';
import { Writable } from 'node:stream';
import { afterEach, describe, expect, test } from 'bun:test';
import { DeadlockException, DriverException } from '@mikro-orm/core';
import { type ExecutionContext, type INestApplication, Logger } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { AuthGuard, IS_PUBLIC_KEY, Public } from '../../src/common/auth/auth.guard';
import { AppConfig } from '../../src/common/config/env';
import { DomainError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { isTransientInfrastructureError } from '../../src/common/errors/transient-error';
import { HealthController } from '../../src/common/health/health.controller';
import { assignCorrelationId, CORRELATION_ID_HEADER } from '../../src/common/http/correlation';
import { errorResponse, HTTP_STATUS_BY_FAILURE_CODE } from '../../src/common/http/domain-exception.filter';
import {
  createHttpDrainMiddleware,
  installProcessShutdownHandlers,
  ShutdownService,
} from '../../src/common/http/shutdown.service';
import { isUuid } from '../../src/common/ids';

const EXPECTED_STATUS: Record<FailureCode, number> = {
  INVALID_PAYLOAD: 400,
  INVALID_AMOUNT: 400,
  INVALID_CURRENCY: 400,
  UNSUPPORTED_KIND: 400,
  REFERENCE_REQUIRED: 400,
  REFERENCE_NOT_ALLOWED: 400,
  WALLET_NOT_FOUND: 404,
  TRANSACTION_NOT_FOUND: 404,
  IDEMPOTENCY_PAYLOAD_CONFLICT: 409,
  WALLET_ALREADY_EXISTS: 409,
  INSUFFICIENT_FUNDS: 422,
  NEGATIVE_BALANCE_REVERSAL: 422,
  AMOUNT_OUT_OF_RANGE: 422,
  CURRENCY_MISMATCH: 422,
  WALLET_PLAYER_MISMATCH: 422,
  REFERENCE_NOT_FOUND: 422,
  REFERENCE_NOT_PROCESSED: 422,
  REFERENCE_KIND_NOT_ALLOWED: 422,
  REFERENCE_SCOPE_MISMATCH: 422,
  REFERENCE_ALREADY_REVERSED: 422,
  AMOUNT_MISMATCH: 422,
  PERMANENT_INTEGRITY_ERROR: 500,
};

function driverError(code: string): DriverException {
  return new DriverException(Object.assign(new Error(`insert into wallets values ('secret') - ${code}`), { code }));
}

describe('FailureCode to HTTP status', () => {
  test('every failure code has a status', () => {
    expect(Object.keys(HTTP_STATUS_BY_FAILURE_CODE).sort()).toEqual(Object.values(FailureCode).sort());
  });

  test.each(Object.entries(EXPECTED_STATUS))('%s -> %d', (code, status) => {
    const error = new DomainError(code as FailureCode, `because ${code}`);
    expect(errorResponse(error, 'corr-1')).toEqual({
      status,
      body: { code, message: `because ${code}`, correlationId: 'corr-1' },
    });
  });
});

describe('infrastructure errors', () => {
  test.each([
    ['connection failure', driverError('08006')],
    ['too many connections', driverError('53300')],
    ['server shutting down', driverError('57P01')],
    ['statement timeout', driverError('57014')],
    ['lock not available', driverError('55P03')],
    ['deadlock', new DeadlockException(Object.assign(new Error('deadlock detected'), { code: '40P01' }))],
    ['serialization failure', driverError('40001')],
    ['refused socket', Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' })],
    ['refused socket as a cause', new Error('query failed', { cause: Object.assign(new Error('x'), { code: 'ECONNRESET' }) })],
  ])('%s is transient: 503 without internal details', (_case, error) => {
    expect(isTransientInfrastructureError(error)).toBe(true);
    const { status, body } = errorResponse(error, 'corr-2');
    expect(status).toBe(503);
    expect(body).toEqual({
      code: 'SERVICE_UNAVAILABLE',
      message: 'A dependency is temporarily unavailable; retry later',
      correlationId: 'corr-2',
    });
  });

  test.each([
    ['unique violation', driverError('23505')],
    ['permission denied', driverError('42501')],
    ['check violation', driverError('23514')],
    ['a bug', new TypeError('cannot read properties of undefined')],
    ['a thrown string', 'boom'],
  ])('%s is not transient: 500 with a generic message', (_case, error) => {
    expect(isTransientInfrastructureError(error)).toBe(false);
    expect(errorResponse(error, 'corr-3')).toEqual({
      status: 500,
      body: { code: 'INTERNAL_ERROR', message: 'Internal server error', correlationId: 'corr-3' },
    });
  });
});

describe('correlation id', () => {
  function exchange(header?: string | string[]) {
    const request = { headers: header === undefined ? {} : { [CORRELATION_ID_HEADER]: header } } as IncomingMessage;
    const headers: Record<string, string> = {};
    const response = {
      headersSent: false,
      setHeader: (name: string, value: string) => {
        headers[name] = value;
      },
    } as unknown as ServerResponse;
    return { request, response, headers };
  }

  test('keeps a well-formed caller id and echoes it', () => {
    const { request, response, headers } = exchange('provider-a:req.123_X');
    expect(assignCorrelationId(request, response)).toBe('provider-a:req.123_X');
    expect(headers[CORRELATION_ID_HEADER]).toBe('provider-a:req.123_X');
  });

  test.each([
    ['missing', undefined],
    ['empty', ''],
    ['too long', 'a'.repeat(129)],
    ['with spaces', 'a b'],
    ['with a line break', 'a\nb'],
    ['with markup', '<script>'],
    ['repeated', ['a', 'b']],
  ])('replaces a %s id with a new UUID', (_case, header) => {
    const { request, response, headers } = exchange(header);
    const id = assignCorrelationId(request, response);
    expect(isUuid(id)).toBe(true);
    expect(headers[CORRELATION_ID_HEADER]).toBe(id);
  });

  test('is assigned once per request', () => {
    const { request, response } = exchange();
    expect(assignCorrelationId(request, response)).toBe(assignCorrelationId(request, response));
  });
});

describe('AuthGuard', () => {
  const guard = new AuthGuard(new Reflector());
  const context = (target: object) =>
    ({ getHandler: () => () => undefined, getClass: () => target }) as unknown as ExecutionContext;

  test('health checks are public', () => {
    expect(Reflect.getMetadata(IS_PUBLIC_KEY, HealthController)).toBe(true);
  });

  test('is a no-op: public and protected routes are both allowed', () => {
    @Public()
    class OpenController {}
    class ProtectedController {}

    expect(guard.canActivate(context(OpenController))).toBe(true);
    expect(guard.canActivate(context(ProtectedController))).toBe(true);
  });
});

function createShutdownService(shutdownTimeoutMs = 1000): ShutdownService {
  return new ShutdownService({ shutdownTimeoutMs } as AppConfig);
}

class FakeSocket extends EventEmitter {
  destroyed = false;

  destroy(): this {
    if (!this.destroyed) {
      this.destroyed = true;
      this.emit('close');
    }
    return this;
  }
}

function createFakeHttpExchange(url = '/wallets') {
  const socket = new FakeSocket();
  const req = {
    url,
    headers: {},
    socket: socket as unknown as Socket,
  } as unknown as IncomingMessage;

  let body = '';
  const writable = new Writable({
    write(chunk, _encoding, callback) {
      body += Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk);
      callback();
    },
  });
  const originalEnd = writable.end.bind(writable);
  const headers: Record<string, string> = {};

  const res = Object.assign(writable, {
    socket: socket as unknown as Socket,
    statusCode: 200,
    headersSent: false,
    setHeader(name: string, value: string) {
      headers[name.toLowerCase()] = value;
      return res;
    },
    getHeader(name: string) {
      return headers[name.toLowerCase()];
    },
    end(chunk?: unknown) {
      (res as { headersSent: boolean }).headersSent = true;
      if (typeof chunk === 'string') {
        originalEnd(chunk);
      } else {
        originalEnd();
      }
      return res;
    },
  }) as unknown as ServerResponse;

  return {
    req,
    res,
    socket,
    headers,
    getStatusCode: () => res.statusCode,
    getBody: () => body,
  };
}

describe('ShutdownService & graceful HTTP drain (unit)', () => {
  const cleanupFns: Array<() => void> = [];

  afterEach(() => {
    while (cleanupFns.length > 0) {
      cleanupFns.pop()?.();
    }
    process.exitCode = 0;
  });

  test('computes cooperative and hard monotonic budgets and keeps beginShutdown() idempotent', () => {
    const shutdown = createShutdownService(1000);
    let now = 5_000;
    shutdown.setClockForTesting(() => now);

    // Reserve is min(250, floor(1000 * 0.1)) = 100ms -> cooperative budget = 900ms.
    expect(shutdown.teardownReserveMs).toBe(100);
    expect(shutdown.cooperativeBudgetMs).toBe(900);
    expect(shutdown.isShuttingDown()).toBe(false);
    expect(shutdown.remainingCooperativeMs()).toBe(900);
    expect(shutdown.remainingHardMs()).toBe(1000);
    expect(shutdown.elapsedMs()).toBe(0);
    expect(shutdown.reason()).toBeUndefined();

    expect(shutdown.beginShutdown('SIGTERM')).toBe(true);
    expect(shutdown.isShuttingDown()).toBe(true);
    expect(shutdown.reason()).toBe('SIGTERM');

    now += 350;
    // Duplicate beginShutdown calls must preserve the original start timestamp and reason.
    expect(shutdown.beginShutdown('SIGINT')).toBe(false);
    expect(shutdown.beginShutdown('app.close')).toBe(false);
    expect(shutdown.reason()).toBe('SIGTERM');
    expect(shutdown.elapsedMs()).toBe(350);
    expect(shutdown.remainingCooperativeMs()).toBe(550);
    expect(shutdown.remainingHardMs()).toBe(650);

    now += 700;
    expect(shutdown.remainingCooperativeMs()).toBe(0);
    expect(shutdown.remainingHardMs()).toBe(0);
  });

  test('shares a single monotonic deadline across workers without summing timeouts', async () => {
    const shutdown = createShutdownService(200);
    shutdown.beginShutdown('SIGTERM');

    await shutdown.runWithinCooperativeBudget('WorkerA', async () => {
      await Bun.sleep(70);
    });
    const afterWorkerA = shutdown.remainingCooperativeMs();
    expect(afterWorkerA).toBeLessThan(120);
    expect(afterWorkerA).toBeGreaterThan(20);
    expect(shutdown.didTimeout()).toBe(false);

    // WorkerB hangs past the remaining cooperative budget; it must cut off using the residual budget, not a fresh 180ms.
    let resolveWorkerB!: () => void;
    const workerBGate = new Promise<void>((resolve) => {
      resolveWorkerB = resolve;
    });
    const beforeB = performance.now();
    await shutdown.runWithinCooperativeBudget('WorkerB', () => workerBGate, ['postgres']);
    const workerBDuration = performance.now() - beforeB;

    expect(workerBDuration).toBeLessThan(150);
    expect(shutdown.didTimeout()).toBe(true);
    expect(shutdown.remainingCooperativeMs()).toBe(0);
    // F1: Even after runWithinCooperativeBudget returns, WorkerB's real Promise MUST remain tracked as exceeded_budget_active!
    expect(shutdown.activeCriticalOperationCount('postgres')).toBe(1);
    expect(shutdown.activeOperationSnapshots()[0]?.state).toBe('exceeded_budget_active');

    resolveWorkerB();
    await shutdown.waitForResourceIdle('postgres');
    expect(shutdown.activeCriticalOperationCount('postgres')).toBe(0);
  });

  test('F1/F3/F4 unit: tracks critical operation states without counter underflow, caps SQS budgets, and holds beforeApplicationShutdown until settlement', async () => {
    const shutdown = createShutdownService(300);

    // 1. Completed, cancelled, and failed state transitions + context sanitization.
    const okResult = await shutdown.trackCriticalOperation(
      {
        kind: 'http_financial',
        name: 'WagerProcessorService.process',
        resources: ['postgres'],
        context: { walletId: 'wallet-123', emptyField: '', ignored: undefined },
      },
      async () => 'done',
    );
    expect(okResult).toBe('done');
    expect(shutdown.activeCriticalOperationCount()).toBe(0);

    const abortController = new AbortController();
    abortController.abort(new Error('probe aborted'));
    await expect(
      shutdown.trackCriticalOperation(
        {
          kind: 'health_probe',
          name: 'HealthController.pingPostgres',
          resources: ['postgres'],
          signal: abortController.signal,
        },
        async () => {
          throw abortController.signal.reason;
        },
      ),
    ).rejects.toThrow('probe aborted');

    await expect(
      shutdown.trackCriticalOperation(
        {
          kind: 'outbox_publish',
          name: 'OutboxPublisherService.publishBatch',
          resources: ['postgres', 'sqs'],
        },
        async () => {
          throw new Error('sqsboom');
        },
      ),
    ).rejects.toThrow('sqsboom');

    const settled = shutdown.recentSettledOperationSnapshots();
    expect(settled.map((s) => s.state)).toEqual(['completed', 'cancelled', 'failed']);
    expect(settled[0]?.context).toEqual({ walletId: 'wallet-123' });

    // 2. SQS call budget capping and 0ms exhaustion guard.
    const normalBudget = shutdown.acquireSqsCallBudget(5_000, 'test-op');
    expect(normalBudget.exhausted).toBe(false);
    if (!normalBudget.exhausted) {
      expect(normalBudget.budgetMs).toBe(5_000);
      normalBudget.dispose();
    }

    let now = 1_000;
    shutdown.setClockForTesting(() => now);
    shutdown.beginShutdown('SIGTERM'); // cooperative budget = 270ms (300 - 30)
    now += 200; // 70ms remaining

    const cappedBudget = shutdown.acquireSqsCallBudget(5_000, 'test-op');
    expect(cappedBudget.exhausted).toBe(false);
    if (!cappedBudget.exhausted) {
      expect(cappedBudget.budgetMs).toBe(70);
      cappedBudget.dispose();
    }

    now += 70; // 0ms remaining
    const zeroBudget = shutdown.acquireSqsCallBudget(5_000, 'test-op');
    expect(zeroBudget).toEqual({ exhausted: true });

    // 3. beforeApplicationShutdown sets httpDrainCompleted (F4) and holds until active critical op settles (F1).
    const holdShutdown = createShutdownService(150); // cooperative budget = 135ms
    let releaseOp!: () => void;
    const opPromise = holdShutdown.trackCriticalOperation(
      {
        kind: 'http_financial',
        name: 'WagerProcessorService.process',
        resources: ['postgres'],
      },
      () =>
        new Promise<void>((resolve) => {
          releaseOp = resolve;
        }),
    );

    let beforeShutdownResolved = false;
    const beforeShutdownPromise = holdShutdown.beforeApplicationShutdown().then(() => {
      beforeShutdownResolved = true;
    });

    await Bun.sleep(170);
    // Cooperative budget has expired, HTTP drain is marked complete, yet beforeApplicationShutdown MUST still be holding!
    expect(holdShutdown.isHttpDrainCompleted()).toBe(true);
    expect(holdShutdown.didTimeout()).toBe(true);
    expect(holdShutdown.activeOperationSnapshots()[0]?.state).toBe('exceeded_budget_active');
    expect(beforeShutdownResolved).toBe(false);

    releaseOp();
    await opPromise;
    await beforeShutdownPromise;
    expect(beforeShutdownResolved).toBe(true);
    expect(holdShutdown.activeCriticalOperationCount()).toBe(0);
  });

  test('ignores pre-finished, pre-ended, pre-destroyed responses and destroyed sockets', () => {
    const shutdown = createShutdownService(1000);

    const finishedExchange = createFakeHttpExchange();
    Object.defineProperty(finishedExchange.res, 'writableFinished', { value: true });
    shutdown.trackRequest(finishedExchange.req, finishedExchange.res);
    expect(shutdown.inFlightHttpCount()).toBe(0);

    const endedExchange = createFakeHttpExchange();
    Object.defineProperty(endedExchange.res, 'writableEnded', { value: true });
    shutdown.trackRequest(endedExchange.req, endedExchange.res);
    expect(shutdown.inFlightHttpCount()).toBe(0);

    const destroyedResExchange = createFakeHttpExchange();
    destroyedResExchange.res.destroy();
    shutdown.trackRequest(destroyedResExchange.req, destroyedResExchange.res);
    expect(shutdown.inFlightHttpCount()).toBe(0);

    const destroyedSocketExchange = createFakeHttpExchange();
    destroyedSocketExchange.socket.destroy();
    shutdown.trackRequest(destroyedSocketExchange.req, destroyedSocketExchange.res);
    expect(shutdown.inFlightHttpCount()).toBe(0);
  });

  test('tracks active requests and releases symmetrically on finish or premature socket close', async () => {
    const shutdown = createShutdownService(1000);

    const normal = createFakeHttpExchange();
    shutdown.trackRequest(normal.req, normal.res);
    expect(shutdown.inFlightHttpCount()).toBe(1);
    expect(normal.socket.listenerCount('close')).toBe(1);

    normal.res.end('ok');
    await Bun.sleep(10);
    expect(shutdown.inFlightHttpCount()).toBe(0);
    expect(normal.socket.listenerCount('close')).toBe(0);
    // Subsequent socket close must not underflow inFlightHttpCount.
    normal.socket.destroy();
    expect(shutdown.inFlightHttpCount()).toBe(0);

    const aborted = createFakeHttpExchange();
    shutdown.trackRequest(aborted.req, aborted.res);
    expect(shutdown.inFlightHttpCount()).toBe(1);
    aborted.socket.destroy();
    expect(shutdown.inFlightHttpCount()).toBe(0);
  });

  test('drain middleware rejects business routes with 503 during shutdown while preserving and tracking health routes', () => {
    const shutdown = createShutdownService(1000);
    const middleware = createHttpDrainMiddleware(shutdown);

    const activeBusiness = createFakeHttpExchange('/wallets');
    let nextCalled = false;
    middleware(activeBusiness.req, activeBusiness.res, () => {
      nextCalled = true;
    });
    expect(nextCalled).toBe(true);
    expect(shutdown.inFlightHttpCount()).toBe(1);
    activeBusiness.socket.destroy();
    expect(shutdown.inFlightHttpCount()).toBe(0);

    shutdown.beginShutdown('SIGTERM');

    const rejectedBusiness = createFakeHttpExchange('/wager-transactions?foo=1');
    rejectedBusiness.req.headers[CORRELATION_ID_HEADER] = 'corr-drain-1';
    let businessNextCalled = false;
    middleware(rejectedBusiness.req, rejectedBusiness.res, () => {
      businessNextCalled = true;
    });
    expect(businessNextCalled).toBe(false);
    expect(rejectedBusiness.getStatusCode()).toBe(503);
    expect(rejectedBusiness.headers.connection).toBe('close');
    expect(rejectedBusiness.headers[CORRELATION_ID_HEADER]).toBe('corr-drain-1');
    expect(JSON.parse(rejectedBusiness.getBody())).toEqual({
      code: 'SERVICE_UNAVAILABLE',
      message: 'Service is shutting down; retry later',
      correlationId: 'corr-drain-1',
    });

    for (const healthPath of ['/health/live', '/health/ready', '/health/ready?verbose=1']) {
      const healthExchange = createFakeHttpExchange(healthPath);
      // Express router strips req.url to '/' on wildcard mounts while preserving req.originalUrl.
      healthExchange.req.url = '/';
      (healthExchange.req as IncomingMessage & { originalUrl?: string }).originalUrl = healthPath;
      let healthNextCalled = false;
      middleware(healthExchange.req, healthExchange.res, () => {
        healthNextCalled = true;
      });
      expect(healthNextCalled).toBe(true);
      expect(healthExchange.headers.connection).toBe('close');
      expect(shutdown.inFlightHttpCount()).toBe(1);
      healthExchange.socket.destroy();
      expect(shutdown.inFlightHttpCount()).toBe(0);
    }
  });

  test('closeApplication() returns a single shared Promise across concurrent callers', async () => {
    const shutdown = createShutdownService(1000);
    let closeCalls = 0;
    const fakeApp = {
      getHttpServer: () => ({}),
      close: async () => {
        closeCalls += 1;
        await Bun.sleep(30);
      },
    } as unknown as INestApplication;

    const first = shutdown.closeApplication(fakeApp, 'SIGTERM');
    const second = shutdown.closeApplication(fakeApp, 'SIGINT');

    expect(first).toBe(second);
    await Promise.all([first, second]);
    expect(closeCalls).toBe(1);
    expect(shutdown.reason()).toBe('SIGTERM');
  });

  test('signal handlers absorb duplicate signals, keep listeners during drain, and do not call onExit(0) on clean shutdown', async () => {
    const shutdown = createShutdownService(1000);
    const exitCodes: number[] = [];
    let closeCalls = 0;
    let releaseClose!: () => void;
    const closeGate = new Promise<void>((resolve) => {
      releaseClose = resolve;
    });

    const fakeApp = {
      getHttpServer: () => ({}),
      close: async () => {
        closeCalls += 1;
        await closeGate;
      },
      flushLogs: () => undefined,
    } as unknown as INestApplication;

    const sigtermBefore = process.listenerCount('SIGTERM');
    const sigintBefore = process.listenerCount('SIGINT');

    const detach = installProcessShutdownHandlers({
      app: fakeApp,
      shutdown,
      logger: new Logger('TestBootstrap'),
      onExit: (code) => exitCodes.push(code),
    });
    cleanupFns.push(detach);

    expect(process.listenerCount('SIGTERM')).toBe(sigtermBefore + 1);
    expect(process.listenerCount('SIGINT')).toBe(sigintBefore + 1);

    // Emit concurrent SIGTERM and SIGINT while app.close() is still in progress.
    process.emit('SIGTERM', 'SIGTERM');
    process.emit('SIGINT', 'SIGINT');
    process.emit('SIGTERM', 'SIGTERM');
    await Promise.resolve();

    expect(closeCalls).toBe(1);
    // Listeners MUST remain attached while shutdown is running so OS default termination is never triggered mid-flight.
    expect(process.listenerCount('SIGTERM')).toBe(sigtermBefore + 1);
    expect(process.listenerCount('SIGINT')).toBe(sigintBefore + 1);

    releaseClose();
    await shutdown.closeApplication(fakeApp, 'SIGTERM');
    await Bun.sleep(10);

    expect(closeCalls).toBe(1);
    expect(exitCodes).toEqual([]);
    expect(process.listenerCount('SIGTERM')).toBe(sigtermBefore);
    expect(process.listenerCount('SIGINT')).toBe(sigintBefore);
  });

  test('active watchdog fires with code 1 when app.close() hangs and teardown errors also exit with code 1', async () => {
    // Case 1: Hung app.close() triggers active watchdog.
    const hungShutdown = createShutdownService(80);
    const hungExits: number[] = [];
    const hungApp = {
      getHttpServer: () => ({}),
      close: () => new Promise<void>(() => {}),
      flushLogs: () => undefined,
    } as unknown as INestApplication;

    const detachHung = installProcessShutdownHandlers({
      app: hungApp,
      shutdown: hungShutdown,
      logger: new Logger('TestWatchdog'),
      onExit: (code) => hungExits.push(code),
    });
    cleanupFns.push(detachHung);

    process.emit('SIGTERM', 'SIGTERM');
    await Bun.sleep(130);
    detachHung();

    expect(hungShutdown.didTimeout()).toBe(true);
    expect(hungExits).toEqual([1]);

    // Case 2: Teardown error inside worker or app.close() triggers exit code 1.
    const failingShutdown = createShutdownService(500);
    const failingExits: number[] = [];
    const failingApp = {
      getHttpServer: () => ({}),
      close: async () => {
        await failingShutdown.runWithinCooperativeBudget('BrokenWorker', async () => {
          throw new Error('worker stop boom');
        }).catch(() => undefined);
      },
      flushLogs: () => undefined,
    } as unknown as INestApplication;

    const detachFailing = installProcessShutdownHandlers({
      app: failingApp,
      shutdown: failingShutdown,
      logger: new Logger('TestTeardownError'),
      onExit: (code) => failingExits.push(code),
    });
    cleanupFns.push(detachFailing);

    process.emit('SIGTERM', 'SIGTERM');
    await Bun.sleep(30);

    expect(failingShutdown.hasTeardownError()).toBe(true);
    expect(failingShutdown.hasFailed()).toBe(true);
    expect(failingExits).toEqual([1]);
  });
});
