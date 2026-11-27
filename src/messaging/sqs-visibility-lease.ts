import {
  ChangeMessageVisibilityBatchCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { Logger } from '@nestjs/common';
import type { BoundedCallBudget, ShutdownService } from '../common/http/shutdown.service';

const MAX_SQS_VISIBILITY_BATCH_SIZE = 10;
const MIN_HEARTBEAT_INTERVAL_MS = 250;
const MAX_HEARTBEAT_INTERVAL_MS = 10_000;
const MIN_MAX_EXTENSION_MS = 120_000;

const RECEIPT_INVALID_CODES = new Set([
  'ReceiptHandleIsInvalid',
  'AWS.SimpleQueueService.MessageNotInflight',
  'MessageNotInflight',
]);

export type MessageLeaseState = 'queued' | 'in_flight' | 'completed' | 'released' | 'lost';

interface TrackedMessageLease {
  readonly index: number;
  readonly messageId: string | undefined;
  readonly receiptHandle: string | undefined;
  readonly groupId: string | undefined;
  leaseExpiresAtMono: number;
  state: MessageLeaseState;
  leaseLostReported: boolean;
}

export interface BatchVisibilityLeaseOptions {
  readonly sqs: Pick<SQSClient, 'send'>;
  readonly queueUrl: string;
  readonly messages: readonly Message[];
  readonly visibilityTimeoutSeconds: number;
  readonly defaultCallTimeoutMs: number;
  readonly logger: Pick<Logger, 'warn'>;
  readonly shutdown?: ShutdownService;
  readonly isStopping: () => boolean;
  readonly isGroupBlocked: (groupId: string) => boolean;
  readonly clock?: () => number;
  /** Monotonic timestamp captured immediately when `ReceiveMessage` returned the batch. */
  readonly receivedAtMono?: number;
  readonly maxExtensionMs?: number;
}

export function deriveVisibilityHeartbeatIntervalMs(visibilityTimeoutSeconds: number): number {
  const timeoutMs = visibilityTimeoutSeconds * 1_000;
  return Math.min(MAX_HEARTBEAT_INTERVAL_MS, Math.max(MIN_HEARTBEAT_INTERVAL_MS, Math.floor(timeoutMs / 3)));
}

/**
 * Maintains SQS visibility leases (`ChangeMessageVisibilityBatch`) for messages currently owned
 * by this process (`in_flight` and unblocked `queued`), preventing another process from receiving
 * the same `MessageGroupId` while this process is still working, and revoking local ownership if
 * a `ReceiptHandle` expires or fails renewal before execution starts.
 */
export class BatchVisibilityLeaseManager {
  private readonly entries: TrackedMessageLease[];
  private readonly lostGroups = new Set<string>();
  private readonly clock: () => number;
  private readonly visibilityTimeoutMs: number;
  private readonly maxExtensionMs: number;
  private readonly startedAtMono: number;

  private timer: ReturnType<typeof setTimeout> | undefined;
  private activeRenewalPromise: Promise<void> | undefined;
  private stopped = false;

  constructor(private readonly options: BatchVisibilityLeaseOptions) {
    this.clock = options.clock ?? (() => performance.now());
    this.visibilityTimeoutMs = Math.max(0, options.visibilityTimeoutSeconds * 1_000);
    this.maxExtensionMs =
      options.maxExtensionMs ?? Math.max(this.visibilityTimeoutMs * 6, MIN_MAX_EXTENSION_MS);
    this.startedAtMono = options.receivedAtMono ?? this.clock();

    const initialExpiry = this.startedAtMono + this.visibilityTimeoutMs;
    this.entries = options.messages.map((msg, index) => ({
      index,
      messageId: msg.MessageId,
      receiptHandle: msg.ReceiptHandle,
      groupId: msg.Attributes?.MessageGroupId,
      leaseExpiresAtMono: initialExpiry,
      state: 'queued',
      leaseLostReported: false,
    }));
  }

  start(): void {
    if (this.stopped || this.entries.length === 0 || this.options.visibilityTimeoutSeconds <= 0) {
      return;
    }
    const actionable = this.entries.some((entry) => entry.receiptHandle !== undefined);
    if (!actionable) {
      return;
    }
    this.scheduleNextTick(true);
  }

  /**
   * Verifies that this process still holds a valid SQS visibility lease on `messages[index]`
   * and transitions it from `'queued'` to `'in_flight'`. Returns `false` if the lease already
   * expired or an earlier message in the same FIFO group lost its lease.
   */
  claimForExecution(index: number): boolean {
    const entry = this.entries[index];
    if (entry === undefined) {
      return false;
    }
    if (entry.state === 'lost' || (entry.groupId !== undefined && this.lostGroups.has(entry.groupId))) {
      entry.state = 'lost';
      return false;
    }
    if (this.visibilityTimeoutMs > 0 && entry.receiptHandle !== undefined) {
      const now = this.clock();
      if (now >= entry.leaseExpiresAtMono) {
        this.markLeaseLost(entry, 'SQS visibility timeout elapsed before message execution started');
        return false;
      }
    }
    entry.state = 'in_flight';
    return true;
  }

  markCompleted(index: number): void {
    const entry = this.entries[index];
    if (entry === undefined || entry.state === 'lost') {
      return;
    }
    if (
      this.visibilityTimeoutMs > 0 &&
      entry.receiptHandle !== undefined &&
      this.clock() >= entry.leaseExpiresAtMono
    ) {
      this.markLeaseLost(entry, 'SQS visibility timeout elapsed before in-flight message completed');
    }
    entry.state = 'completed';
  }

  markReleasedFrom(startIndex: number): void {
    for (let i = startIndex; i < this.entries.length; i += 1) {
      const entry = this.entries[i];
      if (entry !== undefined && entry.state === 'queued') {
        entry.state = 'released';
      }
    }
  }

  lostGroupIds(): ReadonlySet<string> {
    return this.lostGroups;
  }

  leaseStateCounts(): Record<MessageLeaseState, number> {
    const counts: Record<MessageLeaseState, number> = {
      queued: 0,
      in_flight: 0,
      completed: 0,
      released: 0,
      lost: 0,
    };
    for (const entry of this.entries) {
      counts[entry.state] += 1;
    }
    return counts;
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (this.activeRenewalPromise !== undefined) {
      await this.activeRenewalPromise;
    }
  }

  /** Exposed for deterministic unit testing of renewal/loss mechanics without wall-clock sleeps. */
  async runRenewalCycle(): Promise<void> {
    if (this.activeRenewalPromise !== undefined) {
      return this.activeRenewalPromise;
    }
    if (this.stopped || this.options.visibilityTimeoutSeconds <= 0) {
      return;
    }

    const now = this.clock();
    if (now - this.startedAtMono >= this.maxExtensionMs) {
      this.options.logger.warn({
        message: 'SQS visibility heartbeat reached max extension cap; stopping further renewals',
        maxExtensionMs: this.maxExtensionMs,
      });
      this.stopped = true;
      return;
    }

    if (this.visibilityTimeoutMs > 0) {
      for (const entry of this.entries) {
        if (
          entry.receiptHandle !== undefined &&
          (entry.state === 'in_flight' || entry.state === 'queued') &&
          !entry.leaseLostReported &&
          now >= entry.leaseExpiresAtMono
        ) {
          this.markLeaseLost(entry, 'SQS visibility timeout elapsed before heartbeat renewal could run');
        }
      }
    }

    const shuttingDownOrStopping =
      this.options.isStopping() || this.options.shutdown?.isShuttingDown() === true;

    const candidates = this.entries.filter((entry) => {
      if (entry.receiptHandle === undefined || entry.leaseLostReported) {
        return false;
      }
      if (entry.groupId !== undefined && this.lostGroups.has(entry.groupId)) {
        return false;
      }
      if (entry.state === 'in_flight') {
        return true;
      }
      if (entry.state === 'queued') {
        if (shuttingDownOrStopping) {
          return false;
        }
        if (entry.groupId !== undefined && this.options.isGroupBlocked(entry.groupId)) {
          return false;
        }
        return true;
      }
      return false;
    });

    if (candidates.length === 0) {
      return;
    }

    const executeRenewal = async (): Promise<void> => {
      interface GroupRenewalQueue {
        readonly groupId: string | undefined;
        readonly items: TrackedMessageLease[];
      }

      const queues: GroupRenewalQueue[] = [];
      const queuesByGroup = new Map<string, GroupRenewalQueue>();

      for (const candidate of candidates) {
        if (candidate.groupId !== undefined) {
          let groupQueue = queuesByGroup.get(candidate.groupId);
          if (groupQueue === undefined) {
            groupQueue = { groupId: candidate.groupId, items: [] };
            queuesByGroup.set(candidate.groupId, groupQueue);
            queues.push(groupQueue);
          }
          groupQueue.items.push(candidate);
        } else {
          queues.push({ groupId: undefined, items: [candidate] });
        }
      }

      for (;;) {
        if (this.stopped) {
          return;
        }

        const waveNow = this.clock();
        const waveHeads: Array<{ readonly queue: GroupRenewalQueue; readonly entry: TrackedMessageLease }> = [];
        const currentlyShuttingDown =
          this.options.isStopping() || this.options.shutdown?.isShuttingDown() === true;

        for (const queue of queues) {
          while (queue.items.length > 0) {
            const head = queue.items[0]!;
            if (head.state === 'completed' || head.state === 'released') {
              queue.items.shift();
              continue;
            }
            if (this.visibilityTimeoutMs > 0 && waveNow >= head.leaseExpiresAtMono) {
              this.markLeaseLost(head, 'SQS visibility timeout elapsed before FIFO wave renewal could run');
              queue.items.length = 0;
              break;
            }
            if (
              head.leaseLostReported ||
              (queue.groupId !== undefined && this.lostGroups.has(queue.groupId)) ||
              (head.state === 'queued' &&
                (currentlyShuttingDown ||
                  (queue.groupId !== undefined && this.options.isGroupBlocked(queue.groupId))))
            ) {
              queue.items.length = 0;
              break;
            }
            waveHeads.push({ queue, entry: head });
            break;
          }
        }

        if (waveHeads.length === 0) {
          return;
        }

        for (let offset = 0; offset < waveHeads.length; offset += MAX_SQS_VISIBILITY_BATCH_SIZE) {
          if (this.stopped) {
            return;
          }
          const chunk = waveHeads.slice(offset, offset + MAX_SQS_VISIBILITY_BATCH_SIZE);
          const budget = this.acquireBudget();
          if (budget.exhausted) {
            return;
          }

          try {
            const response = await this.options.sqs.send(
              new ChangeMessageVisibilityBatchCommand({
                QueueUrl: this.options.queueUrl,
                Entries: chunk.map((item, idx) => ({
                  Id: String(idx),
                  ReceiptHandle: item.entry.receiptHandle!,
                  VisibilityTimeout: this.options.visibilityTimeoutSeconds,
                })),
              }),
              { abortSignal: budget.signal },
            );

            const refreshedAt = this.clock();
            const succeededIdx = new Set<number>();
            for (const ok of response.Successful ?? []) {
              const parsed = ok.Id !== undefined ? Number(ok.Id) : Number.NaN;
              if (Number.isInteger(parsed)) {
                succeededIdx.add(parsed);
              }
            }

            const failedByIdx = new Map<number, { Code?: string; Message?: string }>();
            for (const fail of response.Failed ?? []) {
              const parsed = fail.Id !== undefined ? Number(fail.Id) : Number.NaN;
              if (Number.isInteger(parsed)) {
                failedByIdx.set(parsed, fail);
              }
            }

            for (let idx = 0; idx < chunk.length; idx += 1) {
              const item = chunk[idx]!;
              const entry = item.entry;
              if (entry.state === 'completed') {
                // Message committed and was acknowledged concurrently while renewal was in flight; advance group queue.
                item.queue.items.shift();
                continue;
              }
              if (succeededIdx.has(idx) && !failedByIdx.has(idx)) {
                entry.leaseExpiresAtMono = refreshedAt + this.visibilityTimeoutMs;
                item.queue.items.shift();
                continue;
              }
              const failure = failedByIdx.get(idx);
              const invalidReceipt = failure?.Code !== undefined && RECEIPT_INVALID_CODES.has(failure.Code);
              if (invalidReceipt || refreshedAt >= entry.leaseExpiresAtMono) {
                this.markLeaseLost(
                  entry,
                  failure?.Message ?? `Visibility renewal failed (${failure?.Code ?? 'unconfirmed'})`,
                );
              }
              // Never extend later messages in the same FIFO group when its head failed renewal.
              item.queue.items.length = 0;
            }
          } catch (error) {
            const failedAt = this.clock();
            const reason = error instanceof Error ? error.message : String(error);
            for (const item of chunk) {
              if (failedAt >= item.entry.leaseExpiresAtMono) {
                this.markLeaseLost(item.entry, reason);
              }
              item.queue.items.length = 0;
            }
            if (!this.stopped) {
              this.options.logger.warn({
                message: 'SQS visibility heartbeat batch failed',
                batchSize: chunk.length,
                reason,
              });
            }
          } finally {
            budget.dispose();
          }
        }
      }
    };

    const tracked =
      this.options.shutdown !== undefined
        ? this.options.shutdown.trackCriticalOperation(
            {
              kind: 'worker',
              name: 'WagerTransactionsConsumer.visibilityHeartbeat',
              resources: ['sqs'],
            },
            executeRenewal,
          )
        : executeRenewal();

    this.activeRenewalPromise = tracked;
    try {
      await tracked;
    } finally {
      if (this.activeRenewalPromise === tracked) {
        this.activeRenewalPromise = undefined;
      }
    }
  }

  private scheduleNextTick(isInitialTick = false): void {
    if (this.stopped) {
      return;
    }
    const shuttingDownOrStopping =
      this.options.isStopping() || this.options.shutdown?.isShuttingDown() === true;
    if (shuttingDownOrStopping && !this.entries.some((entry) => entry.state === 'in_flight')) {
      return;
    }
    const intervalMs = deriveVisibilityHeartbeatIntervalMs(this.options.visibilityTimeoutSeconds);
    const delayMs = isInitialTick
      ? Math.max(0, intervalMs - Math.max(0, this.clock() - this.startedAtMono))
      : intervalMs;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.runRenewalCycle().finally(() => {
        if (!this.stopped) {
          this.scheduleNextTick(false);
        }
      });
    }, delayMs);
    this.timer.unref?.();
  }

  private acquireBudget(): BoundedCallBudget {
    if (this.options.shutdown !== undefined) {
      return this.options.shutdown.acquireSqsCallBudget(
        this.options.defaultCallTimeoutMs,
        'WagerTransactionsConsumer.visibilityHeartbeat',
      );
    }
    return {
      exhausted: false,
      budgetMs: this.options.defaultCallTimeoutMs,
      signal: AbortSignal.timeout(this.options.defaultCallTimeoutMs),
      dispose: () => undefined,
    };
  }

  private markLeaseLost(entry: TrackedMessageLease, reason: string): void {
    if (entry.leaseLostReported || entry.state === 'completed' || entry.state === 'released') {
      return;
    }
    entry.leaseLostReported = true;
    const wasQueued = entry.state === 'queued';
    if (wasQueued) {
      entry.state = 'lost';
    }
    if (entry.groupId !== undefined) {
      this.lostGroups.add(entry.groupId);
    }
    this.options.logger.warn({
      message: 'SQS message visibility lease lost; revoking local batch ownership for group tail',
      sqsMessageId: entry.messageId,
      messageGroupId: entry.groupId,
      wasQueued,
      reason,
    });
  }
}
