import { IsolationLevel } from '@mikro-orm/core';
import { MikroORM } from '@mikro-orm/postgresql';
import { Injectable, Logger, type OnApplicationBootstrap, type OnModuleDestroy, Optional } from '@nestjs/common';
import { AppConfig } from '../common/config/env';
import { lockConflictSqlState } from '../common/errors/transient-error';
import { ShutdownService } from '../common/http/shutdown.service';
import { safeErrorFields } from '../common/observability/log-fields';
import { guardMetrics, MetricsRegistry, recordStatusTransition } from '../common/observability/metrics';
import { computeJitteredDelayMs } from '../common/instance/instance-identity';
import {
  type PendingReferencePolicy,
  type ResolvePendingReferenceResult,
  WagerProcessorService,
} from './wager-processor.service';
import { WagerTransactionStatus } from './wager-transaction';

const DEFAULT_BATCH_SIZE = 25;

interface DuePendingCandidateRow {
  id: string;
  wallet_id: string;
}

export interface PendingReferenceBatchOptions {
  readonly now?: Date;
  readonly batchSize?: number;
  readonly policy?: PendingReferencePolicy;
}

export interface PendingReferenceBatchStats {
  readonly scanned: number;
  readonly resolved: number;
  readonly rescheduled: number;
  readonly skipped: number;
  readonly failed: number;
}

@Injectable()
export class PendingReferenceWorker implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(PendingReferenceWorker.name);
  private running = false;
  private stopping = false;
  private loopPromise: Promise<void> | undefined;
  private wakeWaiter: (() => void) | undefined;
  private activeBatchPromise: Promise<PendingReferenceBatchStats> | undefined;
  private detachShutdownWake: (() => void) | undefined;

  constructor(
    private readonly orm: MikroORM,
    private readonly processor: WagerProcessorService,
    private readonly config: AppConfig,
    @Optional() private readonly shutdown?: ShutdownService,
    @Optional() private readonly metrics?: MetricsRegistry,
  ) {}

  onApplicationBootstrap(): void {
    if (this.config.workersEnabled && this.shutdown?.isShuttingDown() !== true) {
      this.start();
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.shutdown !== undefined) {
      await this.shutdown.runWithinCooperativeBudget(PendingReferenceWorker.name, () => this.stop(), ['postgres']);
      return;
    }
    await this.stop();
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.stopping = false;
    this.running = true;
    if (this.shutdown !== undefined && !this.shutdown.shutdownSignal.aborted) {
      const onShutdownAbort = (): void => {
        this.wakeWaiter?.();
      };
      this.shutdown.shutdownSignal.addEventListener('abort', onShutdownAbort, { once: true });
      this.detachShutdownWake = () => {
        this.shutdown?.shutdownSignal.removeEventListener('abort', onShutdownAbort);
      };
    }
    this.loopPromise = this.runLoop();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.running = false;
    this.detachShutdownWake?.();
    this.detachShutdownWake = undefined;
    this.wakeWaiter?.();
    await this.loopPromise;
    this.loopPromise = undefined;
    await this.activeBatchPromise;
    this.stopping = false;
  }

  /**
   * Two-phase resolution pass:
   * 1. Lock-free index scan of due PENDING_REFERENCE rows.
   * 2. Short per-item READ COMMITTED transaction acquiring wallets FOR UPDATE -> wager_transactions FOR UPDATE.
   */
  async processDueBatch(options: PendingReferenceBatchOptions = {}): Promise<PendingReferenceBatchStats> {
    if (this.activeBatchPromise !== undefined) {
      return this.activeBatchPromise;
    }
    const work =
      this.shutdown !== undefined
        ? this.shutdown.trackCriticalOperation(
            {
              kind: 'pending_reference',
              name: 'PendingReferenceWorker.processDueBatch',
              resources: ['postgres'],
            },
            () => this.executeBatch(options),
          )
        : this.executeBatch(options);
    this.activeBatchPromise = work;
    try {
      return await work;
    } finally {
      if (this.activeBatchPromise === work) {
        this.activeBatchPromise = undefined;
      }
    }
  }

  private async executeBatch(options: PendingReferenceBatchOptions): Promise<PendingReferenceBatchStats> {
    const batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    if (!Number.isInteger(batchSize) || batchSize < 1) {
      throw new RangeError('batchSize must be a positive integer');
    }

    if (this.stopping || this.shutdown?.isShuttingDown() === true) {
      return {
        scanned: 0,
        resolved: 0,
        rescheduled: 0,
        skipped: 0,
        failed: 0,
      };
    }

    const candidates =
      options.now !== undefined
        ? await this.orm.em.fork().execute<DuePendingCandidateRow[]>(
            `select id, wallet_id
             from wager_transactions
             where status = ?
               and next_attempt_at <= ?
             order by next_attempt_at asc, id asc
             limit ?`,
            [WagerTransactionStatus.PendingReference, options.now, batchSize],
          )
        : await this.orm.em.fork().execute<DuePendingCandidateRow[]>(
            `select id, wallet_id
             from wager_transactions
             where status = ?
               and next_attempt_at <= clock_timestamp()
             order by next_attempt_at asc, id asc
             limit ?`,
            [WagerTransactionStatus.PendingReference, batchSize],
          );

    let resolved = 0;
    let rescheduled = 0;
    let skipped = 0;
    let failed = 0;

    for (const candidate of candidates) {
      if (this.stopping || this.shutdown?.isShuttingDown() === true) {
        break;
      }
      const started = performance.now();
      try {
        const result = await this.resolveOne(candidate.id, candidate.wallet_id, options);
        guardMetrics(() => {
          this.metrics?.observe('wager_processing_duration_ms', performance.now() - started, {
            channel: 'pending_reference',
            outcome: 'completed',
          });
          if (result.outcome === 'resolved') {
            recordStatusTransition(this.metrics, result.transaction.status);
          } else if (result.outcome === 'rescheduled') {
            this.metrics?.increment('wager_retries_total', { channel: 'pending_reference', cause: 'business' });
          }
        });
        if (result.outcome === 'resolved') {
          resolved += 1;
        } else if (result.outcome === 'rescheduled') {
          rescheduled += 1;
        } else {
          skipped += 1;
        }
      } catch (error) {
        failed += 1;
        const sqlstate = lockConflictSqlState(error);
        guardMetrics(() => {
          this.metrics?.observe('wager_processing_duration_ms', performance.now() - started, {
            channel: 'pending_reference',
            outcome: 'error',
          });
          if (sqlstate !== undefined) {
            this.metrics?.increment('wager_lock_conflicts_total', { sqlstate });
          }
        });
        try {
          this.logger.error('Failed to process pending reference transaction', {
            transactionId: candidate.id,
            walletId: candidate.wallet_id,
            ...safeErrorFields(error),
          });
        } catch {
          // Logging must not change the batch counters already recorded above.
        }
      }
    }

    return {
      scanned: candidates.length,
      resolved,
      rescheduled,
      skipped,
      failed,
    };
  }

  private resolveOne(
    transactionId: string,
    walletId: string,
    options: PendingReferenceBatchOptions,
  ): Promise<ResolvePendingReferenceResult> {
    return this.orm.em.fork().transactional(
      (txEm) =>
        this.processor.resolvePendingReferenceInTransaction(txEm, transactionId, walletId, {
          ...(options.now !== undefined ? { now: options.now } : {}),
          ...(options.policy !== undefined ? { policy: options.policy } : {}),
        }),
      { isolationLevel: IsolationLevel.READ_COMMITTED },
    );
  }

  private async runLoop(): Promise<void> {
    while (this.running && this.shutdown?.isShuttingDown() !== true) {
      let stats: PendingReferenceBatchStats | undefined;
      try {
        stats = await this.processDueBatch({ batchSize: DEFAULT_BATCH_SIZE });
      } catch (error) {
        const sqlstate = lockConflictSqlState(error);
        guardMetrics(() => {
          if (sqlstate !== undefined) {
            this.metrics?.increment('wager_lock_conflicts_total', { sqlstate });
          }
        });
        try {
          this.logger.error('PendingReferenceWorker scan cycle failed', safeErrorFields(error));
        } catch {
          // A scan-log failure must not stop the worker loop.
        }
      }

      if (!this.running || this.shutdown?.isShuttingDown() === true) {
        break;
      }

      const saturated = stats !== undefined && stats.scanned >= DEFAULT_BATCH_SIZE && stats.failed === 0;
      if (!saturated) {
        await this.sleep(computeJitteredDelayMs(this.config.pendingReferencePollIntervalMs));
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.wakeWaiter = undefined;
        resolve();
      }, ms);
      this.wakeWaiter = () => {
        clearTimeout(timer);
        this.wakeWaiter = undefined;
        resolve();
      };
    });
  }
}
