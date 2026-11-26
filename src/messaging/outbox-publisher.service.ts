import { GetQueueUrlCommand, SendMessageBatchCommand, SQSClient } from '@aws-sdk/client-sqs';
import { IsolationLevel } from '@mikro-orm/core';
import { type EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Injectable, Logger, type OnApplicationBootstrap, type OnModuleDestroy } from '@nestjs/common';
import { AppConfig } from '../common/config/env';
import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { isUuid } from '../common/ids';
import { OUTBOX_QUARANTINED_UNTIL_MS, OutboxMessage } from '../common/messaging/outbox-message';

const MAX_SQS_BATCH_SIZE = 10;

interface OutboxHeadRow {
  id: string;
  sequence_number: string | number;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown> | string;
  occurred_at: Date | string;
  attempts: number;
  next_attempt_at: Date | string | null;
  published_at: Date | string | null;
}

export interface PublishBatchOptions {
  readonly now?: Date;
  readonly batchSize?: number;
  /** Test/diagnostic hook invoked after SQS send and row updates, immediately before SQL COMMIT. */
  readonly beforeCommit?: (txEm: EntityManager, publishedIds: readonly string[]) => Promise<void> | void;
}

export interface PublishBatchStats {
  readonly selected: number;
  readonly published: number;
  readonly failed: number;
  readonly quarantined: number;
  readonly publishedIds: readonly string[];
}

@Injectable()
export class OutboxPublisherService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisherService.name);
  private running = false;
  private loopPromise: Promise<void> | undefined;
  private wakeWaiter: (() => void) | undefined;
  private activeBatchPromise: Promise<PublishBatchStats> | undefined;
  private integrationQueueUrl: string | undefined;

  constructor(
    private readonly orm: MikroORM,
    private readonly sqs: SQSClient,
    private readonly config: AppConfig,
  ) {}

  onApplicationBootstrap(): void {
    if (this.config.workersEnabled) {
      this.start();
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.stop();
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    this.loopPromise = this.runLoop();
  }

  async stop(): Promise<void> {
    this.running = false;
    this.wakeWaiter?.();
    await this.loopPromise;
    this.loopPromise = undefined;
    await this.activeBatchPromise;
  }

  clearQueueUrlCache(): void {
    this.integrationQueueUrl = undefined;
  }

  /**
   * Publishes up to `batchSize` (<= 10) causal head events — at most 1 unpublished head event per
   * `aggregate_id` (`walletId`) — ordered by `sequence_number ASC` under `FOR UPDATE OF o SKIP LOCKED`.
   */
  async publishBatch(options: PublishBatchOptions = {}): Promise<PublishBatchStats> {
    if (this.activeBatchPromise !== undefined) {
      return this.activeBatchPromise;
    }
    const work = this.executeBatch(options);
    this.activeBatchPromise = work;
    try {
      return await work;
    } finally {
      if (this.activeBatchPromise === work) {
        this.activeBatchPromise = undefined;
      }
    }
  }

  /**
   * Operator recovery action: makes the quarantined head event of an aggregate eligible for
   * immediate retry without decreasing `attempts` and without unblocking events behind it out of order.
   */
  async requeueQuarantinedAggregate(aggregateId: string, now = new Date()): Promise<string | undefined> {
    if (!isUuid(aggregateId)) {
      throw new DomainError(FailureCode.InvalidPayload, 'aggregateId must be a UUID');
    }
    const rows = await this.orm.em.fork().execute<{ id: string }[]>(
      `with head as (
         select id
         from outbox_messages
         where aggregate_id = ?
           and published_at is null
           and next_attempt_at = ?
         order by sequence_number asc
         limit 1
         for update
       )
       update outbox_messages o
       set next_attempt_at = ?
       from head
       where o.id = head.id
       returning o.id`,
      [aggregateId, new Date(OUTBOX_QUARANTINED_UNTIL_MS), now],
    );
    return rows[0]?.id;
  }

  private async executeBatch(options: PublishBatchOptions): Promise<PublishBatchStats> {
    const batchSize = options.batchSize ?? MAX_SQS_BATCH_SIZE;
    if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > MAX_SQS_BATCH_SIZE) {
      throw new RangeError(`batchSize must be an integer between 1 and ${MAX_SQS_BATCH_SIZE}`);
    }

    const now = options.now ?? new Date();

    // Resolve the target queue URL before acquiring row locks in PostgreSQL, unless nothing is due.
    let queueUrl: string;
    try {
      queueUrl = await this.getIntegrationQueueUrl();
    } catch (error) {
      this.integrationQueueUrl = undefined;
      return this.recordQueueResolutionFailure(now, batchSize, error);
    }

    return this.orm.em.fork().transactional(
      async (txEm) => {
        const rows = await txEm.execute<OutboxHeadRow[]>(
          `select o.id,
                  o.sequence_number,
                  o.aggregate_id,
                  o.event_type,
                  o.payload,
                  o.occurred_at,
                  o.attempts,
                  o.next_attempt_at,
                  o.published_at
           from outbox_messages o
           where o.published_at is null
             and (o.next_attempt_at is null or o.next_attempt_at <= ?)
             and not exists (
               select 1
               from outbox_messages prev
               where prev.aggregate_id = o.aggregate_id
                 and prev.published_at is null
                 and prev.sequence_number < o.sequence_number
             )
           order by o.sequence_number asc
           limit ?
           for update of o skip locked`,
          [now, batchSize],
        );

        if (rows.length === 0) {
          return {
            selected: 0,
            published: 0,
            failed: 0,
            quarantined: 0,
            publishedIds: [],
          };
        }

        const messages = rows.map(rehydrateOutboxRow);
        const succeededIds = new Set<string>();

        try {
          const response = await this.sqs.send(
            new SendMessageBatchCommand({
              QueueUrl: queueUrl,
              Entries: messages.map((msg) => ({
                Id: msg.id,
                MessageBody: JSON.stringify(msg.payload),
                MessageGroupId: msg.aggregateId,
                MessageDeduplicationId: msg.id,
              })),
            }),
            { abortSignal: AbortSignal.timeout(this.config.sqsCallTimeoutMs) },
          );
          for (const item of response.Successful ?? []) {
            if (item.Id !== undefined) {
              succeededIds.add(item.Id);
            }
          }
          for (const failure of response.Failed ?? []) {
            this.logger.warn({
              message: 'Outbox event rejected by SQS SendMessageBatch',
              eventId: failure.Id,
              code: failure.Code,
              senderFault: failure.SenderFault,
              reason: failure.Message,
            });
          }
        } catch (error) {
          this.integrationQueueUrl = undefined;
          this.logger.warn({
            message: 'Outbox SQS batch send failed; scheduling retry for locked head events',
            batchCount: messages.length,
            reason: error instanceof Error ? error.message : String(error),
          });
        }

        const publishedIds: string[] = [];
        let failed = 0;
        let quarantined = 0;

        for (const msg of messages) {
          if (succeededIds.has(msg.id)) {
            const publishedAt = options.now ?? new Date();
            msg.markPublished(publishedAt);
            const state = msg.toState();
            await txEm.execute(
              `update outbox_messages
               set published_at = ?
               where id = ? and published_at is null`,
              [state.publishedAt, state.id],
            );
            publishedIds.push(msg.id);
          } else {
            const retryAtBase = options.now ?? new Date();
            msg.scheduleRetry(retryAtBase);
            const state = msg.toState();
            await txEm.execute(
              `update outbox_messages
               set attempts = ?, next_attempt_at = ?
               where id = ? and published_at is null`,
              [state.attempts, state.nextAttemptAt, state.id],
            );
            failed += 1;
            if (msg.isQuarantined()) {
              quarantined += 1;
              this.logger.error({
                message: 'Outbox head event quarantined after reaching max automatic attempts',
                eventId: msg.id,
                aggregateId: msg.aggregateId,
                eventType: msg.eventType,
                attempts: msg.attempts,
              });
            }
          }
        }

        if (options.beforeCommit !== undefined) {
          await options.beforeCommit(txEm, publishedIds);
        }

        return {
          selected: messages.length,
          published: publishedIds.length,
          failed,
          quarantined,
          publishedIds,
        };
      },
      { isolationLevel: IsolationLevel.READ_COMMITTED },
    );
  }

  /**
   * When GetQueueUrl fails (e.g. SQS unreachable or queue missing), any currently due head rows
   * still record a failed attempt and advance exponential backoff cleanly.
   */
  private async recordQueueResolutionFailure(
    now: Date,
    batchSize: number,
    cause: unknown,
  ): Promise<PublishBatchStats> {
    this.logger.warn({
      message: 'Failed to resolve SQS integration events queue URL',
      queueName: this.config.integrationEventsQueue,
      reason: cause instanceof Error ? cause.message : String(cause),
    });

    return this.orm.em.fork().transactional(
      async (txEm) => {
        const rows = await txEm.execute<OutboxHeadRow[]>(
          `select o.id,
                  o.sequence_number,
                  o.aggregate_id,
                  o.event_type,
                  o.payload,
                  o.occurred_at,
                  o.attempts,
                  o.next_attempt_at,
                  o.published_at
           from outbox_messages o
           where o.published_at is null
             and (o.next_attempt_at is null or o.next_attempt_at <= ?)
             and not exists (
               select 1
               from outbox_messages prev
               where prev.aggregate_id = o.aggregate_id
                 and prev.published_at is null
                 and prev.sequence_number < o.sequence_number
             )
           order by o.sequence_number asc
           limit ?
           for update of o skip locked`,
          [now, batchSize],
        );

        let quarantined = 0;
        for (const row of rows) {
          const msg = rehydrateOutboxRow(row);
          msg.scheduleRetry(now);
          const state = msg.toState();
          await txEm.execute(
            `update outbox_messages
             set attempts = ?, next_attempt_at = ?
             where id = ? and published_at is null`,
            [state.attempts, state.nextAttemptAt, state.id],
          );
          if (msg.isQuarantined()) {
            quarantined += 1;
          }
        }

        return {
          selected: rows.length,
          published: 0,
          failed: rows.length,
          quarantined,
          publishedIds: [],
        };
      },
      { isolationLevel: IsolationLevel.READ_COMMITTED },
    );
  }

  private async getIntegrationQueueUrl(): Promise<string> {
    if (this.integrationQueueUrl === undefined) {
      const response = await this.sqs.send(
        new GetQueueUrlCommand({ QueueName: this.config.integrationEventsQueue }),
        { abortSignal: AbortSignal.timeout(this.config.sqsCallTimeoutMs) },
      );
      if (!response.QueueUrl) {
        throw new Error(`Queue URL missing for ${this.config.integrationEventsQueue}`);
      }
      this.integrationQueueUrl = response.QueueUrl;
    }
    return this.integrationQueueUrl;
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      let stats: PublishBatchStats | undefined;
      try {
        stats = await this.publishBatch();
      } catch (error) {
        this.logger.error({
          message: 'Outbox publisher cycle failed',
          reason: error instanceof Error ? error.message : String(error),
        });
      }

      if (!this.running) {
        break;
      }

      // Immediately drain subsequent events of aggregates that just advanced; sleep when idle or failing.
      if (stats === undefined || stats.published === 0) {
        await this.sleep(this.config.outboxPollIntervalMs);
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

function rehydrateOutboxRow(row: OutboxHeadRow): OutboxMessage {
  const payload =
    typeof row.payload === 'string'
      ? (JSON.parse(row.payload) as Record<string, unknown>)
      : row.payload;

  return OutboxMessage.rehydrate({
    id: row.id,
    aggregateId: row.aggregate_id,
    eventType: row.event_type,
    payload,
    occurredAt: toDate(row.occurred_at),
    attempts: row.attempts,
    nextAttemptAt: row.next_attempt_at === null ? null : toDate(row.next_attempt_at),
    publishedAt: row.published_at === null ? null : toDate(row.published_at),
  });
}

function toDate(value: Date | string): Date {
  return value instanceof Date ? value : new Date(value);
}
