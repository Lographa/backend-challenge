import 'reflect-metadata';
import { DeleteMessageCommand, SendMessageCommand, type Message } from '@aws-sdk/client-sqs';
import { Logger } from '@nestjs/common';
import { describe, expect, test } from 'bun:test';
import { AppConfig } from '../../src/common/config/env';
import { DomainError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { lockConflictSqlState } from '../../src/common/errors/transient-error';
import { DomainExceptionFilter } from '../../src/common/http/domain-exception.filter';
import { CooperativeBudgetExhaustedError, createProcessShutdownController, ShutdownService } from '../../src/common/http/shutdown.service';
import { newId } from '../../src/common/ids';
import { Money } from '../../src/common/money/money';
import { WagerTransactionsConsumer } from '../../src/messaging/wager-transactions.consumer';
import {
  OUTBOX_OLDEST_UNPUBLISHED_LAG_SQL,
  OutboxPublisherService,
  outboxLagMilliseconds,
} from '../../src/messaging/outbox-publisher.service';
import type { MetricsSnapshot } from '../../src/common/observability/metrics';
import { MetricsRegistry } from '../../src/common/observability/metrics';
import { PendingReferenceWorker } from '../../src/wagering/pending-reference.worker';
import { WagerProcessorService } from '../../src/wagering/wager-processor.service';
import { WagerTransactionStatus } from '../../src/wagering/wager-transaction';
import { parseWagerRequest } from '../../src/wagering/wager-payload';
import { WalletsService } from '../../src/wallets/wallets.service';

function counter(snapshot: MetricsSnapshot, name: string, labels: Record<string, string> = {}): number {
  const found = (snapshot.counters[name] ?? []).find((item) =>
    Object.entries(labels).every(([key, value]) => item[key] === value),
  );
  return typeof found?.value === 'number' ? found.value : 0;
}

function makeConfig(): AppConfig {
  return AppConfig.load({
    NODE_ENV: 'test',
    PORT: '0',
    DATABASE_URL: 'postgres://wagering_app:wagering_app@localhost:5433/wagering_test',
    AWS_REGION: 'us-east-1',
    AWS_ENDPOINT_URL: 'http://localhost:4566',
    AWS_ACCESS_KEY_ID: 'test',
    AWS_SECRET_ACCESS_KEY: 'test',
    SQS_WAGER_TRANSACTIONS_QUEUE: 'wager-transactions.fifo',
    SQS_WAGER_TRANSACTIONS_DLQ: 'wager-transactions-dlq.fifo',
    SQS_INTEGRATION_EVENTS_QUEUE: 'wagering-events.fifo',
    SHUTDOWN_TIMEOUT_MS: '2000',
    WORKERS_ENABLED: 'false',
  });
}

function command() {
  const parsed = parseWagerRequest(
    {
      providerId: 'provider-a',
      externalTransactionId: 'ext-1',
      playerId: newId(),
      walletId: newId(),
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
    },
    'idem-1',
  );
  return { ...parsed, correlationId: 'corr-1' };
}

function processed(status: WagerTransactionStatus, idempotentReplay = false) {
  return { transaction: { status }, idempotentReplay };
}

function wagerMessage(walletId: string, playerId: string): Message {
  return {
    MessageId: 'sqs-1',
    ReceiptHandle: 'rh-1',
    Attributes: { MessageGroupId: walletId },
    Body: JSON.stringify({
      type: 'WagerTransactionRequested',
      messageId: 'msg-1',
      occurredAt: '2026-11-26T00:00:00.000Z',
      correlationId: 'corr-1',
      data: {
        providerId: 'provider-a',
        externalTransactionId: 'ext-1',
        idempotencyKey: 'idem-1',
        playerId,
        walletId,
        roundId: 'round-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '10.00', currency: 'BRL' },
      },
    }),
  };
}

describe('transactional metric placement', () => {
  test('HTTP counts a status or replay only after the transaction resolves, and a hash conflict only after it rejects', async () => {
    const metrics = new MetricsRegistry('http');
    let stage = 'inside';
    const processor = new WagerProcessorService(
      {
        em: {
          fork: () => ({
            transactional: async () => {
              expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(0);
              stage = 'after';
              return processed(WagerTransactionStatus.Processed);
            },
          }),
        },
      } as never,
      undefined,
      undefined,
      metrics,
    );

    const result = await processor.process(command());
    expect(stage).toBe('after');
    expect(result.idempotentReplay).toBe(false);
    expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(1);

    const replay = new WagerProcessorService(
      { em: { fork: () => ({ transactional: async () => processed(WagerTransactionStatus.Processed, true) }) } } as never,
      undefined,
      undefined,
      metrics,
    );
    await replay.process(command());
    expect(counter(metrics.snapshot(), 'wager_duplicates_total', { cause: 'idempotency_replay' })).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(1);

    const conflict = new WagerProcessorService(
      {
        em: {
          fork: () => ({
            transactional: async () => {
              throw new DomainError(FailureCode.IdempotencyPayloadConflict, 'different payload');
            },
          }),
        },
      } as never,
      undefined,
      undefined,
      metrics,
    );
    await expect(conflict.process(command())).rejects.toBeInstanceOf(DomainError);
    expect(counter(metrics.snapshot(), 'wager_hash_conflicts_total', { cause: 'idempotency' })).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(1);
  });

  test('HTTP lock conflicts are not counted by the processor; 40P01 and 55P03 are counted by the filter, 40001 is not', async () => {
    const metrics = new MetricsRegistry('http-lock');
    const processor = new WagerProcessorService(
      {
        em: {
          fork: () => ({
            transactional: async () => {
              throw Object.assign(new Error('DeadlockException'), { name: 'DeadlockException', code: '40P01' });
            },
          }),
        },
      } as never,
      undefined,
      undefined,
      metrics,
    );
    await expect(processor.process(command())).rejects.toThrow('DeadlockException');
    expect(metrics.snapshot().counters.wager_lock_conflicts_total).toBeUndefined();
    expect(metrics.snapshot().histograms.wager_processing_duration_ms?.[0]?.outcome).toBe('error');

    const filter = new DomainExceptionFilter(metrics);
    const host = {
      switchToHttp: () => ({
        getRequest: () => ({ headers: { 'x-correlation-id': 'corr-lock' }, method: 'POST', url: '/wagers/1' }),
        getResponse: () => ({ headersSent: false, setHeader: () => undefined }),
      }),
    };
    const deadlock = Object.assign(new Error('deadlock'), { code: '40P01' });
    const locked = Object.assign(new Error('locked'), { cause: Object.assign(new Error('inner'), { code: '55P03' }) });
    const serialization = Object.assign(new Error('serialization'), { name: 'DeadlockException', code: '40001' });
    expect(() => filter.catch(deadlock, host as never)).toThrow('DomainExceptionFilter must be registered');
    expect(() => filter.catch(locked, host as never)).toThrow('DomainExceptionFilter must be registered');
    expect(() => filter.catch(serialization, host as never)).toThrow('DomainExceptionFilter must be registered');
    expect(counter(metrics.snapshot(), 'wager_lock_conflicts_total', { sqlstate: '40P01' })).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_lock_conflicts_total', { sqlstate: '55P03' })).toBe(1);
    expect(lockConflictSqlState(serialization)).toBeUndefined();
    expect(metrics.snapshot().counters.wager_lock_conflicts_total).toHaveLength(2);
  });

  test('a throwing metrics object does not change the HTTP result', async () => {
    const expected = processed(WagerTransactionStatus.Rejected);
    const processor = new WagerProcessorService(
      { em: { fork: () => ({ transactional: async () => expected }) } } as never,
      undefined,
      undefined,
      { observe() { throw new Error('metrics down'); }, increment() { throw new Error('metrics down'); } } as never,
    );
    const result = await processor.process(command());
    expect(result).toBe(expected as never);
  });

  test('SQS retries, duplicates, hash conflicts, ack and DLQ follow the committed outcome', async () => {
    const walletId = newId();
    const playerId = newId();
    const message = wagerMessage(walletId, playerId);
    const metrics = new MetricsRegistry('sqs');
    const calls: string[] = [];

    function consumer(transactional: () => Promise<unknown>, send?: (command: unknown) => Promise<unknown>) {
      const instance = new WagerTransactionsConsumer(
        { send: send ?? (async () => { throw new Error('SQS should not be called'); }) } as never,
        { em: { fork: () => ({ transactional }) } } as never,
        {} as never,
        makeConfig(),
        undefined,
        metrics,
      );
      (instance as unknown as { mainQueueUrl: string }).mainQueueUrl = 'http://main';
      (instance as unknown as { dlqUrl: string }).dlqUrl = 'http://dlq';
      return (instance as unknown as {
        processReceivedMessage: (input: Message, options: object) => Promise<{ outcome: { status: string }; acknowledged: boolean }>;
      }).processReceivedMessage.bind(instance);
    }

    const processedHandle = consumer(
      async () => {
        expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(0);
        return { kind: 'processed', result: processed(WagerTransactionStatus.Processed) };
      },
      async (command) => {
        expect(command).toBeInstanceOf(DeleteMessageCommand);
        expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(1);
        calls.push('delete');
        return {};
      },
    );
    const processedOutcome = await processedHandle(message, {});
    expect(processedOutcome.outcome.status).toBe('processed');
    expect(processedOutcome.acknowledged).toBe(true);
    expect(calls).toEqual(['delete']);

    const retryHandle = consumer(async () => {
      throw new DomainError(FailureCode.WalletNotFound, 'missing');
    });
    const retry = await retryHandle(message, {});
    expect(retry.outcome.status).toBe('retry_later');
    expect(retry.acknowledged).toBe(false);
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'sqs', cause: 'business' })).toBe(1);

    const infra = consumer(async () => {
      throw Object.assign(new Error('serialization'), { code: '40001' });
    });
    await infra(message, {});
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'sqs', cause: 'infrastructure' })).toBe(1);
    expect(metrics.snapshot().counters.wager_lock_conflicts_total).toBeUndefined();

    const deadlock = consumer(async () => {
      throw Object.assign(new Error('deadlock'), { code: '40P01' });
    });
    await deadlock(message, {});
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'sqs', cause: 'infrastructure' })).toBe(2);
    expect(counter(metrics.snapshot(), 'wager_lock_conflicts_total', { sqlstate: '40P01' })).toBe(1);

    const unexpected = consumer(async () => {
      throw new Error('bug');
    });
    await unexpected(message, {});
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'sqs', cause: 'unexpected' })).toBe(1);

    const duplicate = consumer(async () => ({ kind: 'duplicate' }), async () => ({}));
    const duplicateOutcome = await duplicate(message, {});
    expect(duplicateOutcome.outcome.status).toBe('duplicate');
    expect(counter(metrics.snapshot(), 'wager_duplicates_total', { cause: 'inbox' })).toBe(1);

    const replay = consumer(
      async () => ({ kind: 'processed', result: processed(WagerTransactionStatus.Processed, true) }),
      async () => ({}),
    );
    await replay(message, {});
    expect(counter(metrics.snapshot(), 'wager_duplicates_total', { cause: 'idempotency_replay' })).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(1);

    const hash = consumer(
      async () => ({ kind: 'business_conflict', code: FailureCode.IdempotencyPayloadConflict }),
      async () => ({}),
    );
    await hash(message, {});
    expect(counter(metrics.snapshot(), 'wager_hash_conflicts_total', { cause: 'idempotency' })).toBe(1);

    let dlqSends = 0;
    const mismatch = consumer(
      async () => ({ kind: 'inbox_hash_mismatch' }),
      async (command) => {
        expect(counter(metrics.snapshot(), 'wager_dlq_forwards_completed_total')).toBe(0);
        if (command instanceof SendMessageCommand) {
          dlqSends += 1;
          return {};
        }
        if (command instanceof DeleteMessageCommand) {
          return {};
        }
        throw new Error('unexpected command');
      },
    );
    const dead = await mismatch(message, {});
    expect(dead.outcome.status).toBe('dead_lettered');
    expect(dead.acknowledged).toBe(true);
    expect(dlqSends).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_hash_conflicts_total', { cause: 'inbox_payload' })).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_dlq_forwards_completed_total')).toBe(1);

    const incomplete = consumer(
      async () => ({ kind: 'inbox_hash_mismatch' }),
      async (command) => {
        if (command instanceof SendMessageCommand) {
          return {};
        }
        throw new Error('delete failed');
      },
    );
    const incompleteOutcome = await incomplete(message, {});
    expect(incompleteOutcome.acknowledged).toBe(false);
    expect(counter(metrics.snapshot(), 'wager_dlq_forwards_completed_total')).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'sqs', cause: 'unexpected' })).toBe(1);

    const budget = new WagerTransactionsConsumer(
      { send: async () => { throw new CooperativeBudgetExhaustedError('budget'); } } as never,
      { em: { fork: () => ({ transactional: async () => { throw new Error('should not transact'); } }) } } as never,
      {} as never,
      makeConfig(),
      undefined,
      metrics,
    );
    const budgetOutcome = await (
      budget as unknown as {
        processReceivedMessage: (input: Message, options: object) => Promise<{ outcome: { status: string }; acknowledged: boolean }>;
      }
    ).processReceivedMessage(message, {});
    expect(budgetOutcome.outcome.status).toBe('retry_later');
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'sqs', cause: 'business' })).toBe(1);
  });

  test('DLQ completion requires an executed DeleteMessage, and a logger failure still returns false', async () => {
    const walletId = newId();
    const playerId = newId();
    const metrics = new MetricsRegistry('dlq-delete');
    const warns: string[] = [];
    const logged: unknown[] = [];

    function bind(message: Message, send: (command: unknown) => Promise<unknown>) {
      const instance = new WagerTransactionsConsumer(
        { send } as never,
        {
          em: {
            fork: () => ({
              transactional: async () => ({ kind: 'inbox_hash_mismatch' }),
            }),
          },
        } as never,
        {} as never,
        makeConfig(),
        undefined,
        metrics,
      );
      (instance as unknown as { mainQueueUrl: string }).mainQueueUrl = 'http://main';
      (instance as unknown as { dlqUrl: string }).dlqUrl = 'http://dlq';
      (instance as unknown as { logger: { warn: (message: string, fields?: unknown) => void; error: () => void } }).logger = {
        warn: (text, fields) => {
          warns.push(text);
          logged.push(fields);
        },
        error: () => undefined,
      };
      return (
        instance as unknown as {
          processReceivedMessage: (input: Message, options: object) => Promise<{ outcome: { status: string }; acknowledged: boolean }>;
          acknowledgeAfterCommit: (queueUrl: string, receiptHandle: string | undefined, messageId: string) => Promise<boolean>;
        }
      );
    }

    let deletes = 0;
    const missingHandle = wagerMessage(walletId, playerId);
    delete missingHandle.ReceiptHandle;
    const withoutHandle = bind(missingHandle, async (command) => {
      if (command instanceof DeleteMessageCommand) {
        deletes += 1;
      }
      return {};
    });
    const missing = await withoutHandle.processReceivedMessage(missingHandle, {});
    expect(missing.outcome.status).toBe('dead_lettered');
    expect(missing.acknowledged).toBe(true);
    expect(deletes).toBe(0);
    expect(counter(metrics.snapshot(), 'wager_dlq_forwards_completed_total')).toBe(0);
    expect(warns).not.toContain('Poison message forwarded to DLQ and acknowledged');

    const failedDelete = bind(wagerMessage(walletId, playerId), async (command) => {
      if (command instanceof DeleteMessageCommand) {
        throw new Error('delete failed: balance 10.00 BRL');
      }
      return {};
    });
    const failed = await failedDelete.processReceivedMessage(wagerMessage(walletId, playerId), {});
    expect(failed.acknowledged).toBe(false);
    expect(failed.outcome.status).toBe('dead_lettered');
    expect(counter(metrics.snapshot(), 'wager_dlq_forwards_completed_total')).toBe(0);
    expect(warns).not.toContain('Poison message forwarded to DLQ and acknowledged');
    expect(JSON.stringify(logged)).not.toContain('10.00');
    expect(JSON.stringify(logged)).not.toContain('balance');

    const throwing = new WagerTransactionsConsumer(
      { send: async () => { throw new Error('delete failed: balance 10.00 BRL'); } } as never,
      {} as never,
      {} as never,
      makeConfig(),
      undefined,
      metrics,
    );
    (throwing as unknown as { logger: { warn: () => void } }).logger = {
      warn: () => {
        throw new Error('logger down');
      },
    };
    const acknowledged = await (
      throwing as unknown as {
        acknowledgeAfterCommit: (queueUrl: string, receiptHandle: string | undefined, messageId: string) => Promise<boolean>;
      }
    ).acknowledgeAfterCommit('http://main', 'rh-1', 'msg-1');
    expect(acknowledged).toBe(false);
    expect(counter(metrics.snapshot(), 'wager_dlq_forwards_completed_total')).toBe(0);
  });

  test('outbox retries count failed minus quarantined only after the batch transaction resolves', async () => {
    const metrics = new MetricsRegistry('outbox');
    const sql: string[] = [];
    function publisher(transactional: () => Promise<unknown>) {
      const instance = new OutboxPublisherService(
        { em: { fork: () => ({ transactional, execute: async (statement: string) => { sql.push(statement); return []; } }) } } as never,
        {} as never,
        makeConfig(),
        undefined,
        metrics,
      );
      (instance as unknown as { integrationQueueUrl: string }).integrationQueueUrl = 'http://events';
      return instance;
    }

    const committed = publisher(async () => {
      expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'outbox', cause: 'infrastructure' })).toBe(0);
      return { selected: 3, published: 0, failed: 3, quarantined: 1, publishedIds: [] };
    });
    await committed.publishBatch();
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'outbox', cause: 'infrastructure' })).toBe(2);
    expect(sql).toEqual([]);

    const quarantined = publisher(async () => ({ selected: 2, published: 0, failed: 2, quarantined: 2, publishedIds: [] }));
    await quarantined.publishBatch();
    const empty = publisher(async () => ({ selected: 0, published: 0, failed: 0, quarantined: 0, publishedIds: [] }));
    await empty.publishBatch();
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'outbox', cause: 'infrastructure' })).toBe(2);

    const rolledBack = publisher(async () => {
      throw new Error('rolled back');
    });
    await expect(rolledBack.publishBatch()).rejects.toThrow('rolled back');
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'outbox', cause: 'infrastructure' })).toBe(2);

    expect(OUTBOX_OLDEST_UNPUBLISHED_LAG_SQL.toLowerCase()).not.toContain('for update');
    expect(OUTBOX_OLDEST_UNPUBLISHED_LAG_SQL).toContain('clock_timestamp()');
    expect(OUTBOX_OLDEST_UNPUBLISHED_LAG_SQL).toContain('order by occurred_at asc');
    expect(outboxLagMilliseconds(undefined)).toBe(0);
    expect(outboxLagMilliseconds('15')).toBe(15);
    expect(outboxLagMilliseconds(-4)).toBe(0);

    const sampler = publisher(async () => ({ selected: 0, published: 0, failed: 0, quarantined: 0, publishedIds: [] }));
    (sampler as unknown as { orm: { em: { fork: () => { execute: (statement: string) => Promise<unknown> } } } }).orm = {
      em: {
        fork: () => ({
          execute: async (statement: string) => {
            sql.push(statement);
            return [{ lag_ms: '20' }];
          },
          transactional: async () => ({ selected: 0, published: 0, failed: 0, quarantined: 0, publishedIds: [] }),
        }),
      },
    };
    await (sampler as unknown as { sampleOutboxLag: () => Promise<void> }).sampleOutboxLag();
    await (sampler as unknown as { sampleOutboxLag: () => Promise<void> }).sampleOutboxLag();
    expect(sql.filter((statement) => statement === OUTBOX_OLDEST_UNPUBLISHED_LAG_SQL)).toHaveLength(1);
    expect(metrics.snapshot().gauges.wager_outbox_lag_ms).toEqual({ value: 20, sharedSample: true });
    await sampler.publishBatch();
    expect(sql.filter((statement) => statement === OUTBOX_OLDEST_UNPUBLISHED_LAG_SQL)).toHaveLength(1);
  });

  test('a thrown outbox cycle counts a lock conflict and does not count a retry', async () => {
    const metrics = new MetricsRegistry('outbox-cycle');
    const shutdown = new ShutdownService(makeConfig());
    const publisher = new OutboxPublisherService({} as never, {} as never, makeConfig(), shutdown, metrics);
    publisher.publishBatch = async () => {
      shutdown.beginShutdown('test');
      throw Object.assign(new Error('deadlock'), { code: '40P01' });
    };
    publisher.start();
    await publisher.stop();
    expect(counter(metrics.snapshot(), 'wager_lock_conflicts_total', { sqlstate: '40P01' })).toBe(1);
    expect(metrics.snapshot().counters.wager_retries_total).toBeUndefined();
  });

  test('pending reference counts a committed reschedule as a business retry and a resolution as a status', async () => {
    const metrics = new MetricsRegistry('pending');
    const walletId = newId();
    const transactionId = newId();
    function worker(transactional: () => Promise<unknown>) {
      return new PendingReferenceWorker(
        {
          em: {
            fork: () => ({
              execute: async () => [{ id: transactionId, wallet_id: walletId }],
              transactional,
            }),
          },
        } as never,
        {} as never,
        makeConfig(),
        undefined,
        metrics,
      );
    }

    const rescheduled = worker(async () => {
      expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'pending_reference', cause: 'business' })).toBe(0);
      return { outcome: 'rescheduled', transaction: { status: WagerTransactionStatus.PendingReference } };
    });
    const rescheduledStats = await rescheduled.processDueBatch({ now: new Date(), batchSize: 1 });
    expect(rescheduledStats.rescheduled).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'pending_reference', cause: 'business' })).toBe(1);
    expect(metrics.snapshot().counters.wager_transaction_status_transitions_total).toBeUndefined();

    const resolved = worker(async () => ({ outcome: 'resolved', transaction: { status: WagerTransactionStatus.Rejected } }));
    await resolved.processDueBatch({ now: new Date(), batchSize: 1 });
    expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'REJECTED' })).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'pending_reference', cause: 'business' })).toBe(1);

    const skipped = worker(async () => ({ outcome: 'skipped' }));
    const skippedStats = await skipped.processDueBatch({ now: new Date(), batchSize: 1 });
    expect(skippedStats.skipped).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'pending_reference', cause: 'business' })).toBe(1);

    const locked = worker(async () => {
      throw Object.assign(new Error('locked'), { code: '55P03' });
    });
    const lockedStats = await locked.processDueBatch({ now: new Date(), batchSize: 1 });
    expect(lockedStats.failed).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_lock_conflicts_total', { sqlstate: '55P03' })).toBe(1);
    expect(counter(metrics.snapshot(), 'wager_retries_total', { channel: 'pending_reference', cause: 'business' })).toBe(1);
    expect(metrics.snapshot().histograms.wager_processing_duration_ms?.some((item) => item.outcome === 'error')).toBe(true);
  });

  test('wallet opening and reconciliation count only committed outcomes, and a log failure still returns the report', async () => {
    const metrics = new MetricsRegistry('wallets');
    const opened = new WalletsService(
      { em: { fork: () => ({ transactional: async () => undefined }) } } as never,
      undefined,
      metrics,
    );
    await opened.open(newId(), Money.from({ amount: '0.00', currency: 'BRL' }), 'corr-zero');
    expect(metrics.snapshot().counters.wager_transaction_status_transitions_total).toBeUndefined();
    await opened.open(newId(), Money.from({ amount: '5.00', currency: 'BRL' }), 'corr-open');
    expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(1);

    const failing = new WalletsService(
      { em: { fork: () => ({ transactional: async () => { throw new Error('rolled back'); } }) } } as never,
      undefined,
      metrics,
    );
    await expect(failing.open(newId(), Money.from({ amount: '5.00', currency: 'BRL' }), 'corr-fail')).rejects.toThrow('rolled back');
    expect(counter(metrics.snapshot(), 'wager_transaction_status_transitions_total', { status: 'PROCESSED' })).toBe(1);

    const report = {
      walletId: newId(),
      storedBalance: { amount: '11.00', currency: 'BRL' },
      calculatedBalance: { amount: '10.00', currency: 'BRL' },
      difference: { amount: '1.00', currency: 'BRL' },
      consistent: false,
      checkedEntries: 1,
    };
    const wallets = new WalletsService(
      { em: { fork: () => ({ transactional: async () => report }) } } as never,
      undefined,
      metrics,
    );
    (wallets as unknown as { logger: { warn: () => void } }).logger = {
      warn: () => {
        throw new Error('logger down');
      },
    };
    await expect(wallets.reconcile(newId(), 'corr-div')).resolves.toBe(report);
    await expect(wallets.reconcile(newId(), 'corr-div-2')).resolves.toBe(report);
    expect(counter(metrics.snapshot(), 'wallet_reconciliation_mismatch_total')).toBe(2);

    const consistent = { ...report, consistent: true };
    const calm = new WalletsService(
      { em: { fork: () => ({ transactional: async () => consistent }) } } as never,
      undefined,
      metrics,
    );
    await calm.reconcile(newId(), 'corr-ok');
    expect(counter(metrics.snapshot(), 'wallet_reconciliation_mismatch_total')).toBe(2);
  });

  test('shutdown keeps drain increments and the hard-timeout callback does not emit the snapshot', async () => {
    const metrics = new MetricsRegistry('shutdown');
    const emissions: string[] = [];
    (metrics as unknown as { logger: { log: (message: string) => void; warn: () => void } }).logger = {
      log: (message) => emissions.push(message),
      warn: () => undefined,
    };
    metrics.increment('wager_dlq_forwards_completed_total');
    metrics.startPeriodicSnapshot(60_000);
    const shutdown = new ShutdownService({ shutdownTimeoutMs: 5_000 } as AppConfig, undefined, metrics);
    expect(shutdown.beginShutdown('test')).toBe(true);
    expect(shutdown.beginShutdown('again')).toBe(false);
    metrics.increment('wallet_reconciliation_mismatch_total');
    expect(counter(metrics.snapshot(), 'wager_dlq_forwards_completed_total')).toBe(1);
    expect(counter(metrics.snapshot(), 'wallet_reconciliation_mismatch_total')).toBe(1);
    expect((metrics as unknown as { timer?: unknown }).timer).toBeUndefined();

    const graceful = new MetricsRegistry('graceful');
    const gracefulEmissions: Array<Record<string, unknown>> = [];
    (graceful as unknown as { logger: { log: (message: string, fields: Record<string, unknown>) => void; warn: () => void } }).logger =
      {
        log: (_message, fields) => gracefulEmissions.push(fields),
        warn: () => undefined,
      };
    const gracefulShutdown = new ShutdownService({ shutdownTimeoutMs: 5_000 } as AppConfig, undefined, graceful);
    let exited = false;
    const controller = createProcessShutdownController({
      app: {
        close: async () => {
          graceful.increment('wager_transaction_status_transitions_total', { status: 'PENDING_REFERENCE' });
        },
        flushLogs: () => undefined,
      },
      shutdown: gracefulShutdown,
      logger: new Logger('metrics-shutdown'),
      onExit: () => {
        exited = true;
      },
    });
    try {
      await controller.triggerSignal('SIGTERM');
    } finally {
      controller.detach();
    }
    expect(exited).toBe(false);
    expect(gracefulEmissions).toHaveLength(1);
    expect(gracefulEmissions[0]?.emission).toBe('shutdown');
    const counters = gracefulEmissions[0]?.counters as Record<string, Array<{ status?: string; value: number }>>;
    expect(counters.wager_transaction_status_transitions_total?.[0]).toEqual({ status: 'PENDING_REFERENCE', value: 1 });

    const hard = new MetricsRegistry('hard');
    const hardEmissions: string[] = [];
    (hard as unknown as { logger: { log: (message: string) => void; warn: () => void } }).logger = {
      log: (message) => hardEmissions.push(message),
      warn: () => undefined,
    };
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let watchdogSawSnapshot: boolean | undefined;
    const hardShutdown = new ShutdownService({ shutdownTimeoutMs: 80 } as AppConfig, undefined, hard);
    const hardController = createProcessShutdownController({
      app: { close: () => gate, flushLogs: () => undefined },
      shutdown: hardShutdown,
      logger: new Logger('metrics-hard-timeout'),
      onExit: () => {
        if (watchdogSawSnapshot === undefined) {
          watchdogSawSnapshot = hardEmissions.includes('metrics_snapshot');
        }
        release();
      },
    });
    try {
      await hardController.triggerSignal('SIGTERM');
    } finally {
      hardController.detach();
    }
    expect(watchdogSawSnapshot).toBe(false);
    expect(hardEmissions).toContain('metrics_snapshot');
  });
});
