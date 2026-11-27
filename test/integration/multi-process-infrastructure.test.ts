import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, spyOn, test } from 'bun:test';
import {
  ChangeMessageVisibilityBatchCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { NestFactory } from '@nestjs/core';
import type { SQL } from 'bun';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { newId } from '../../src/common/ids';
import { INSTANCE_ID_HEADER } from '../../src/common/instance/instance-identity';
import { OutboxPublisherService } from '../../src/messaging/outbox-publisher.service';
import {
  toWagerTransactionSendInput,
  WAGER_CONSUMER_NAME,
  WAGER_REQUESTED_MESSAGE_TYPE,
  type WagerTransactionRequestedEnvelope,
} from '../../src/messaging/wager-message';
import { WagerTransactionsConsumer } from '../../src/messaging/wager-transactions.consumer';
import { PendingReferenceWorker } from '../../src/wagering/pending-reference.worker';
import { WagerTransactionKind, WagerTransactionStatus } from '../../src/wagering/wager-transaction';
import type { WalletReconciliation } from '../../src/wallets/wallet-reconciliation';
import type { WalletView } from '../../src/wallets/wallets.controller';
import { createTestOrm, createTestSql, resetSchema, testDatabaseUrl, testRoleName } from '../support/database';
import {
  createClusterQueues,
  createTestSqsClient,
  type IsolatedClusterQueues,
  MultiProcessCluster,
  type RunningServiceInstance,
} from '../support/multi-process';

describe('Multi-process infrastructure (Stage 7.2 — wagering_test + LocalStack + real OS processes)', () => {
  let migratorOrm: MikroORM;
  let adminSql: SQL;
  let appSql: SQL;
  let sqs: SQSClient;
  let cluster: MultiProcessCluster;
  let activeQueues: IsolatedClusterQueues | undefined;

  beforeAll(async () => {
    migratorOrm = await createTestOrm('migrator');
    adminSql = createTestSql('admin');
    appSql = createTestSql('app');
    sqs = createTestSqsClient();
    cluster = new MultiProcessCluster();

    // Explicit target database and role confirmation before mutating any data:
    const [appIdentity] = await appSql<Array<{ db: string; usr: string }>>`
      select current_database() as db, current_user as usr
    `;
    expect(appIdentity?.db).toBe('wagering_test');
    expect(appIdentity?.usr).toBe(testRoleName('app'));
  });

  beforeEach(async () => {
    await resetSchema(migratorOrm);
  });

  afterEach(async () => {
    await cluster.stopAllGracefully().catch(async (err) => {
      await cluster.forceKillAll();
      throw err;
    });
    if (activeQueues !== undefined) {
      await activeQueues.destroy();
      activeQueues = undefined;
    }
  });

  afterAll(async () => {
    await cluster.forceKillAll();
    if (activeQueues !== undefined) {
      await activeQueues.destroy();
    }
    sqs.destroy();
    await appSql.close();
    await adminSql.close();
    await migratorOrm.close(true);
  });

  async function openWalletVia(
    instance: RunningServiceInstance,
    amount = '100.00',
    currency = 'BRL',
  ): Promise<WalletView> {
    const res = await instance.request('/wallets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId: newId(),
        initialBalance: { amount, currency },
      }),
    });
    expect(res.status).toBe(201);
    return (await res.json()) as WalletView;
  }

  async function reconcileWalletVia(
    instance: RunningServiceInstance,
    walletId: string,
  ): Promise<WalletReconciliation> {
    const res = await instance.request(`/wallets/${walletId}/reconciliation`, {
      method: 'POST',
    });
    expect(res.status).toBe(200);
    return (await res.json()) as WalletReconciliation;
  }

  function buildEnvelope(
    wallet: WalletView,
    overrides: Partial<{
      messageId: string;
      externalTransactionId: string;
      idempotencyKey: string;
      kind: WagerTransactionKind;
      amount: string;
      referenceExternalTransactionId: string;
    }> = {},
  ): WagerTransactionRequestedEnvelope {
    const extId = overrides.externalTransactionId ?? `ext-${newId()}`;
    return {
      messageId: overrides.messageId ?? `msg-${newId()}`,
      type: WAGER_REQUESTED_MESSAGE_TYPE,
      occurredAt: new Date().toISOString(),
      correlationId: `corr-${newId().slice(0, 12)}`,
      data: {
        providerId: 'provider-mp',
        externalTransactionId: extId,
        idempotencyKey: overrides.idempotencyKey ?? `idem-${extId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `round-${newId().slice(0, 8)}`,
        gameId: 'slots-mp',
        kind: overrides.kind ?? WagerTransactionKind.Bet,
        money: { amount: overrides.amount ?? '25.00', currency: wallet.balance.currency },
        ...(overrides.referenceExternalTransactionId !== undefined
          ? { referenceExternalTransactionId: overrides.referenceExternalTransactionId }
          : {}),
      },
    };
  }

  test('7.2.1: two real OS processes initialize concurrently on PORT=0 with isolated instanceId, headers, structured logs, and PostgreSQL application_name pools', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });

    const [instA, instB] = await Promise.all([
      cluster.spawn({
        instanceId: 'mp-node-a',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
      }),
      cluster.spawn({
        instanceId: 'mp-node-b',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
      }),
    ]);

    expect(instA.pid).not.toBe(instB.pid);
    expect(instA.port).toBeGreaterThan(0);
    expect(instB.port).toBeGreaterThan(0);
    expect(instA.port).not.toBe(instB.port);

    const [liveA, readyA, liveB, readyB] = await Promise.all([
      instA.request('/health/live'),
      instA.request('/health/ready'),
      instB.request('/health/live'),
      instB.request('/health/ready'),
    ]);

    expect(liveA.status).toBe(200);
    expect(liveA.headers.get(INSTANCE_ID_HEADER)).toBe('mp-node-a');
    expect(await liveA.json()).toEqual({ status: 'ok' });

    expect(readyA.status).toBe(200);
    expect(readyA.headers.get(INSTANCE_ID_HEADER)).toBe('mp-node-a');
    expect(await readyA.json()).toEqual({ status: 'ok', checks: { postgres: 'up', sqs: 'up' } });

    expect(liveB.status).toBe(200);
    expect(liveB.headers.get(INSTANCE_ID_HEADER)).toBe('mp-node-b');
    expect(readyB.status).toBe(200);
    expect(readyB.headers.get(INSTANCE_ID_HEADER)).toBe('mp-node-b');

    // Open a wallet on instA and read it immediately from instB (zero process-local state assumptions):
    const wallet = await openWalletVia(instA, '150.00');
    const getFromB = await instB.request(`/wallets/${wallet.id}`);
    expect(getFromB.status).toBe(200);
    expect(getFromB.headers.get(INSTANCE_ID_HEADER)).toBe('mp-node-b');
    expect((await getFromB.json()) as WalletView).toEqual(wallet);

    // Verify per-instance PostgreSQL connection attribution in pg_stat_activity on wagering_test:
    const activeSessions = await adminSql<Array<{ application_name: string; usename: string; count: number }>>`
      select application_name, usename, count(*)::int as count
      from pg_stat_activity
      where datname = current_database()
        and application_name in (${instA.pgApplicationName}, ${instB.pgApplicationName})
      group by application_name, usename
    `;
    const byAppName = new Map(activeSessions.map((r) => [r.application_name, r]));
    expect(byAppName.get('wagering:mp-node-a')?.usename).toBe(testRoleName('app'));
    expect(byAppName.get('wagering:mp-node-a')?.count).toBeGreaterThanOrEqual(1);
    expect(byAppName.get('wagering:mp-node-b')?.usename).toBe(testRoleName('app'));
    expect(byAppName.get('wagering:mp-node-b')?.count).toBeGreaterThanOrEqual(1);

    // Structured JSON logs carry instanceId and pid:
    expect(instA.logs().every((entry) => entry.instanceId === 'mp-node-a' && entry.pid === instA.pid)).toBe(true);
    expect(instB.logs().every((entry) => entry.instanceId === 'mp-node-b' && entry.pid === instB.pid)).toBe(true);
  }, 25_000);

  test('7.2.2: SQS visibility lease heartbeat maintains exclusive ownership across 2 live processes when wallet lock contention exceeds initial VisibilityTimeout', async () => {
    // Short 2-second SQS VisibilityTimeout -> heartbeat renews automatically every ~666ms!
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 2, maxReceiveCount: 5 });

    const [instA, instB] = await Promise.all([
      cluster.spawn({
        instanceId: 'mp-lease-a',
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
      }),
      cluster.spawn({
        instanceId: 'mp-lease-b',
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
      }),
    ]);

    const wallet = await openWalletVia(instA, '100.00');
    const envelope = buildEnvelope(wallet, { amount: '40.00' });

    // Hold a real PostgreSQL row lock on wallet for 3.0s (> 2.0s VisibilityTimeout!) while both instances poll SQS:
    let notifyRowLocked!: () => void;
    const rowLocked = new Promise<void>((resolve) => {
      notifyRowLocked = resolve;
    });
    let releaseRowLock!: () => void;
    const holdRowLock = new Promise<void>((resolve) => {
      releaseRowLock = resolve;
    });

    const blockerTx = appSql.begin(async (tx) => {
      await tx`select id from wallets where id = ${wallet.id} for update`;
      notifyRowLocked();
      await holdRowLock;
    });

    await rowLocked;

    try {
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envelope)));
      // Keep the wallet row locked for 2.8s (exceeding the 2.0s initial SQS VisibilityTimeout):
      await Bun.sleep(2_800);
    } finally {
      releaseRowLock();
      await blockerTx;
    }

    // Wait for the owning instance to commit and ACK the message:
    const deadline = Date.now() + 8_000;
    let processedTx: { status: string; balance?: { amount: string } } | undefined;
    while (Date.now() < deadline) {
      const lookup = await instB.request(
        `/providers/provider-mp/wagering/transactions/${envelope.data.externalTransactionId}`,
      );
      if (lookup.status === 200) {
        const body = (await lookup.json()) as { status: string; observedBalance: { amount: string } | null };
        if (body.status === WagerTransactionStatus.Processed) {
          processedTx = { status: body.status, balance: body.observedBalance ?? undefined };
          break;
        }
      }
      await Bun.sleep(50);
    }

    expect(processedTx?.status).toBe(WagerTransactionStatus.Processed);
    expect(processedTx?.balance?.amount).toBe('60.00');

    // Allow a brief settle window so DeleteMessage finishes cleanly:
    await Bun.sleep(300);

    // Neither instance logged a failed DeleteMessage (ReceiptHandle stayed valid thanks to heartbeat renewal!):
    const allLogs = [...instA.logs(), ...instB.logs()];
    const deleteFailures = allLogs.filter((entry) => {
      const msg = entry.message as Record<string, unknown> | undefined;
      return typeof msg === 'object' && msg !== null && msg.message === 'DeleteMessage failed after commit; redelivery will be deduplicated by inbox';
    });
    expect(deleteFailures).toHaveLength(0);

    const [inboxRow] = await appSql<Array<{ count: number }>>`
      select count(*)::int as count
      from inbox_messages
      where consumer_name = ${WAGER_CONSUMER_NAME}
        and message_id = ${envelope.messageId}
        and processed_at is not null
    `;
    expect(inboxRow?.count).toBe(1);

    const recon = await reconcileWalletVia(instB, wallet.id);
    expect(recon.consistent).toBe(true);
    expect(recon.storedBalance.amount).toBe('60.00');
  }, 25_000);

  test('7.2.3: lost SQS visibility lease revokes local batch ownership so stale queued messages are not executed concurrently with the new owner', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 1, maxReceiveCount: 5 });

    const config1 = AppConfig.load({
      ...process.env,
      INSTANCE_ID: 'mp-fenced-1',
      DATABASE_URL: testDatabaseUrl('app'),
      WORKERS_ENABLED: 'false',
      SQS_WAGER_TRANSACTIONS_QUEUE: activeQueues.txQueueName,
      SQS_WAGER_TRANSACTIONS_DLQ: activeQueues.dlqName,
      SQS_INTEGRATION_EVENTS_QUEUE: activeQueues.eventsQueueName,
      SQS_WAIT_TIME_SECONDS: '0',
    });
    const config2 = AppConfig.load({
      ...process.env,
      INSTANCE_ID: 'mp-fenced-2',
      DATABASE_URL: testDatabaseUrl('app'),
      WORKERS_ENABLED: 'false',
      SQS_WAGER_TRANSACTIONS_QUEUE: activeQueues.txQueueName,
      SQS_WAGER_TRANSACTIONS_DLQ: activeQueues.dlqName,
      SQS_INTEGRATION_EVENTS_QUEUE: activeQueues.eventsQueueName,
      SQS_WAIT_TIME_SECONDS: '0',
    });

    const app1 = await NestFactory.createApplicationContext(AppModule.forRoot(config1), { logger: false });
    const app2 = await NestFactory.createApplicationContext(AppModule.forRoot(config2), { logger: false });

    try {
      const sqs1 = app1.get(SQSClient);
      const realSend1 = sqs1.send.bind(sqs1);
      // Simulate network partition on app1's heartbeat renewals so M2's 1s lease expires while M1 is still running:
      const spy1 = spyOn(sqs1, 'send').mockImplementation(async (command: unknown, options?: unknown) => {
        if (command instanceof ChangeMessageVisibilityBatchCommand) {
          throw new Error('simulated SQS renewal timeout on instance 1');
        }
        return realSend1(command as Parameters<typeof realSend1>[0], options as Parameters<typeof realSend1>[1]);
      });

      const consumer1 = app1.get(WagerTransactionsConsumer);
      const consumer2 = app2.get(WagerTransactionsConsumer);

      // Seed 2 wallets using a temporary HTTP instance or WalletsService:
      const { WalletsService } = await import('../../src/wallets/wallets.service');
      const { Money } = await import('../../src/common/money/money');
      const walletsSvc = app2.get(WalletsService);
      const w1 = await walletsSvc.open(newId(), Money.from({ amount: '100.00', currency: 'BRL' }), newId());
      const w2 = await walletsSvc.open(newId(), Money.from({ amount: '100.00', currency: 'BRL' }), newId());

      const w1View: WalletView = {
        id: w1.id,
        playerId: w1.playerId,
        balance: w1.balance.toJSON(),
        version: w1.version,
      };
      const w2View: WalletView = {
        id: w2.id,
        playerId: w2.playerId,
        balance: w2.balance.toJSON(),
        version: w2.version,
      };

      const env1 = buildEnvelope(w1View, { amount: '30.00' });
      const env2 = buildEnvelope(w2View, { amount: '45.00' });

      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, env1)));
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, env2)));

      const executedByConsumer1: string[] = [];

      const stats1 = await consumer1.pollOnce({
        maxNumberOfMessages: 10,
        waitTimeSeconds: 1,
        beforeCommit: async (_txEm, messageId) => {
          executedByConsumer1.push(messageId);
          if (messageId === env1.messageId) {
            // Delay 1.25s while heartbeat renewals fail on Instance 1 so env2's 1.0s SQS
            // visibility lease expires before Instance 1 finishes env1!
            await Bun.sleep(1_250);
          }
        },
      });

      spy1.mockRestore();

      expect(stats1.received).toBe(2);
      // Instance 1 executed ONLY env1 and fenced env2 before starting it because its visibility lease expired!
      expect(executedByConsumer1).toEqual([env1.messageId]);
      expect(stats1.processed).toBe(1);
      expect(stats1.retried).toBe(1);

      // Now Instance 2 polls the queue (SQS ReceiveMessage may return 1..N messages per call;
      // if env1 also reappeared because its 1s lease elapsed before DeleteMessage, Inbox deduplicates env1!):
      let totalProcessedBy2 = 0;
      const drainDeadline = Date.now() + 5_000;
      while (totalProcessedBy2 < 1 && Date.now() < drainDeadline) {
        const consumer2Stats = await consumer2.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 1 });
        totalProcessedBy2 += consumer2Stats.processed;
      }
      expect(totalProcessedBy2).toBe(1);

      const recon1 = await walletsSvc.reconcile(w1.id, newId());
      const recon2 = await walletsSvc.reconcile(w2.id, newId());
      expect(recon1.consistent).toBe(true);
      expect(recon1.storedBalance.amount).toBe('70.00');
      expect(recon2.consistent).toBe(true);
      expect(recon2.storedBalance.amount).toBe('55.00');
    } finally {
      await Promise.all([app1.close(), app2.close()]);
    }
  }, 25_000);

  test('7.2.4: shared PostgreSQL clock authority prevents clock-skew races and coordinates Outbox + PendingReference across 2 live processes', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });

    // Part A: Verify OutboxPublisherService and PendingReferenceWorker use PostgreSQL clock_timestamp()
    // rather than host Date when options.now is omitted:
    const clockTestConfig = AppConfig.load({
      ...process.env,
      INSTANCE_ID: 'mp-clock-check',
      DATABASE_URL: testDatabaseUrl('app'),
      WORKERS_ENABLED: 'false',
      SQS_WAGER_TRANSACTIONS_QUEUE: activeQueues.txQueueName,
      SQS_WAGER_TRANSACTIONS_DLQ: activeQueues.dlqName,
      SQS_INTEGRATION_EVENTS_QUEUE: activeQueues.eventsQueueName,
    });
    const clockApp = await NestFactory.createApplicationContext(AppModule.forRoot(clockTestConfig), {
      logger: false,
    });
    try {
      const { WalletsService } = await import('../../src/wallets/wallets.service');
      const { Money } = await import('../../src/common/money/money');
      const walletsSvc = clockApp.get(WalletsService);
      const publisher = clockApp.get(OutboxPublisherService);
      const pendingWorker = clockApp.get(PendingReferenceWorker);

      const w = await walletsSvc.open(newId(), Money.from({ amount: '50.00', currency: 'BRL' }), newId());

      // Push the outbox row's next_attempt_at +5 seconds ahead of PostgreSQL clock_timestamp():
      await migratorOrm.em.fork().execute(
        `update outbox_messages set next_attempt_at = clock_timestamp() + interval '5 seconds' where aggregate_id = ?`,
        [w.id],
      );

      // Even if local JS Date were +60 seconds skewed ahead, publishBatch() (with options.now omitted)
      // queries PostgreSQL clock_timestamp() and must NOT prematurely select or burn attempts!
      const RealDate = globalThis.Date;
      const skewedMs = RealDate.now() + 60_000;
      class SkewedDate extends RealDate {
        constructor(...args: unknown[]) {
          if (args.length === 0) {
            super(skewedMs);
            return;
          }
          super(...(args as [string | number | Date]));
        }
        static override now(): number {
          return skewedMs;
        }
      }
      globalThis.Date = SkewedDate as DateConstructor;
      try {
        const prematurePublish = await publisher.publishBatch();
        expect(prematurePublish.selected).toBe(0);
        const prematurePending = await pendingWorker.processDueBatch();
        expect(prematurePending.scanned).toBe(0);
      } finally {
        globalThis.Date = RealDate;
      }

      // Reset next_attempt_at back to clock_timestamp() so live instances can publish cleanly:
      await migratorOrm.em.fork().execute(
        `update outbox_messages set next_attempt_at = clock_timestamp() where aggregate_id = ?`,
        [w.id],
      );
    } finally {
      await clockApp.close();
    }

    // Part B: Spawn 2 live OS processes (WORKERS_ENABLED=true) and exercise cross-process PENDING_REFERENCE + Outbox ordering:
    const [instA, instB] = await Promise.all([
      cluster.spawn({
        instanceId: 'mp-coord-a',
        queues: activeQueues,
        workersEnabled: true,
        outboxPollIntervalMs: 50,
        pendingReferencePollIntervalMs: 60,
      }),
      cluster.spawn({
        instanceId: 'mp-coord-b',
        queues: activeQueues,
        workersEnabled: true,
        outboxPollIntervalMs: 50,
        pendingReferencePollIntervalMs: 60,
      }),
    ]);

    const wallet = await openWalletVia(instA, '100.00');
    const betExtId = `bet-ref-${newId()}`;
    const winExtId = `win-after-${newId()}`;

    // 1. Submit out-of-order WIN to Instance A BEFORE the referenced BET arrives -> 202 PENDING_REFERENCE!
    const winRes = await instA.request('/wagering/transactions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-${winExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-mp',
        externalTransactionId: winExtId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-coord-1',
        gameId: 'slots-mp',
        kind: WagerTransactionKind.Win,
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: betExtId,
      }),
    });
    expect(winRes.status).toBe(202);

    // 2. Submit the referenced BET to Instance B -> 200 PROCESSED (100 - 30 = 70)!
    const betRes = await instB.request('/wagering/transactions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-${betExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-mp',
        externalTransactionId: betExtId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-coord-1',
        gameId: 'slots-mp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '30.00', currency: 'BRL' },
      }),
    });
    expect(betRes.status).toBe(200);

    // 3. Coexisting PendingReferenceWorkers resolve the parked WIN exactly once (70 + 50 = 120):
    const resolveDeadline = Date.now() + 8_000;
    let resolvedWinStatus: string | undefined;
    while (Date.now() < resolveDeadline) {
      const checkWin = await instA.request(`/providers/provider-mp/wagering/transactions/${winExtId}`);
      if (checkWin.status === 200) {
        const view = (await checkWin.json()) as { status: string };
        if (view.status === WagerTransactionStatus.Processed) {
          resolvedWinStatus = view.status;
          break;
        }
      }
      await Bun.sleep(50);
    }
    expect(resolvedWinStatus).toBe(WagerTransactionStatus.Processed);

    // 4. Wait until all outbox_messages for wallet are published and verify strict causal FIFO arrival order in SQS:
    const outboxDeadline = Date.now() + 8_000;
    while (Date.now() < outboxDeadline) {
      const [pendingOutbox] = await appSql<Array<{ unpublished: number }>>`
        select count(*)::int as unpublished
        from outbox_messages
        where aggregate_id = ${wallet.id}
          and published_at is null
      `;
      if (pendingOutbox?.unpublished === 0) {
        break;
      }
      await Bun.sleep(50);
    }

    const receivedEventIds: string[] = [];
    for (let i = 0; i < 10; i += 1) {
      const recv = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: activeQueues.eventsQueueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
          AttributeNames: ['All'],
        }),
      );
      const msgs = recv.Messages ?? [];
      if (msgs.length === 0) {
        break;
      }
      for (const m of msgs) {
        if (m.Attributes?.MessageGroupId === wallet.id && m.Body !== undefined) {
          const parsed = JSON.parse(m.Body) as { eventId: string };
          receivedEventIds.push(parsed.eventId);
        }
      }
    }

    const dbOutboxOrder = await appSql<Array<{ id: string }>>`
      select id
      from outbox_messages
      where aggregate_id = ${wallet.id}
      order by sequence_number asc
    `;
    expect(dbOutboxOrder.length).toBeGreaterThanOrEqual(4);
    expect(receivedEventIds).toEqual(dbOutboxOrder.map((r) => r.id));

    const recon = await reconcileWalletVia(instB, wallet.id);
    expect(recon.consistent).toBe(true);
    expect(recon.storedBalance.amount).toBe('120.00');
  }, 30_000);

  test('7.2.5: graceful shutdown (SIGTERM) of Instance A drains in-flight M1, hands off queued M2 immediately (VisibilityTimeout=0 on 20s queue) to Instance B, closes only Instance A connections, and leaves Instance B serving HTTP + SQS uninterrupted', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 20 });

    // Start Instance A first so it deterministically receives the [M1, M2] batch before Instance B starts polling:
    const instA = await cluster.spawn({
      instanceId: 'mp-survive-a',
      queues: activeQueues,
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
      shutdownTimeoutMs: 6_000,
    });

    const walletA = await openWalletVia(instA, '100.00');
    const walletHandoff = await openWalletVia(instA, '100.00');

    // Hold a row lock on walletA so instA blocks inside M1 while holding M2 queued in its local SQS batch:
    let notifyLocked!: () => void;
    const locked = new Promise<void>((resolve) => {
      notifyLocked = resolve;
    });
    let releaseLock!: () => void;
    const holdLock = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });

    const lockTx = appSql.begin(async (tx) => {
      await tx`select id from wallets where id = ${walletA.id} for update`;
      notifyLocked();
      await holdLock;
    });
    await locked;

    const envM1 = buildEnvelope(walletA, { amount: '35.00' });
    const envM2 = buildEnvelope(walletHandoff, { amount: '25.00' });
    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envM1)));
    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envM2)));

    // Wait until instA's SQS consumer is actively waiting on walletA's lock in PostgreSQL (so it holds [M1, M2]):
    const waitLockDeadline = Date.now() + 5_000;
    let instABlockedOnWallet = false;
    while (Date.now() < waitLockDeadline) {
      const waiting = await adminSql<Array<{ count: number }>>`
        select count(*)::int as count
        from pg_stat_activity
        where datname = current_database()
          and application_name = ${instA.pgApplicationName}
          and wait_event_type = 'Lock'
      `;
      if ((waiting[0]?.count ?? 0) >= 1) {
        instABlockedOnWallet = true;
        break;
      }
      await Bun.sleep(25);
    }
    expect(instABlockedOnWallet).toBe(true);

    // Spawn Instance B while Instance A holds [M1, M2] under the 20-second VisibilityTimeout:
    const instB = await cluster.spawn({
      instanceId: 'mp-survive-b',
      queues: activeQueues,
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
      shutdownTimeoutMs: 6_000,
    });

    const walletB = await openWalletVia(instB, '100.00');
    const walletPostStop = await openWalletVia(instB, '100.00');

    // Trigger graceful SIGTERM ONLY on Instance A!
    const handoffStartMs = Date.now();
    const stopAPromise = instA.stopGracefully('SIGTERM', 8_000);
    await Bun.sleep(100);

    // While Instance A is draining M1:
    // Instance B remains 200 /health/ready and serves live HTTP wagers without any interruption:
    const readyBDuringAShutdown = await instB.request('/health/ready');
    expect(readyBDuringAShutdown.status).toBe(200);
    expect(readyBDuringAShutdown.headers.get(INSTANCE_ID_HEADER)).toBe('mp-survive-b');

    const httpOnBDuringAShutdown = await instB.request('/wagering/transactions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `idem-on-b-${newId()}`,
      },
      body: JSON.stringify({
        providerId: 'provider-mp',
        externalTransactionId: `ext-on-b-${newId()}`,
        playerId: walletB.playerId,
        walletId: walletB.id,
        roundId: 'round-survive-b',
        gameId: 'slots-mp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '20.00', currency: 'BRL' },
      }),
    });
    expect(httpOnBDuringAShutdown.status).toBe(200);

    // Release walletA's lock -> Instance A commits M1, releases M2 (VisibilityTimeout=0), and exits cleanly with code 0:
    releaseLock();
    await lockTx;

    const exitCodeA = await stopAPromise;
    expect(exitCodeA).toBe(0);
    expect(instA.isExited()).toBe(true);
    expect(instB.isExited()).toBe(false);

    // Instance B must pick up and process M2 well before the 20s VisibilityTimeout (proving VisibilityTimeout=0 handoff!):
    const m2Deadline = Date.now() + 5_000;
    let m2Processed = false;
    while (Date.now() < m2Deadline) {
      const checkM2 = await instB.request(
        `/providers/provider-mp/wagering/transactions/${envM2.data.externalTransactionId}`,
      );
      if (checkM2.status === 200) {
        const body = (await checkM2.json()) as { status: string };
        if (body.status === WagerTransactionStatus.Processed) {
          m2Processed = true;
          break;
        }
      }
      await Bun.sleep(40);
    }
    expect(m2Processed).toBe(true);
    expect(Date.now() - handoffStartMs).toBeLessThan(10_000); // Strictly < 20s VisibilityTimeout!

    // Confirm Instance A closed ALL of its PostgreSQL connections while Instance B kept its pool intact:
    const sessionsAfterAStopped = await adminSql<Array<{ application_name: string; count: number }>>`
      select application_name, count(*)::int as count
      from pg_stat_activity
      where datname = current_database()
        and application_name in (${instA.pgApplicationName}, ${instB.pgApplicationName})
      group by application_name
    `;
    const postStopByApp = new Map(sessionsAfterAStopped.map((r) => [r.application_name, r.count]));
    expect(postStopByApp.get(instA.pgApplicationName) ?? 0).toBe(0);
    expect(postStopByApp.get(instB.pgApplicationName) ?? 0).toBeGreaterThanOrEqual(1);

    // Enqueue a fresh SQS wager after Instance A exited -> Instance B consumes it cleanly:
    const envPostStop = buildEnvelope(walletPostStop, { amount: '15.00' });
    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envPostStop)));

    const postStopDeadline = Date.now() + 5_000;
    let postStopProcessed = false;
    while (Date.now() < postStopDeadline) {
      const checkSqs = await instB.request(
        `/providers/provider-mp/wagering/transactions/${envPostStop.data.externalTransactionId}`,
      );
      if (checkSqs.status === 200) {
        const body = (await checkSqs.json()) as { status: string };
        if (body.status === WagerTransactionStatus.Processed) {
          postStopProcessed = true;
          break;
        }
      }
      await Bun.sleep(40);
    }
    expect(postStopProcessed).toBe(true);

    // Verify 100% ledger reconciliation across all 4 wallets via surviving Instance B:
    for (const [wId, expectedAmount] of [
      [walletA.id, '65.00'],
      [walletHandoff.id, '75.00'],
      [walletB.id, '80.00'],
      [walletPostStop.id, '85.00'],
    ] as const) {
      const recon = await reconcileWalletVia(instB, wId);
      expect(recon.consistent).toBe(true);
      expect(recon.storedBalance.amount).toBe(expectedAmount);
    }
  }, 30_000);
});
