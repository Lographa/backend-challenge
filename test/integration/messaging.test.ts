import 'reflect-metadata';
import {
  ChangeMessageVisibilityBatchCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { IsolationLevel } from '@mikro-orm/core';
import { MikroORM } from '@mikro-orm/postgresql';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, spyOn, test } from 'bun:test';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { FailureCode } from '../../src/common/errors/failure-code';
import { newId } from '../../src/common/ids';
import { OUTBOX_MAX_AUTO_ATTEMPTS, OUTBOX_QUARANTINED_UNTIL_MS } from '../../src/common/messaging/outbox-message';
import { OutboxPublisherService } from '../../src/messaging/outbox-publisher.service';
import {
  toWagerTransactionSendInput,
  WAGER_CONSUMER_NAME,
  WAGER_REQUESTED_MESSAGE_TYPE,
  type WagerTransactionRequestedData,
  type WagerTransactionRequestedEnvelope,
} from '../../src/messaging/wager-message';
import { WagerTransactionsConsumer } from '../../src/messaging/wager-transactions.consumer';
import { PendingReferenceWorker } from '../../src/wagering/pending-reference.worker';
import { parseWagerRequest } from '../../src/wagering/wager-payload';
import { WagerProcessorService } from '../../src/wagering/wager-processor.service';
import { WagerTransactionKind, WagerTransactionStatus } from '../../src/wagering/wager-transaction';
import { createTestOrm, createTestSql, resetSchema, testDatabaseUrl, testRoleName } from '../support/database';

type Json = Record<string, any>;

let app1: INestApplication;
let app2: INestApplication;
let baseUrl: string;
let db: SQL;
let sqs: SQSClient;
let consumer1: WagerTransactionsConsumer;
let consumer2: WagerTransactionsConsumer;
let publisher1: OutboxPublisherService;
let publisher2: OutboxPublisherService;
let worker1: PendingReferenceWorker;
let worker2: PendingReferenceWorker;
let mainQueueUrl: string;
let dlqUrl: string;
let eventsQueueUrl: string;

const createdWalletIds: string[] = [];

async function startAppInstance(overrides: Record<string, string> = {}): Promise<INestApplication> {
  const config = AppConfig.load({
    ...process.env,
    DATABASE_URL: testDatabaseUrl('app'),
    DATABASE_POOL_MAX: '15',
    WORKERS_ENABLED: 'false',
    SQS_WAIT_TIME_SECONDS: '0',
    SQS_CALL_TIMEOUT_MS: '5000',
    ...overrides,
  });
  const instance = await NestFactory.create(AppModule.forRoot(config), { logger: false });
  await instance.listen(0);
  return instance;
}

async function resolveQueueUrl(client: SQSClient, queueName: string): Promise<string> {
  const res = await client.send(new GetQueueUrlCommand({ QueueName: queueName }));
  if (!res.QueueUrl) {
    throw new Error(`Missing QueueUrl for ${queueName}`);
  }
  return res.QueueUrl;
}

async function drainQueue(client: SQSClient, queueUrl: string): Promise<Message[]> {
  const collected: Message[] = [];
  for (;;) {
    const res = await client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 0,
        AttributeNames: ['All'],
        MessageSystemAttributeNames: ['All'],
        MessageAttributeNames: ['All'],
      }),
    );
    const batch = res.Messages ?? [];
    if (batch.length === 0) {
      break;
    }
    for (const msg of batch) {
      collected.push(msg);
      if (msg.ReceiptHandle !== undefined) {
        await client.send(
          new DeleteMessageCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: msg.ReceiptHandle,
          }),
        );
      }
    }
  }
  // Order by SQS's authoritative FIFO ingestion SequenceNumber when present.
  return collected.sort((a, b) => {
    const seqA = a.Attributes?.SequenceNumber;
    const seqB = b.Attributes?.SequenceNumber;
    if (seqA === undefined || seqB === undefined) {
      return 0;
    }
    const bigA = BigInt(seqA);
    const bigB = BigInt(seqB);
    return bigA < bigB ? -1 : bigA > bigB ? 1 : 0;
  });
}

async function withIsolatedFifoQueues<T>(
  options: { visibilityTimeoutSeconds?: number; maxReceiveCount?: number },
  run: (queues: {
    txQueueName: string;
    txQueueUrl: string;
    dlqQueueName: string;
    dlqQueueUrl: string;
    eventsQueueName: string;
    eventsQueueUrl: string;
  }) => Promise<T>,
): Promise<T> {
  const suffix = newId().replace(/-/g, '').slice(-12);
  const dlqQueueName = `wt-dlq-${suffix}.fifo`;
  const txQueueName = `wt-tx-${suffix}.fifo`;
  const eventsQueueName = `wt-ev-${suffix}.fifo`;

  const createdDlq = await sqs.send(
    new CreateQueueCommand({
      QueueName: dlqQueueName,
      Attributes: { FifoQueue: 'true' },
    }),
  );
  const dlqQueueUrl = createdDlq.QueueUrl!;
  const dlqAttrs = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: dlqQueueUrl,
      AttributeNames: ['QueueArn'],
    }),
  );
  const dlqArn = dlqAttrs.Attributes?.QueueArn!;

  const createdTx = await sqs.send(
    new CreateQueueCommand({
      QueueName: txQueueName,
      Attributes: {
        FifoQueue: 'true',
        VisibilityTimeout: String(options.visibilityTimeoutSeconds ?? 30),
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: dlqArn,
          maxReceiveCount: String(options.maxReceiveCount ?? 5),
        }),
      },
    }),
  );
  const txQueueUrl = createdTx.QueueUrl!;

  const createdEvents = await sqs.send(
    new CreateQueueCommand({
      QueueName: eventsQueueName,
      Attributes: { FifoQueue: 'true' },
    }),
  );
  const isolatedEventsUrl = createdEvents.QueueUrl!;

  try {
    return await run({
      txQueueName,
      txQueueUrl,
      dlqQueueName,
      dlqQueueUrl,
      eventsQueueName,
      eventsQueueUrl: isolatedEventsUrl,
    });
  } finally {
    await Promise.allSettled([
      sqs.send(new DeleteQueueCommand({ QueueUrl: txQueueUrl })),
      sqs.send(new DeleteQueueCommand({ QueueUrl: dlqQueueUrl })),
      sqs.send(new DeleteQueueCommand({ QueueUrl: isolatedEventsUrl })),
    ]);
  }
}

beforeAll(async () => {
  const migrator: MikroORM = await createTestOrm('migrator');
  try {
    await resetSchema(migrator);
  } finally {
    await migrator.close();
  }

  app1 = await startAppInstance();
  app2 = await startAppInstance();
  baseUrl = await app1.getUrl();

  sqs = app1.get(SQSClient);
  consumer1 = app1.get(WagerTransactionsConsumer);
  consumer2 = app2.get(WagerTransactionsConsumer);
  publisher1 = app1.get(OutboxPublisherService);
  publisher2 = app2.get(OutboxPublisherService);
  worker1 = app1.get(PendingReferenceWorker);
  worker2 = app2.get(PendingReferenceWorker);
  db = createTestSql('app');

  const config = app1.get(AppConfig);
  mainQueueUrl = await resolveQueueUrl(sqs, config.wagerTransactionsQueue);
  dlqUrl = await resolveQueueUrl(sqs, config.wagerTransactionsDlq);
  eventsQueueUrl = await resolveQueueUrl(sqs, config.integrationEventsQueue);

  await drainQueue(sqs, mainQueueUrl);
  await drainQueue(sqs, dlqUrl);
  await drainQueue(sqs, eventsQueueUrl);
});

afterAll(async () => {
  await drainQueue(sqs, mainQueueUrl).catch(() => undefined);
  await drainQueue(sqs, dlqUrl).catch(() => undefined);
  await drainQueue(sqs, eventsQueueUrl).catch(() => undefined);
  await app2?.close();
  await app1?.close();
  await db?.close();
});

async function call(
  method: 'GET' | 'POST',
  path: string,
  body?: unknown,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: Json; headers: Headers }> {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as Json, headers: response.headers };
}

interface OpenedWallet {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}

async function openWallet(amount: string, playerId = newId(), currency = 'BRL'): Promise<OpenedWallet> {
  const response = await call('POST', '/wallets', { playerId, initialBalance: { amount, currency } });
  expect(response.status).toBe(201);
  createdWalletIds.push(response.body.id);
  return response.body as OpenedWallet;
}

async function submitHttpWager(
  payload: Record<string, unknown>,
  idempotencyKey?: string,
  extraHeaders: Record<string, string> = {},
): Promise<{ status: number; body: Json; headers: Headers }> {
  const key =
    idempotencyKey ??
    `${String(payload.providerId ?? 'provider-a')}:${String(payload.externalTransactionId ?? newId())}`;
  return call('POST', '/wagering/transactions', payload, {
    'idempotency-key': key,
    ...extraHeaders,
  });
}

function buildWagerEnvelope(
  wallet: { id: string; playerId: string },
  overrides: Partial<WagerTransactionRequestedData> = {},
  envelopeOverrides: Partial<Omit<WagerTransactionRequestedEnvelope, 'data'>> = {},
): WagerTransactionRequestedEnvelope {
  const providerId = overrides.providerId ?? `prov-${newId().slice(-8)}`;
  const externalTransactionId = overrides.externalTransactionId ?? `ext-${newId()}`;
  const idempotencyKey = overrides.idempotencyKey ?? `${providerId}:${externalTransactionId}`;
  return {
    messageId: envelopeOverrides.messageId ?? `msg-${newId()}`,
    type: WAGER_REQUESTED_MESSAGE_TYPE,
    occurredAt: envelopeOverrides.occurredAt ?? new Date().toISOString(),
    ...(envelopeOverrides.correlationId !== undefined ? { correlationId: envelopeOverrides.correlationId } : {}),
    data: {
      providerId,
      externalTransactionId,
      idempotencyKey,
      playerId: overrides.playerId ?? wallet.playerId,
      walletId: overrides.walletId ?? wallet.id,
      roundId: overrides.roundId ?? `round-${newId().slice(-8)}`,
      gameId: overrides.gameId ?? 'fortune-chimp',
      kind: overrides.kind ?? WagerTransactionKind.Bet,
      money: overrides.money ?? { amount: '25.00', currency: 'BRL' },
      ...(overrides.referenceExternalTransactionId !== undefined
        ? { referenceExternalTransactionId: overrides.referenceExternalTransactionId }
        : {}),
    },
  };
}

async function publishAllDueOutbox(publisher: OutboxPublisherService, now?: Date): Promise<number> {
  let total = 0;
  for (let guard = 0; guard < 100; guard += 1) {
    const stats = await publisher.publishBatch(now !== undefined ? { now } : {});
    total += stats.published;
    if (stats.published === 0) {
      break;
    }
  }
  return total;
}

describe('Database & role isolation guard', () => {
  test('uses exclusively wagering_test database and wagering_app role', async () => {
    const [identity] = await db`select current_database() as database_name, current_user as role_name`;
    expect(identity.database_name).toBe('wagering_test');
    expect(identity.role_name).toBe(testRoleName('app'));
  });
});

describe('4.1 & 4.2 — SQS Consumer and Persistent Inbox', () => {
  test('processes a valid FIFO message in READ COMMITTED, persists Inbox + financial state atomically, and ACKs only after commit', async () => {
    await drainQueue(sqs, mainQueueUrl);
    const wallet = await openWallet('100.00');
    const envelope = buildWagerEnvelope(
      wallet,
      { kind: WagerTransactionKind.Bet, money: { amount: '25.00', currency: 'BRL' } },
      { correlationId: 'corr-sqs-valid-1' },
    );

    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(mainQueueUrl, envelope)));

    let observedIsolation = '';
    let inFlightCountBeforeCommit = -1;

    const stats = await consumer1.pollOnce({
      waitTimeSeconds: 1,
      beforeCommit: async (txEm) => {
        const isoRows = await txEm.execute<{ transaction_isolation: string }[]>(
          'show transaction_isolation',
        );
        observedIsolation = isoRows[0]?.transaction_isolation ?? '';

        // Outside the uncommitted transaction, neither Inbox nor Wallet balance is visible yet.
        const [outsideWallet] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
        expect(outsideWallet.balance_amount).toBe('100.00');
        expect(outsideWallet.version).toBe(1);

        const outsideInbox = await db`
          select 1 from inbox_messages
          where consumer_name = ${WAGER_CONSUMER_NAME} and message_id = ${envelope.messageId}
        `;
        expect(outsideInbox).toHaveLength(0);

        // SQS message MUST still be in-flight (not deleted before PostgreSQL commits).
        const attrs = await sqs.send(
          new GetQueueAttributesCommand({
            QueueUrl: mainQueueUrl,
            AttributeNames: ['ApproximateNumberOfMessagesNotVisible'],
          }),
        );
        inFlightCountBeforeCommit = Number(attrs.Attributes?.ApproximateNumberOfMessagesNotVisible ?? '0');
      },
    });

    expect(stats.processed).toBe(1);
    expect(observedIsolation).toBe('read committed');
    expect(inFlightCountBeforeCommit).toBeGreaterThanOrEqual(1);

    // After COMMIT, SQS message was deleted (visibility release finds nothing left).
    const postAckAttrs = await sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: mainQueueUrl,
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
      }),
    );
    expect(Number(postAckAttrs.Attributes?.ApproximateNumberOfMessages ?? '0')).toBe(0);
    expect(Number(postAckAttrs.Attributes?.ApproximateNumberOfMessagesNotVisible ?? '0')).toBe(0);

    // Verify Inbox, Wallet, WagerTransaction, Ledger and Outbox in PostgreSQL.
    const [inboxRow] = await db`
      select consumer_name, message_id, payload_hash, received_at, processed_at
      from inbox_messages
      where consumer_name = ${WAGER_CONSUMER_NAME} and message_id = ${envelope.messageId}
    `;
    expect(inboxRow).toBeDefined();
    expect(inboxRow.processed_at).not.toBeNull();
    expect(new Date(inboxRow.processed_at).getTime()).toBeGreaterThanOrEqual(
      new Date(inboxRow.received_at).getTime(),
    );

    const [walletRow] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
    expect(walletRow).toEqual({ balance_amount: '75.00', version: 2 });

    const [txRow] = await db`
      select id, status, observed_balance_amount
      from wager_transactions
      where idempotency_key = ${envelope.data.idempotencyKey}
    `;
    expect(txRow.status).toBe(WagerTransactionStatus.Processed);
    expect(txRow.observed_balance_amount).toBe('75.00');

    const ledgerRows = await db`
      select direction, amount, balance_before, balance_after, wallet_version
      from wallet_ledger_entries
      where transaction_id = ${txRow.id}
    `;
    expect(ledgerRows).toEqual([
      {
        direction: 'DEBIT',
        amount: '25.00',
        balance_before: '100.00',
        balance_after: '75.00',
        wallet_version: 2,
      },
    ]);

    const outboxRows = await db`
      select event_type, payload
      from outbox_messages
      where payload->'data'->>'transactionId' = ${txRow.id}
      order by sequence_number asc
    `;
    expect(outboxRows.map((r: Json) => r.event_type)).toEqual([
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
    ]);
    for (const row of outboxRows) {
      expect(row.payload.correlationId).toBe('corr-sqs-valid-1');
      expect(row.payload.causationId).toBe(envelope.messageId);
    }
  });

  test('deduplicates sequential and concurrent redeliveries of the same messageId and preserves cross-channel idempotency', async () => {
    const wallet = await openWallet('200.00');
    const envelope = buildWagerEnvelope(wallet, {
      kind: WagerTransactionKind.Bet,
      money: { amount: '40.00', currency: 'BRL' },
    });

    // 1. Simultaneous delivery of the exact same messageId to 2 consumer instances.
    const syntheticMessage = (): Message => ({
      MessageId: `sqs-${newId()}`,
      Body: JSON.stringify(envelope),
      Attributes: { MessageGroupId: wallet.id },
    });

    const [firstOutcome, secondOutcome] = await Promise.all([
      consumer1.handleMessage(syntheticMessage()),
      consumer2.handleMessage(syntheticMessage()),
    ]);

    const statuses = [firstOutcome.status, secondOutcome.status].sort();
    expect(statuses).toEqual(['duplicate', 'processed']);

    // 2. Sequential redelivery over real SQS (new SQS DeduplicationId simulating post-5min redelivery).
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: mainQueueUrl,
        MessageBody: JSON.stringify(envelope),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: `redelivery-${newId()}`,
      }),
    );
    const pollStats = await consumer1.pollOnce({ waitTimeSeconds: 0 });
    expect(pollStats.duplicates).toBe(1);
    expect(pollStats.processed).toBe(0);

    // 3. Cross-channel replay over HTTP with the same Idempotency-Key -> idempotentReplay: true, no new debit.
    const httpReplay = await submitHttpWager(
      {
        providerId: envelope.data.providerId,
        externalTransactionId: envelope.data.externalTransactionId,
        playerId: envelope.data.playerId,
        walletId: envelope.data.walletId,
        roundId: envelope.data.roundId,
        gameId: envelope.data.gameId,
        kind: envelope.data.kind,
        money: envelope.data.money,
      },
      envelope.data.idempotencyKey,
    );
    expect(httpReplay.status).toBe(200);
    expect(httpReplay.body.idempotentReplay).toBe(true);
    expect(httpReplay.body.balance).toEqual({ amount: '160.00', currency: 'BRL' });

    // 4. New SQS messageId carrying an Idempotency-Key with a conflicting payload -> closes Inbox, keeps state intact.
    const conflictEnvelope = buildWagerEnvelope(
      wallet,
      {
        providerId: envelope.data.providerId,
        externalTransactionId: envelope.data.externalTransactionId,
        idempotencyKey: envelope.data.idempotencyKey,
        kind: WagerTransactionKind.Bet,
        money: { amount: '99.00', currency: 'BRL' },
      },
      { messageId: `msg-conflict-${newId()}` },
    );
    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(mainQueueUrl, conflictEnvelope)));
    const conflictStats = await consumer1.pollOnce({ waitTimeSeconds: 0 });
    expect(conflictStats.conflicts).toBe(1);

    const [conflictInbox] = await db`
      select processed_at from inbox_messages
      where consumer_name = ${WAGER_CONSUMER_NAME} and message_id = ${conflictEnvelope.messageId}
    `;
    expect(conflictInbox.processed_at).not.toBeNull();

    // Verify wallet moved ONCE (200.00 -> 160.00, version 2) across all 4 deliveries.
    const [walletRow] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
    expect(walletRow).toEqual({ balance_amount: '160.00', version: 2 });

    const txRows = await db`
      select id from wager_transactions where wallet_id = ${wallet.id} and kind <> 'OPENING'
    `;
    expect(txRows).toHaveLength(1);
  });

  test('routes invalid poison messages to DLQ without financial effect, and refuses to ACK if DLQ send fails', async () => {
    await drainQueue(sqs, mainQueueUrl);
    await drainQueue(sqs, dlqUrl);
    const wallet = await openWallet('100.00');

    // First process one valid message so we can also test messageId payloadHash tampering.
    const validEnvelope = buildWagerEnvelope(wallet, {
      kind: WagerTransactionKind.Bet,
      money: { amount: '10.00', currency: 'BRL' },
    });
    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(mainQueueUrl, validEnvelope)));
    expect((await consumer1.pollOnce({ waitTimeSeconds: 0 })).processed).toBe(1);

    const poisonBodies = [
      { body: '{broken-json', group: wallet.id },
      { body: JSON.stringify({ messageId: `m-${newId()}`, type: 'WrongType', occurredAt: new Date().toISOString(), data: {} }), group: wallet.id },
      {
        body: JSON.stringify(
          buildWagerEnvelope(wallet, { kind: WagerTransactionKind.Bet, money: { amount: '0.00', currency: 'BRL' } }),
        ),
        group: wallet.id,
      },
      {
        body: JSON.stringify(buildWagerEnvelope(wallet, { kind: WagerTransactionKind.Opening })),
        group: wallet.id,
      },
      {
        // Valid JSON & domain fields, but wrong SQS MessageGroupId.
        body: JSON.stringify(buildWagerEnvelope(wallet)),
        group: newId(),
      },
      {
        // Reusing validEnvelope.messageId with a mutated business payload -> inbox_hash_mismatch -> DLQ!
        body: JSON.stringify(
          buildWagerEnvelope(
            wallet,
            { kind: WagerTransactionKind.Bet, money: { amount: '55.00', currency: 'BRL' } },
            { messageId: validEnvelope.messageId },
          ),
        ),
        group: wallet.id,
      },
    ];

    for (const item of poisonBodies) {
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: mainQueueUrl,
          MessageBody: item.body,
          MessageGroupId: item.group,
          MessageDeduplicationId: `poison-${newId()}`,
        }),
      );
    }

    const stats = await consumer1.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 0 });
    expect(stats.deadLettered).toBe(poisonBodies.length);
    expect(stats.processed).toBe(0);

    // Wallet still reflects only the single valid 10.00 BET (90.00, version 2).
    const [walletRow] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
    expect(walletRow).toEqual({ balance_amount: '90.00', version: 2 });

    const dlqMessages = await drainQueue(sqs, dlqUrl);
    expect(dlqMessages).toHaveLength(poisonBodies.length);
    for (const dlqMsg of dlqMessages) {
      expect(dlqMsg.MessageAttributes?.failureCode?.StringValue).toBeDefined();
    }

    // Safety net: if DLQ write fails (missing DLQ), consumer MUST NOT DeleteMessage from main queue.
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 1, maxReceiveCount: 5 }, async (queues) => {
      const brokenDlqApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: `nonexistent-dlq-${newId().slice(-8)}.fifo`,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });
      try {
        const brokenConsumer = brokenDlqApp.get(WagerTransactionsConsumer);
        await sqs.send(
          new SendMessageCommand({
            QueueUrl: queues.txQueueUrl,
            MessageBody: '{poison-when-dlq-down',
            MessageGroupId: wallet.id,
            MessageDeduplicationId: `dlq-down-${newId()}`,
          }),
        );

        const brokenStats = await brokenConsumer.pollOnce({ waitTimeSeconds: 1 });
        expect(brokenStats.deadLettered).toBe(0);
        expect(brokenStats.retried).toBe(1);

        // Wait for 1s visibility timeout using SQS long-poll: message is still on main queue (not silently lost!).
        const waitRes = await sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: queues.txQueueUrl,
            MaxNumberOfMessages: 1,
            WaitTimeSeconds: 3,
          }),
        );
        const recovered = waitRes.Messages ?? [];
        expect(recovered).toHaveLength(1);
        expect(recovered[0]?.Body).toBe('{poison-when-dlq-down');
        if (recovered[0]?.ReceiptHandle !== undefined) {
          await sqs.send(
            new DeleteMessageCommand({ QueueUrl: queues.txQueueUrl, ReceiptHandle: recovered[0].ReceiptHandle }),
          );
        }
      } finally {
        await brokenDlqApp.close();
      }
    });
  }, 15_000);

  test(
    'rolls back Inbox + financial state on pre-commit failure without ACKing, then recovers cleanly on SQS redelivery',
    async () => {
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 1, maxReceiveCount: 5 }, async (queues) => {
      const isolatedApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      try {
        const isolatedConsumer = isolatedApp.get(WagerTransactionsConsumer);
        const wallet = await openWallet('100.00');
        const envelope = buildWagerEnvelope(wallet, {
          kind: WagerTransactionKind.Bet,
          money: { amount: '30.00', currency: 'BRL' },
        });

        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, envelope)));

        // Attempt 1: crash after all SQL statements executed inside txEm, immediately before COMMIT.
        let sawUncommittedStateInsideTx = false;
        const firstAttempt = await isolatedConsumer.pollOnce({
          waitTimeSeconds: 1,
          beforeCommit: async (txEm) => {
            const innerInbox = await txEm.execute<{ processed_at: Date | null }[]>(
              `select processed_at from inbox_messages where consumer_name = ? and message_id = ?`,
              [WAGER_CONSUMER_NAME, envelope.messageId],
            );
            expect(innerInbox).toHaveLength(1);
            expect(innerInbox[0]?.processed_at).not.toBeNull();
            sawUncommittedStateInsideTx = true;
            throw new Error('simulated crash right before PostgreSQL COMMIT');
          },
        });

        expect(sawUncommittedStateInsideTx).toBe(true);
        expect(firstAttempt.retried).toBe(1);
        expect(firstAttempt.processed).toBe(0);

        // Verify atomic rollback across ALL 5 tables in PostgreSQL.
        const inboxAfterCrash = await db`
          select 1 from inbox_messages
          where consumer_name = ${WAGER_CONSUMER_NAME} and message_id = ${envelope.messageId}
        `;
        expect(inboxAfterCrash).toHaveLength(0);

        const [walletAfterCrash] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
        expect(walletAfterCrash).toEqual({ balance_amount: '100.00', version: 1 });

        const txAfterCrash = await db`
          select 1 from wager_transactions where idempotency_key = ${envelope.data.idempotencyKey}
        `;
        expect(txAfterCrash).toHaveLength(0);

        const ledgerAfterCrash = await db`
          select 1 from wallet_ledger_entries where wallet_id = ${wallet.id} and wallet_version > 1
        `;
        expect(ledgerAfterCrash).toHaveLength(0);

        const outboxAfterCrash = await db`
          select 1 from outbox_messages
          where aggregate_id = ${wallet.id} and event_type = 'WagerTransactionProcessed'
            and payload->'data'->>'externalTransactionId' = ${envelope.data.externalTransactionId}
        `;
        expect(outboxAfterCrash).toHaveLength(0);

        // Attempt 2 (long-polls across the 1s VisibilityTimeout): succeeds, commits atomically, and ACKs.
        const secondAttempt = await isolatedConsumer.pollOnce({ waitTimeSeconds: 3 });
        expect(secondAttempt.processed).toBe(1);

        const [walletAfterRecovery] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
        expect(walletAfterRecovery).toEqual({ balance_amount: '70.00', version: 2 });

        const [inboxAfterRecovery] = await db`
          select processed_at from inbox_messages
          where consumer_name = ${WAGER_CONSUMER_NAME} and message_id = ${envelope.messageId}
        `;
        expect(inboxAfterRecovery.processed_at).not.toBeNull();
      } finally {
        await isolatedApp.close();
      }
    });
  },
    15_000,
  );

  test(
    'rolls back Inbox when WagerProcessorService fails (WALLET_NOT_FOUND) and lets SQS RedrivePolicy move to DLQ after maxReceiveCount',
    async () => {
      await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 1, maxReceiveCount: 2 }, async (queues) => {
        const isolatedApp = await startAppInstance({
          SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
          SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
          SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
        });

        try {
          const isolatedConsumer = isolatedApp.get(WagerTransactionsConsumer);
          const missingWalletId = newId();
          const envelope = buildWagerEnvelope({ id: missingWalletId, playerId: newId() });

          await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, envelope)));

          // Delivery 1 -> WALLET_NOT_FOUND -> rolls back Inbox, no ACK.
          const poll1 = await isolatedConsumer.pollOnce({ waitTimeSeconds: 1 });
          expect(poll1.retried).toBe(1);

          const inboxAfter1 = await db`
            select 1 from inbox_messages
            where consumer_name = ${WAGER_CONSUMER_NAME} and message_id = ${envelope.messageId}
          `;
          expect(inboxAfter1).toHaveLength(0);

          // Drive retries across 1s VisibilityTimeout until SQS RedrivePolicy (maxReceiveCount = 2) moves it to DLQ.
          let redriven: Message[] = [];
          const deadline = Date.now() + 10_000;
          while (Date.now() < deadline && redriven.length === 0) {
            await isolatedConsumer.pollOnce({ waitTimeSeconds: 1 });
            const dlqCheck = await sqs.send(
              new ReceiveMessageCommand({
                QueueUrl: queues.dlqQueueUrl,
                MaxNumberOfMessages: 10,
                WaitTimeSeconds: 0,
              }),
            );
            redriven = dlqCheck.Messages ?? [];
            if (redriven.length === 0) {
              await Bun.sleep(200);
            }
          }

          expect(redriven).toHaveLength(1);
          expect(JSON.parse(redriven[0]?.Body ?? '{}').messageId).toBe(envelope.messageId);

          // Inbox still has 0 rows because the message never succeeded.
          const inboxFinal = await db`
            select 1 from inbox_messages
            where consumer_name = ${WAGER_CONSUMER_NAME} and message_id = ${envelope.messageId}
          `;
          expect(inboxFinal).toHaveLength(0);
        } finally {
          await isolatedApp.close();
        }
      });
    },
    15_000,
  );

  test('on stop/SIGTERM finishes the in-flight message and batch-releases remaining same-group messages deterministically within timeout', async () => {
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 30, maxReceiveCount: 5 }, async (queues) => {
      const stoppingApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
        SHUTDOWN_TIMEOUT_MS: '5000',
      });
      const standbyApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      const stoppingSqs = stoppingApp.get(SQSClient);
      const originalSend = stoppingSqs.send.bind(stoppingSqs);
      let visibilityBatchCalls = 0;
      const sendSpy = spyOn(stoppingSqs, 'send').mockImplementation((command: any, options?: any) => {
        if (command instanceof ChangeMessageVisibilityBatchCommand) {
          visibilityBatchCalls += 1;
        }
        return originalSend(command, options);
      });

      try {
        const stoppingConsumer = stoppingApp.get(WagerTransactionsConsumer);
        const standbyConsumer = standbyApp.get(WagerTransactionsConsumer);

        // Two messages of the EXACT SAME MessageGroupId (wallet.id) competing for 100.00 balance:
        // env1 (BET 80.00) must win PROCESSED; env2 (BET 80.00) must run second and be REJECTED (INSUFFICIENT_FUNDS).
        const wallet = await openWallet('100.00');
        const env1 = buildWagerEnvelope(wallet, {
          externalTransactionId: `sigterm-bet-1-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });
        const env2 = buildWagerEnvelope(wallet, {
          externalTransactionId: `sigterm-bet-2-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });

        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, env1)));
        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, env2)));

        const stopStartedAt = Date.now();
        let stopDone: Promise<void> | undefined;
        const stoppingStats = await stoppingConsumer.pollOnce({
          maxNumberOfMessages: 10,
          waitTimeSeconds: 1,
          beforeCommit: () => {
            stopDone ??= stoppingConsumer.stop();
          },
        });
        await stopDone;
        const shutdownElapsedMs = Date.now() - stopStartedAt;

        // Unconditional deterministic assertions: both messages were received, #1 processed, #2 batch-released.
        expect(stoppingStats.received).toBe(2);
        expect(stoppingStats.processed).toBe(1);
        expect(stoppingStats.visibilityReleased).toBe(1);
        expect(visibilityBatchCalls).toBe(1);
        expect(shutdownElapsedMs).toBeLessThan(2_000);

        // Standby consumer immediately receives env2 without waiting for the 30s VisibilityTimeout!
        const standbyStats = await standbyConsumer.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 1 });
        expect(standbyStats.received).toBe(1);
        expect(standbyStats.processed).toBe(1);

        const [walletRow] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
        expect(walletRow).toEqual({ balance_amount: '20.00', version: 2 });

        const [tx1] = await db`
          select status, failure_code from wager_transactions where idempotency_key = ${env1.data.idempotencyKey}
        `;
        const [tx2] = await db`
          select status, failure_code from wager_transactions where idempotency_key = ${env2.data.idempotencyKey}
        `;
        expect(tx1).toEqual({ status: WagerTransactionStatus.Processed, failure_code: null });
        expect(tx2).toEqual({
          status: WagerTransactionStatus.Rejected,
          failure_code: FailureCode.InsufficientFunds,
        });
      } finally {
        sendSpy.mockRestore();
        await standbyApp.close();
        await stoppingApp.close();
      }
    });
  });

  test('enforces blockedGroups when M1 fails transiently (with and without SIGTERM) so M2 never executes or releases ahead of M1', async () => {
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 1, maxReceiveCount: 5 }, async (queues) => {
      const appA = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });
      const appB = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      try {
        const consumerA = appA.get(WagerTransactionsConsumer);
        const consumerB = appB.get(WagerTransactionsConsumer);

        // Part 1: Normal poll batch with [M1 (BET 80), M2 (BET 80)] on a 100.00 wallet.
        // If M2 ever ran before M1, M2 would win PROCESSED (leaving 20.00) and M1 would be REJECTED!
        const wallet1 = await openWallet('100.00');
        const m1 = buildWagerEnvelope(wallet1, {
          externalTransactionId: `blk-m1-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });
        const m2 = buildWagerEnvelope(wallet1, {
          externalTransactionId: `blk-m2-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });

        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, m1)));
        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, m2)));

        let failM1Once = true;
        const poll1 = await consumerA.pollOnce({
          maxNumberOfMessages: 10,
          waitTimeSeconds: 1,
          beforeCommit: (_txEm, messageId) => {
            if (failM1Once && messageId === m1.messageId) {
              failM1Once = false;
              throw new Error('simulated transient DB error on M1');
            }
          },
        });

        expect(poll1.received).toBe(2);
        expect(poll1.processed).toBe(0);
        expect(poll1.retried).toBe(2);
        expect(poll1.visibilityReleased).toBe(0);

        // Neither M1 nor M2 was committed while M1 failed.
        const txCountAfterFail = await db`
          select 1 from wager_transactions where wallet_id = ${wallet1.id} and kind <> 'OPENING'
        `;
        expect(txCountAfterFail).toHaveLength(0);

        // Wait across the 1s VisibilityTimeout: SQS redelivers [M1, M2] in strict FIFO order -> M1 PROCESSED, M2 REJECTED!
        const poll2 = await consumerA.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 3 });
        expect(poll2.received).toBe(2);
        expect(poll2.processed).toBe(2);

        const [m1Row] = await db`select status from wager_transactions where idempotency_key = ${m1.data.idempotencyKey}`;
        const [m2Row] = await db`
          select status, failure_code from wager_transactions where idempotency_key = ${m2.data.idempotencyKey}
        `;
        expect(m1Row.status).toBe(WagerTransactionStatus.Processed);
        expect(m2Row).toEqual({
          status: WagerTransactionStatus.Rejected,
          failure_code: FailureCode.InsufficientFunds,
        });

        // Part 2: M1 fails transiently AT THE SAME TIME SIGTERM/stop() arrives, with [N1 (wallet2), N2 (wallet2), K1 (wallet3)] in flight!
        // Use a 3s VisibilityTimeout queue so immediate standby check (waitTimeSeconds: 0) has deterministic headroom.
        await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 3, maxReceiveCount: 5 }, async (part2Queues) => {
          const stopAppA = await startAppInstance({
            SQS_WAGER_TRANSACTIONS_QUEUE: part2Queues.txQueueName,
            SQS_WAGER_TRANSACTIONS_DLQ: part2Queues.dlqQueueName,
            SQS_INTEGRATION_EVENTS_QUEUE: part2Queues.eventsQueueName,
          });
          const standbyAppB = await startAppInstance({
            SQS_WAGER_TRANSACTIONS_QUEUE: part2Queues.txQueueName,
            SQS_WAGER_TRANSACTIONS_DLQ: part2Queues.dlqQueueName,
            SQS_INTEGRATION_EVENTS_QUEUE: part2Queues.eventsQueueName,
          });

          try {
            const stopConsumerA = stopAppA.get(WagerTransactionsConsumer);
            const standbyConsumerB = standbyAppB.get(WagerTransactionsConsumer);

            const wallet2 = await openWallet('100.00');
            const wallet3 = await openWallet('100.00');
            const n1 = buildWagerEnvelope(wallet2, {
              externalTransactionId: `blk-sig-n1-${newId()}`,
              money: { amount: '80.00', currency: 'BRL' },
            });
            const n2 = buildWagerEnvelope(wallet2, {
              externalTransactionId: `blk-sig-n2-${newId()}`,
              money: { amount: '80.00', currency: 'BRL' },
            });
            const k1 = buildWagerEnvelope(wallet3, {
              externalTransactionId: `blk-sig-k1-${newId()}`,
              money: { amount: '15.00', currency: 'BRL' },
            });

            await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(part2Queues.txQueueUrl, n1)));
            await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(part2Queues.txQueueUrl, n2)));
            await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(part2Queues.txQueueUrl, k1)));

            let stopPromise: Promise<void> | undefined;
            const sigtermFailStats = await stopConsumerA.pollOnce({
              maxNumberOfMessages: 10,
              waitTimeSeconds: 1,
              beforeCommit: (_txEm, messageId) => {
                if (messageId === n1.messageId) {
                  stopPromise ??= stopConsumerA.stop();
                  throw new Error('simulated transient DB error on N1 during SIGTERM');
                }
              },
            });
            await stopPromise;

            expect(sigtermFailStats.received).toBe(3);
            expect(sigtermFailStats.processed).toBe(0);
            // K1 (unblocked wallet3) WAS released (1); N2 (blocked wallet2) was WITHHELD (retried = N1 + N2 = 2)!
            expect(sigtermFailStats.visibilityReleased).toBe(1);
            expect(sigtermFailStats.retried).toBe(2);

            // Immediately (WaitTimeSeconds: 0, well within the 3s VisibilityTimeout):
            // standbyConsumerB receives ONLY K1 (wallet3), and MUST NOT receive N2 out-of-order ahead of N1!
            const immediateStandby = await standbyConsumerB.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 0 });
            expect(immediateStandby.received).toBe(1);
            expect(immediateStandby.processed).toBe(1);

            const [w3AfterImmediate] = await db`select balance_amount from wallets where id = ${wallet3.id}`;
            const [w2AfterImmediate] = await db`select balance_amount from wallets where id = ${wallet2.id}`;
            expect(w3AfterImmediate.balance_amount).toBe('85.00');
            expect(w2AfterImmediate.balance_amount).toBe('100.00');

            // Long-poll across the 3s VisibilityTimeout: standbyConsumerB receives [N1, N2] in exact FIFO order (N1 wins, N2 rejected)!
            let settledWallet2 = 0;
            for (let attempt = 0; attempt < 5 && settledWallet2 < 2; attempt += 1) {
              const stats = await standbyConsumerB.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 2 });
              settledWallet2 += stats.processed;
            }
            expect(settledWallet2).toBe(2);

            const [n1Row] = await db`select status from wager_transactions where idempotency_key = ${n1.data.idempotencyKey}`;
            const [n2Row] = await db`
              select status, failure_code from wager_transactions where idempotency_key = ${n2.data.idempotencyKey}
            `;
            expect(n1Row.status).toBe(WagerTransactionStatus.Processed);
            expect(n2Row).toEqual({
              status: WagerTransactionStatus.Rejected,
              failure_code: FailureCode.InsufficientFunds,
            });
          } finally {
            await standbyAppB.close();
            await stopAppA.close();
          }
        });
      } finally {
        await appB.close();
        await appA.close();
      }
    });
  }, 20_000);

  test('aborts half-open ReceiveMessage calls via network timeout and retries in runLoop, while exiting cleanly on stop()', async () => {
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 10, maxReceiveCount: 5 }, async (queues) => {
      const timeoutApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
        SQS_WAIT_TIME_SECONDS: '0',
        SQS_CALL_TIMEOUT_MS: '150',
      });

      const appSqs = timeoutApp.get(SQSClient);
      const realSend = appSqs.send.bind(appSqs);
      let stalledReceiveCalls = 0;
      let shouldStallReceive = true;

      const sendSpy = spyOn(appSqs, 'send').mockImplementation((command: any, options?: any) => {
        if (command instanceof ReceiveMessageCommand && shouldStallReceive) {
          stalledReceiveCalls += 1;
          // Simulate a half-open TCP socket that only settles when abortSignal fires.
          return new Promise((_resolve, reject) => {
            const sig = options?.abortSignal as AbortSignal | undefined;
            const onAbort = () => {
              const reason = sig?.reason instanceof Error ? sig.reason : new Error('Aborted');
              reject(reason);
            };
            if (sig?.aborted) {
              onAbort();
              return;
            }
            sig?.addEventListener('abort', onAbort, { once: true });
          });
        }
        return realSend(command, options);
      });

      try {
        const consumer = timeoutApp.get(WagerTransactionsConsumer);

        // 1. Direct pollOnce surfaces the network timeout instead of hanging indefinitely.
        const t0 = Date.now();
        await expect(consumer.pollOnce({ waitTimeSeconds: 0 })).rejects.toThrow();
        expect(Date.now() - t0).toBeLessThan(2_000);
        expect(stalledReceiveCalls).toBe(1);

        // 2. Live background loop survives a half-open timeout, backs off, and processes real traffic once network recovers.
        const wallet = await openWallet('100.00');
        const env = buildWagerEnvelope(wallet, { money: { amount: '20.00', currency: 'BRL' } });
        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, env)));

        const beforeLoopStalls = stalledReceiveCalls;
        consumer.start();

        // Wait until at least 1 loop poll timed out, then restore healthy ReceiveMessage delivery.
        const stallDeadline = Date.now() + 3_000;
        while (stalledReceiveCalls <= beforeLoopStalls && Date.now() < stallDeadline) {
          await Bun.sleep(25);
        }
        expect(stalledReceiveCalls).toBeGreaterThan(beforeLoopStalls);
        shouldStallReceive = false;

        // Consumer loop automatically retries after backoff and processes the queued message!
        const processDeadline = Date.now() + 4_000;
        let processed = false;
        while (Date.now() < processDeadline) {
          const [wRow] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
          if (wRow.version === 2 && wRow.balance_amount === '80.00') {
            processed = true;
            break;
          }
          await Bun.sleep(50);
        }
        expect(processed).toBe(true);

        // 3. Explicit stop() aborts idle polling cleanly and starts no further ReceiveMessage calls afterwards.
        await consumer.stop();
        let postStopReceiveCalls = 0;
        sendSpy.mockImplementation((command: any, options?: any) => {
          if (command instanceof ReceiveMessageCommand) {
            postStopReceiveCalls += 1;
          }
          return realSend(command, options);
        });
        await Bun.sleep(350);
        expect(postStopReceiveCalls).toBe(0);
      } finally {
        sendSpy.mockRestore();
        await timeoutApp.close();
      }
    });
  }, 15_000);

  test('releases visibility in per-group FIFO waves: blocks same-group M2 when M1 fails in wave 1, releases M2 in wave 2 when M1 succeeds, and lets independent groups advance', async () => {
    // Subcase A: M1 fails in wave 1 -> M2 of the same group is NEVER sent to SQS (preventing intra-batch FIFO inversion),
    // while independent walletB releases K1 in wave 1 AND K2 in wave 2!
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 3, maxReceiveCount: 5 }, async (queues) => {
      const stoppingApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });
      const standbyApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      const stoppingSqs = stoppingApp.get(SQSClient);
      const realSend = stoppingSqs.send.bind(stoppingSqs);
      const receiptHandleToGroup = new Map<string, string>();
      const receiptHandleToMessageId = new Map<string, string>();
      const waveBatchGroupIds: string[][] = [];

      const sendSpy = spyOn(stoppingSqs, 'send').mockImplementation(async (command: any, options?: any) => {
        if (command instanceof ReceiveMessageCommand) {
          const res = (await realSend(command, options)) as { Messages?: Message[] };
          for (const msg of res.Messages ?? []) {
            if (msg.ReceiptHandle !== undefined && msg.Attributes?.MessageGroupId !== undefined) {
              receiptHandleToGroup.set(msg.ReceiptHandle, msg.Attributes.MessageGroupId);
            }
            if (msg.ReceiptHandle !== undefined && msg.Body !== undefined) {
              receiptHandleToMessageId.set(msg.ReceiptHandle, JSON.parse(msg.Body).messageId);
            }
          }
          return res;
        }
        if (command instanceof ChangeMessageVisibilityBatchCommand) {
          const entries = command.input.Entries ?? [];
          const groupsInBatch = entries.map((e: any) => receiptHandleToGroup.get(e.ReceiptHandle) ?? 'unknown');
          waveBatchGroupIds.push(groupsInBatch);

          // Every single wave batch MUST contain at most 1 entry per MessageGroupId!
          expect(new Set(groupsInBatch).size).toBe(groupsInBatch.length);

          if (waveBatchGroupIds.length === 1) {
            // Simulate partial SQS failure on walletA's head (M1), while forwarding walletB's head (K1) to real SQS!
            const succeedEntries = entries.filter(
              (e: any) => receiptHandleToGroup.get(e.ReceiptHandle) !== walletA.id,
            );
            const failEntries = entries.filter(
              (e: any) => receiptHandleToGroup.get(e.ReceiptHandle) === walletA.id,
            );
            expect(failEntries).toHaveLength(1);
            expect(succeedEntries).toHaveLength(1);

            const realRes = await realSend(
              new ChangeMessageVisibilityBatchCommand({
                QueueUrl: command.input.QueueUrl,
                Entries: succeedEntries,
              }),
              options,
            );
            return {
              ...realRes,
              Successful: realRes.Successful ?? [],
              Failed: failEntries.map((e: any) => ({
                Id: e.Id,
                Code: 'InternalError',
                SenderFault: false,
                Message: 'simulated wave-1 visibility failure on M1',
              })),
            };
          }
          return realSend(command, options);
        }
        return realSend(command, options);
      });

      const triggerWallet = await openWallet('100.00');
      const walletA = await openWallet('100.00');
      const walletB = await openWallet('100.00');

      try {
        const stoppingConsumer = stoppingApp.get(WagerTransactionsConsumer);
        const standbyConsumer = standbyApp.get(WagerTransactionsConsumer);

        const m0 = buildWagerEnvelope(triggerWallet, { money: { amount: '10.00', currency: 'BRL' } });
        const m1 = buildWagerEnvelope(walletA, {
          externalTransactionId: `wave-fail-m1-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });
        const m2 = buildWagerEnvelope(walletA, {
          externalTransactionId: `wave-fail-m2-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });
        const k1 = buildWagerEnvelope(walletB, {
          externalTransactionId: `wave-ok-k1-${newId()}`,
          money: { amount: '15.00', currency: 'BRL' },
        });
        const k2 = buildWagerEnvelope(walletB, {
          externalTransactionId: `wave-ok-k2-${newId()}`,
          money: { amount: '20.00', currency: 'BRL' },
        });

        for (const env of [m0, m1, m2, k1, k2]) {
          await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, env)));
        }

        let stopPromise: Promise<void> | undefined;
        const stats = await stoppingConsumer.pollOnce({
          maxNumberOfMessages: 10,
          waitTimeSeconds: 1,
          beforeCommit: (_txEm, messageId) => {
            if (messageId === m0.messageId) {
              stopPromise ??= stoppingConsumer.stop();
            }
          },
        });
        await stopPromise;

        // Wave 1 sent [walletA (M1), walletB (K1)]; M1 failed -> Wave 2 sent ONLY [walletB (K2)] (M2 withheld)!
        expect(waveBatchGroupIds).toEqual([[walletA.id, walletB.id], [walletB.id]]);
        expect(stats).toEqual({
          received: 5,
          processed: 1,
          duplicates: 0,
          conflicts: 0,
          deadLettered: 0,
          retried: 2,
          visibilityReleased: 2,
        });

        // Standby immediately receives ONLY walletB's [K1, K2] (0 messages of walletA — M2 did NOT invert ahead of M1!).
        const immediateStandby = await standbyConsumer.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 0 });
        expect(immediateStandby.received).toBe(2);
        expect(immediateStandby.processed).toBe(2);

        const [wBAfterImmediate] = await db`select balance_amount, version from wallets where id = ${walletB.id}`;
        const [wAAfterImmediate] = await db`select balance_amount, version from wallets where id = ${walletA.id}`;
        expect(wBAfterImmediate).toEqual({ balance_amount: '65.00', version: 3 });
        expect(wAAfterImmediate).toEqual({ balance_amount: '100.00', version: 1 });

        // After 3s VisibilityTimeout, standby receives [M1, M2] in strict FIFO order: M1 wins PROCESSED, M2 REJECTED!
        let settledA = 0;
        for (let attempt = 0; attempt < 5 && settledA < 2; attempt += 1) {
          const s = await standbyConsumer.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 2 });
          settledA += s.processed;
        }
        expect(settledA).toBe(2);

        const [m1Row] = await db`select status from wager_transactions where idempotency_key = ${m1.data.idempotencyKey}`;
        const [m2Row] = await db`
          select status, failure_code from wager_transactions where idempotency_key = ${m2.data.idempotencyKey}
        `;
        expect(m1Row.status).toBe(WagerTransactionStatus.Processed);
        expect(m2Row).toEqual({
          status: WagerTransactionStatus.Rejected,
          failure_code: FailureCode.InsufficientFunds,
        });
      } finally {
        sendSpy.mockRestore();
        await standbyApp.close();
        await stoppingApp.close();
      }
    });

    // Subcase B: M1 succeeds in wave 1 -> M2 of the same group IS released in wave 2 (30s VisibilityTimeout bypassed cleanly!).
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 30, maxReceiveCount: 5 }, async (queues) => {
      const stoppingApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });
      const standbyApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      const stoppingSqs = stoppingApp.get(SQSClient);
      const realSend = stoppingSqs.send.bind(stoppingSqs);
      const rhToMsgId = new Map<string, string>();
      const wavesSentMessageIds: string[][] = [];

      const sendSpy = spyOn(stoppingSqs, 'send').mockImplementation(async (command: any, options?: any) => {
        if (command instanceof ReceiveMessageCommand) {
          const res = (await realSend(command, options)) as { Messages?: Message[] };
          for (const msg of res.Messages ?? []) {
            if (msg.ReceiptHandle !== undefined && msg.Body !== undefined) {
              rhToMsgId.set(msg.ReceiptHandle, JSON.parse(msg.Body).messageId);
            }
          }
          return res;
        }
        if (command instanceof ChangeMessageVisibilityBatchCommand) {
          const entries = command.input.Entries ?? [];
          wavesSentMessageIds.push(entries.map((e: any) => rhToMsgId.get(e.ReceiptHandle) ?? 'unknown'));
          return realSend(command, options);
        }
        return realSend(command, options);
      });

      try {
        const stoppingConsumer = stoppingApp.get(WagerTransactionsConsumer);
        const standbyConsumer = standbyApp.get(WagerTransactionsConsumer);

        const triggerWallet = await openWallet('100.00');
        const walletC = await openWallet('100.00');
        const walletD = await openWallet('100.00');

        const c0 = buildWagerEnvelope(triggerWallet, { money: { amount: '10.00', currency: 'BRL' } });
        const c1 = buildWagerEnvelope(walletC, {
          externalTransactionId: `wave-succ-c1-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });
        const c2 = buildWagerEnvelope(walletC, {
          externalTransactionId: `wave-succ-c2-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });
        const d1 = buildWagerEnvelope(walletD, {
          externalTransactionId: `wave-succ-d1-${newId()}`,
          money: { amount: '25.00', currency: 'BRL' },
        });

        for (const env of [c0, c1, c2, d1]) {
          await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, env)));
        }

        let stopDone: Promise<void> | undefined;
        const stats = await stoppingConsumer.pollOnce({
          maxNumberOfMessages: 10,
          waitTimeSeconds: 1,
          beforeCommit: (_txEm, messageId) => {
            if (messageId === c0.messageId) {
              stopDone ??= stoppingConsumer.stop();
            }
          },
        });
        await stopDone;

        // Wave 1 released [c1, d1]; after c1 succeeded, Wave 2 released [c2]!
        expect(wavesSentMessageIds).toEqual([[c1.messageId, d1.messageId], [c2.messageId]]);
        expect(stats.received).toBe(4);
        expect(stats.processed).toBe(1);
        expect(stats.visibilityReleased).toBe(3);
        expect(stats.retried).toBe(0);

        // Standby immediately receives all 3 released messages without waiting for the 30s VisibilityTimeout!
        const standbyStats = await standbyConsumer.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 1 });
        expect(standbyStats.received).toBe(3);
        expect(standbyStats.processed).toBe(3);

        const [c1Row] = await db`select status from wager_transactions where idempotency_key = ${c1.data.idempotencyKey}`;
        const [c2Row] = await db`
          select status, failure_code from wager_transactions where idempotency_key = ${c2.data.idempotencyKey}
        `;
        const [wDRow] = await db`select balance_amount from wallets where id = ${walletD.id}`;
        expect(c1Row.status).toBe(WagerTransactionStatus.Processed);
        expect(c2Row).toEqual({
          status: WagerTransactionStatus.Rejected,
          failure_code: FailureCode.InsufficientFunds,
        });
        expect(wDRow.balance_amount).toBe('75.00');
      } finally {
        sendSpy.mockRestore();
        await standbyApp.close();
        await stoppingApp.close();
      }
    });
  }, 20_000);

  test('withholds same-group M2 during SIGTERM when committed M1 fails DeleteMessageCommand, then deduplicates M1 via Inbox and preserves M2 order', async () => {
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 3, maxReceiveCount: 5 }, async (queues) => {
      const stoppingApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });
      const standbyApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      const stoppingSqs = stoppingApp.get(SQSClient);
      const realSend = stoppingSqs.send.bind(stoppingSqs);
      let deleteFailuresInjected = 0;

      const sendSpy = spyOn(stoppingSqs, 'send').mockImplementation(async (command: any, options?: any) => {
        if (command instanceof DeleteMessageCommand && deleteFailuresInjected === 0) {
          deleteFailuresInjected += 1;
          throw Object.assign(new Error('simulated post-commit DeleteMessage network error'), { code: 'ECONNRESET' });
        }
        return realSend(command, options);
      });

      try {
        const stoppingConsumer = stoppingApp.get(WagerTransactionsConsumer);
        const standbyConsumer = standbyApp.get(WagerTransactionsConsumer);

        const wallet = await openWallet('100.00');
        const otherWallet = await openWallet('100.00');
        const m1 = buildWagerEnvelope(wallet, {
          externalTransactionId: `del-fail-m1-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });
        const m2 = buildWagerEnvelope(wallet, {
          externalTransactionId: `del-fail-m2-${newId()}`,
          money: { amount: '80.00', currency: 'BRL' },
        });
        const otherMsg = buildWagerEnvelope(otherWallet, {
          externalTransactionId: `del-fail-other-${newId()}`,
          money: { amount: '10.00', currency: 'BRL' },
        });

        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, m1)));
        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, m2)));
        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, otherMsg)));

        let stopDone: Promise<void> | undefined;
        const stats = await stoppingConsumer.pollOnce({
          maxNumberOfMessages: 10,
          waitTimeSeconds: 1,
          beforeCommit: (_txEm, messageId) => {
            if (messageId === m1.messageId) {
              stopDone ??= stoppingConsumer.stop();
            }
          },
        });
        await stopDone;

        // M1 committed in PostgreSQL (processed = 1), DeleteMessage failed -> M1 still in-flight in SQS ->
        // M2 of `wallet` MUST be withheld (retried = 1), while `otherWallet` IS released (visibilityReleased = 1)!
        expect(deleteFailuresInjected).toBe(1);
        expect(stats.received).toBe(3);
        expect(stats.processed).toBe(1);
        expect(stats.visibilityReleased).toBe(1);
        expect(stats.retried).toBe(1);

        // Immediately (WaitTimeSeconds: 0): standby receives ONLY otherMsg!
        const immediateStandby = await standbyConsumer.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 0 });
        expect(immediateStandby.received).toBe(1);
        expect(immediateStandby.processed).toBe(1);

        const [otherAfter] = await db`select balance_amount from wallets where id = ${otherWallet.id}`;
        expect(otherAfter.balance_amount).toBe('90.00');

        // After 3s VisibilityTimeout: SQS redelivers [M1, M2] in order -> M1 deduplicated by Inbox, M2 REJECTED!
        let totalDuplicates = 0;
        let totalProcessed = 0;
        for (let attempt = 0; attempt < 5 && totalDuplicates + totalProcessed < 2; attempt += 1) {
          const s = await standbyConsumer.pollOnce({ maxNumberOfMessages: 10, waitTimeSeconds: 2 });
          totalDuplicates += s.duplicates;
          totalProcessed += s.processed;
        }
        expect(totalDuplicates).toBe(1);
        expect(totalProcessed).toBe(1);

        const [walletFinal] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
        expect(walletFinal).toEqual({ balance_amount: '20.00', version: 2 });

        const [m1Row] = await db`select status from wager_transactions where idempotency_key = ${m1.data.idempotencyKey}`;
        const [m2Row] = await db`
          select status, failure_code from wager_transactions where idempotency_key = ${m2.data.idempotencyKey}
        `;
        expect(m1Row.status).toBe(WagerTransactionStatus.Processed);
        expect(m2Row).toEqual({
          status: WagerTransactionStatus.Rejected,
          failure_code: FailureCode.InsufficientFunds,
        });
      } finally {
        sendSpy.mockRestore();
        await standbyApp.close();
        await stoppingApp.close();
      }
    });
  }, 20_000);

  test('enforces wave chunking (>10 groups), transport failure uncertainty, and missing ReceiptHandle group-tail blocking', async () => {
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 30, maxReceiveCount: 5 }, async (queues) => {
      const batchApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      const appSqs = batchApp.get(SQSClient);
      const realSend = appSqs.send.bind(appSqs);
      const consumer = batchApp.get(WagerTransactionsConsumer);
      const wallet = await openWallet('100.00');

      try {
        // 1. >10 groups chunking + multi-wave:
        // 12 distinct groups (grp-0..grp-11), where grp-0 has 2 messages (ok) and grp-3 has 2 messages (head fails in wave 1 chunk 1).
        // Total remaining = 14 messages:
        // - Wave 1 Chunk 1: 10 entries (grp-0..grp-9; grp-3 fails -> 9 released, 2 in grp-3 withheld)
        // - Wave 1 Chunk 2: 2 entries (grp-10..grp-11 -> 2 released)
        // - Wave 2 Chunk 1: 1 entry (grp-0's 2nd message ONLY; grp-3's 2nd message was blocked! -> 1 released)
        const trigger1 = buildWagerEnvelope(wallet, { money: { amount: '5.00', currency: 'BRL' } });
        const batchReceiptHandles: string[][] = [];
        const spy1 = spyOn(appSqs, 'send').mockImplementation(async (command: any, options?: any) => {
          if (command instanceof ReceiveMessageCommand) {
            const remaining: Message[] = [];
            for (let g = 0; g < 12; g += 1) {
              remaining.push({
                MessageId: `c-g${g}-m1`,
                ReceiptHandle: `rh-g${g}-m1`,
                Body: '{}',
                Attributes: { MessageGroupId: `grp-${g}` },
              });
              if (g === 0 || g === 3) {
                remaining.push({
                  MessageId: `c-g${g}-m2`,
                  ReceiptHandle: `rh-g${g}-m2`,
                  Body: '{}',
                  Attributes: { MessageGroupId: `grp-${g}` },
                });
              }
            }
            return {
              Messages: [
                {
                  MessageId: 'syn-trigger-1',
                  ReceiptHandle: undefined,
                  Body: JSON.stringify(trigger1),
                  Attributes: { MessageGroupId: wallet.id },
                },
                ...remaining,
              ],
            };
          }
          if (command instanceof ChangeMessageVisibilityBatchCommand) {
            const entries = command.input.Entries ?? [];
            batchReceiptHandles.push(entries.map((e: any) => e.ReceiptHandle));
            if (batchReceiptHandles.length === 1) {
              return {
                Successful: entries.filter((e: any) => e.ReceiptHandle !== 'rh-g3-m1').map((e: any) => ({ Id: e.Id })),
                Failed: [
                  {
                    Id: entries.find((e: any) => e.ReceiptHandle === 'rh-g3-m1')!.Id,
                    Code: 'InternalError',
                    SenderFault: false,
                    Message: 'simulated partial failure on grp-3 head',
                  },
                ],
              };
            }
            return {
              Successful: entries.map((e: any) => ({ Id: e.Id })),
              Failed: [],
            };
          }
          return realSend(command, options);
        });

        try {
          let stopDone: Promise<void> | undefined;
          const stats1 = await consumer.pollOnce({
            beforeCommit: () => {
              stopDone ??= consumer.stop();
            },
          });
          await stopDone;

          expect(batchReceiptHandles.map((b) => b.length)).toEqual([10, 2, 1]);
          expect(batchReceiptHandles[2]).toEqual(['rh-g0-m2']);
          expect(stats1.received).toBe(15);
          expect(stats1.processed).toBe(1);
          expect(stats1.visibilityReleased).toBe(12);
          expect(stats1.retried).toBe(2);
        } finally {
          spy1.mockRestore();
        }

        // 2. Transport failure (`ECONNRESET`) treats all entries in batch as uncertain and blocks their group tails:
        const trigger2 = buildWagerEnvelope(wallet, { money: { amount: '5.00', currency: 'BRL' } });
        let transportBatchCalls = 0;
        const spy2 = spyOn(appSqs, 'send').mockImplementation(async (command: any, options?: any) => {
          if (command instanceof ReceiveMessageCommand) {
            return {
              Messages: [
                {
                  MessageId: 'syn-trigger-2',
                  ReceiptHandle: undefined,
                  Body: JSON.stringify(trigger2),
                  Attributes: { MessageGroupId: wallet.id },
                },
                { MessageId: 'tf-a1', ReceiptHandle: 'rh-tf-a1', Body: '{}', Attributes: { MessageGroupId: 'grp-tf-a' } },
                { MessageId: 'tf-a2', ReceiptHandle: 'rh-tf-a2', Body: '{}', Attributes: { MessageGroupId: 'grp-tf-a' } },
                { MessageId: 'tf-b1', ReceiptHandle: 'rh-tf-b1', Body: '{}', Attributes: { MessageGroupId: 'grp-tf-b' } },
              ],
            };
          }
          if (command instanceof ChangeMessageVisibilityBatchCommand) {
            transportBatchCalls += 1;
            throw Object.assign(new Error('simulated SQS visibility transport ECONNRESET'), { code: 'ECONNRESET' });
          }
          return realSend(command, options);
        });

        try {
          let stopDone: Promise<void> | undefined;
          const stats2 = await consumer.pollOnce({
            beforeCommit: () => {
              stopDone ??= consumer.stop();
            },
          });
          await stopDone;

          // Wave 1 threw -> grp-tf-a and grp-tf-b marked uncertain/blocked -> Wave 2 NEVER sent tf-a2!
          expect(transportBatchCalls).toBe(1);
          expect(stats2.visibilityReleased).toBe(0);
          expect(stats2.retried).toBe(3);
        } finally {
          spy2.mockRestore();
        }

        // 3. Missing ReceiptHandle counts as withheld AND blocks later messages of the same group:
        const trigger3 = buildWagerEnvelope(wallet, { money: { amount: '5.00', currency: 'BRL' } });
        const missingRhSentHandles: string[][] = [];
        const spy3 = spyOn(appSqs, 'send').mockImplementation(async (command: any, options?: any) => {
          if (command instanceof ReceiveMessageCommand) {
            return {
              Messages: [
                {
                  MessageId: 'syn-trigger-3',
                  ReceiptHandle: undefined,
                  Body: JSON.stringify(trigger3),
                  Attributes: { MessageGroupId: wallet.id },
                },
                {
                  MessageId: 'no-rh-m1',
                  ReceiptHandle: undefined,
                  Body: '{}',
                  Attributes: { MessageGroupId: 'grp-missing-rh' },
                },
                {
                  MessageId: 'has-rh-m2-behind-missing',
                  ReceiptHandle: 'rh-must-not-release',
                  Body: '{}',
                  Attributes: { MessageGroupId: 'grp-missing-rh' },
                },
                {
                  MessageId: 'indep-ok-1',
                  ReceiptHandle: 'rh-indep-ok-1',
                  Body: '{}',
                  Attributes: { MessageGroupId: 'grp-indep-ok' },
                },
              ],
            };
          }
          if (command instanceof ChangeMessageVisibilityBatchCommand) {
            const entries = command.input.Entries ?? [];
            missingRhSentHandles.push(entries.map((e: any) => e.ReceiptHandle));
            return {
              Successful: entries.map((e: any) => ({ Id: e.Id })),
              Failed: [],
            };
          }
          return realSend(command, options);
        });

        try {
          let stopDone: Promise<void> | undefined;
          const stats3 = await consumer.pollOnce({
            beforeCommit: () => {
              stopDone ??= consumer.stop();
            },
          });
          await stopDone;

          expect(missingRhSentHandles).toEqual([['rh-indep-ok-1']]);
          expect(stats3.visibilityReleased).toBe(1);
          expect(stats3.retried).toBe(2);
        } finally {
          spy3.mockRestore();
        }
      } finally {
        await batchApp.close();
      }
    });
  });

  test('shares a single non-resetting global timeout budget across visibility waves and withholds unconfirmed messages when exhausted', async () => {
    await withIsolatedFifoQueues({ visibilityTimeoutSeconds: 30, maxReceiveCount: 5 }, async (queues) => {
      const budgetApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
        SQS_CALL_TIMEOUT_MS: '120',
        SHUTDOWN_TIMEOUT_MS: '5000',
      });

      const appSqs = budgetApp.get(SQSClient);
      const realSend = appSqs.send.bind(appSqs);
      const wallet = await openWallet('100.00');
      const triggerEnv = buildWagerEnvelope(wallet, { money: { amount: '5.00', currency: 'BRL' } });

      const seenSignals: AbortSignal[] = [];
      const seenBatchHandles: string[][] = [];

      const sendSpy = spyOn(appSqs, 'send').mockImplementation(async (command: any, options?: any) => {
        if (command instanceof ReceiveMessageCommand) {
          return {
            Messages: [
              {
                MessageId: 'budget-trigger',
                ReceiptHandle: undefined,
                Body: JSON.stringify(triggerEnv),
                Attributes: { MessageGroupId: wallet.id },
              },
              { MessageId: 'b-m1', ReceiptHandle: 'rh-b-m1', Body: '{}', Attributes: { MessageGroupId: 'grp-budget' } },
              { MessageId: 'b-m2', ReceiptHandle: 'rh-b-m2', Body: '{}', Attributes: { MessageGroupId: 'grp-budget' } },
              { MessageId: 'b-m3', ReceiptHandle: 'rh-b-m3', Body: '{}', Attributes: { MessageGroupId: 'grp-budget' } },
            ],
          };
        }
        if (command instanceof ChangeMessageVisibilityBatchCommand) {
          const entries = command.input.Entries ?? [];
          const sig = options?.abortSignal as AbortSignal;
          seenSignals.push(sig);
          seenBatchHandles.push(entries.map((e: any) => e.ReceiptHandle));

          if (seenBatchHandles.length === 1) {
            // Wave 1 succeeds immediately (b-m1 confirmed), leaving the shared 120ms budget active for Wave 2.
            return {
              Successful: entries.map((e: any) => ({ Id: e.Id })),
              Failed: [],
            };
          }

          // Wave 2 stalls until the SHARED AbortSignal from Wave 1 expires (~120ms total).
          return new Promise((_resolve, reject) => {
            const onAbort = () => {
              reject(sig.reason instanceof Error ? sig.reason : new Error('Shared budget aborted'));
            };
            if (sig.aborted) {
              onAbort();
              return;
            }
            sig.addEventListener('abort', onAbort, { once: true });
          });
        }
        return realSend(command, options);
      });

      try {
        const consumer = budgetApp.get(WagerTransactionsConsumer);
        const startedAt = Date.now();
        let stopDone: Promise<void> | undefined;
        const stats = await consumer.pollOnce({
          beforeCommit: () => {
            stopDone ??= consumer.stop();
          },
        });
        await stopDone;
        const elapsedMs = Date.now() - startedAt;

        // Wave 1 and Wave 2 received the EXACT SAME AbortSignal instance (timeout was NOT reset per wave),
        // Wave 3 (rh-b-m3) was NEVER called, and both unconfirmed b-m2 + b-m3 were withheld!
        expect(seenBatchHandles).toEqual([['rh-b-m1'], ['rh-b-m2']]);
        expect(seenSignals).toHaveLength(2);
        expect(seenSignals[0]).toBe(seenSignals[1]);
        expect(elapsedMs).toBeLessThan(1_500);
        expect(stats.visibilityReleased).toBe(1);
        expect(stats.retried).toBe(2);
      } finally {
        sendSpy.mockRestore();
        await budgetApp.close();
      }
    });
  });
});

describe('4.3 — Outbox Publisher (SKIP LOCKED, sequence_number ordering, retry, quarantine, crash recovery)', () => {
  test('publishes pending events in strict per-wallet sequence_number order and marks published_at only after SQS confirms', async () => {
    await withIsolatedFifoQueues({}, async (queues) => {
      const pubApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      try {
        const publisher = pubApp.get(OutboxPublisherService);
        // Mark pre-existing outbox events from earlier tests published on main publisher first so only new events remain!
        await publishAllDueOutbox(publisher1);
        await drainQueue(sqs, eventsQueueUrl);

        const wallet = await openWallet('100.00');
        const providerId = `prov-${newId().slice(-8)}`;
        await submitHttpWager({
          providerId,
          externalTransactionId: 'seq-bet-1',
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-seq-1',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '30.00', currency: 'BRL' },
        });
        await submitHttpWager({
          providerId,
          externalTransactionId: 'seq-win-1',
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-seq-1',
          gameId: 'fortune-chimp',
          kind: 'WIN',
          money: { amount: '50.00', currency: 'BRL' },
          referenceExternalTransactionId: 'seq-bet-1',
        });
        await submitHttpWager({
          providerId,
          externalTransactionId: 'seq-loss-1',
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-seq-1',
          gameId: 'fortune-chimp',
          kind: 'LOSS',
          money: { amount: '0.00', currency: 'BRL' },
          referenceExternalTransactionId: 'seq-bet-1',
        });

        const expectedDbEvents = await db`
          select id, sequence_number, event_type, payload
          from outbox_messages
          where aggregate_id = ${wallet.id}
          order by sequence_number asc
        `;
        // OPENING (2) + BET (2) + WIN (2) + LOSS (1) = 7 causal events on this wallet.
        expect(expectedDbEvents).toHaveLength(7);

        // Each publishBatch publishes at most 1 head event for this walletId, preserving causal sequence!
        const publishedCount = await publishAllDueOutbox(publisher);
        expect(publishedCount).toBe(7);

        const unpublishedLeft = await db`
          select 1 from outbox_messages where aggregate_id = ${wallet.id} and published_at is null
        `;
        expect(unpublishedLeft).toHaveLength(0);

        const delivered = await drainQueue(sqs, queues.eventsQueueUrl);
        const walletDelivered = delivered
          .map((m) => ({
            groupId: m.Attributes?.MessageGroupId,
            envelope: JSON.parse(m.Body ?? '{}') as Json,
          }))
          .filter((item) => item.envelope.aggregateId === wallet.id);

        expect(walletDelivered).toHaveLength(7);
        for (let i = 0; i < expectedDbEvents.length; i += 1) {
          expect(walletDelivered[i]?.groupId).toBe(wallet.id);
          expect(walletDelivered[i]?.envelope).toEqual(expectedDbEvents[i].payload);
          expect(walletDelivered[i]?.envelope.eventId).toBe(expectedDbEvents[i].id);
        }
      } finally {
        await pubApp.close();
      }
    });
  });

  test('schedules backoff on SQS failure without losing events, blocks later events of the same aggregate, quarantines at 10 attempts, and recovers via requeueQuarantinedAggregate', async () => {
    await publishAllDueOutbox(publisher1);
    await drainQueue(sqs, eventsQueueUrl);

    await withIsolatedFifoQueues({}, async (queues) => {
      const failingApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: `missing-events-${newId().slice(-8)}.fifo`,
      });
      const healthyApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      try {
        const failingPublisher = failingApp.get(OutboxPublisherService);
        const healthyPublisher = healthyApp.get(OutboxPublisherService);

        const walletA = await openWallet('50.00'); // Creates 2 events: seq 1 (Processed) and seq 2 (BalanceChanged)
        const baseNow = new Date();

        // Attempt 1 fails -> head (seq 1) gets attempts=1, next_attempt_at = baseNow + 1000ms; seq 2 stays attempts=0!
        const fail1 = await failingPublisher.publishBatch({ now: baseNow });
        expect(fail1.published).toBe(0);
        expect(fail1.failed).toBe(1);

        const rowsAfterFail1 = await db`
          select id, sequence_number, attempts, next_attempt_at, published_at
          from outbox_messages
          where aggregate_id = ${walletA.id}
          order by sequence_number asc
        `;
        expect(rowsAfterFail1).toHaveLength(2);
        expect(rowsAfterFail1[0].attempts).toBe(1);
        expect(rowsAfterFail1[0].published_at).toBeNull();
        expect(new Date(rowsAfterFail1[0].next_attempt_at).getTime()).toBe(baseNow.getTime() + 1_000);
        expect(rowsAfterFail1[1].attempts).toBe(0);
        expect(rowsAfterFail1[1].published_at).toBeNull();

        // Even with a healthy publisher, running BEFORE head's next_attempt_at MUST NOT skip head and publish seq 2!
        const premature = await healthyPublisher.publishBatch({ now: new Date(baseNow.getTime() + 500) });
        expect(premature.selected).toBe(0);
        expect(premature.published).toBe(0);

        // Meanwhile, an independent walletB publishes immediately without head-of-line blocking from walletA!
        const walletB = await openWallet('25.00');
        const walletBPublished = await publishAllDueOutbox(healthyPublisher, new Date(baseNow.getTime() + 500));
        expect(walletBPublished).toBe(2);

        // Drive walletA's head up to OUTBOX_MAX_AUTO_ATTEMPTS (10) -> quarantined!
        let cursorTime = new Date(baseNow.getTime() + 600_000);
        for (let attempt = 2; attempt <= OUTBOX_MAX_AUTO_ATTEMPTS; attempt += 1) {
          const stats = await failingPublisher.publishBatch({ now: cursorTime });
          expect(stats.failed).toBe(1);
          if (attempt === OUTBOX_MAX_AUTO_ATTEMPTS) {
            expect(stats.quarantined).toBe(1);
          }
          cursorTime = new Date(cursorTime.getTime() + 600_000);
        }

        const [quarantinedHead] = await db`
          select id, attempts, next_attempt_at, published_at
          from outbox_messages
          where id = ${rowsAfterFail1[0].id}
        `;
        expect(quarantinedHead.attempts).toBe(OUTBOX_MAX_AUTO_ATTEMPTS);
        expect(new Date(quarantinedHead.next_attempt_at).getTime()).toBe(OUTBOX_QUARANTINED_UNTIL_MS);
        expect(quarantinedHead.published_at).toBeNull();

        // Automatic healthy publisher skips quarantined walletA even far in the future.
        expect((await healthyPublisher.publishBatch({ now: cursorTime })).selected).toBe(0);

        // Operator requeues quarantined aggregate -> keeps monotonic attempts = 10 and publishes seq 1 then seq 2 in order!
        const requeuedId = await healthyPublisher.requeueQuarantinedAggregate(walletA.id, cursorTime);
        expect(requeuedId).toBe(rowsAfterFail1[0].id);

        const recoveredCount = await publishAllDueOutbox(healthyPublisher, cursorTime);
        expect(recoveredCount).toBe(2);

        const delivered = await drainQueue(sqs, queues.eventsQueueUrl);
        const walletAEvents = delivered
          .map((m) => JSON.parse(m.Body ?? '{}') as Json)
          .filter((e) => e.aggregateId === walletA.id);
        expect(walletAEvents.map((e) => e.eventId)).toEqual([rowsAfterFail1[0].id, rowsAfterFail1[1].id]);
      } finally {
        await healthyApp.close();
        await failingApp.close();
      }
    });
  });

  test('supports concurrent publishers via FOR UPDATE SKIP LOCKED without losing or reordering per-wallet events', async () => {
    await publishAllDueOutbox(publisher1);
    await drainQueue(sqs, eventsQueueUrl);

    await withIsolatedFifoQueues({}, async (queues) => {
      const pubApps = await Promise.all([
        startAppInstance({
          SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
          SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
          SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
        }),
        startAppInstance({
          SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
          SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
          SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
        }),
        startAppInstance({
          SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
          SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
          SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
        }),
      ]);

      try {
        const publishers = pubApps.map((a) => a.get(OutboxPublisherService));
        const wallets = await Promise.all(Array.from({ length: 8 }, () => openWallet('100.00')));

        // Submit 2 bets per wallet -> 2 (OPENING) + 4 (2 BETs) = 6 events per wallet * 8 wallets = 48 events!
        await Promise.all(
          wallets.flatMap((w, idx) => [
            submitHttpWager({
              providerId: 'prov-conc-pub',
              externalTransactionId: `conc-bet-${idx}-1-${newId()}`,
              playerId: w.playerId,
              walletId: w.id,
              roundId: `r-${idx}`,
              gameId: 'fortune-chimp',
              kind: 'BET',
              money: { amount: '10.00', currency: 'BRL' },
            }),
            submitHttpWager({
              providerId: 'prov-conc-pub',
              externalTransactionId: `conc-bet-${idx}-2-${newId()}`,
              playerId: w.playerId,
              walletId: w.id,
              roundId: `r-${idx}`,
              gameId: 'fortune-chimp',
              kind: 'BET',
              money: { amount: '15.00', currency: 'BRL' },
            }),
          ]),
        );

        const walletIdSet = new Set(wallets.map((w) => w.id));

        // Run all 3 publishers concurrently in tight parallel loops until all 48 events are published.
        await Promise.all(
          publishers.map(async (pub) => {
            for (let round = 0; round < 25; round += 1) {
              const stats = await pub.publishBatch({ batchSize: 4 });
              if (stats.selected === 0) {
                break;
              }
            }
          }),
        );

        const unpublished = await db`
          select id from outbox_messages
          where aggregate_id in ${db(wallets.map((w) => w.id))}
            and published_at is null
        `;
        expect(unpublished).toHaveLength(0);

        const delivered = await drainQueue(sqs, queues.eventsQueueUrl);
        const byWallet = new Map<string, string[]>();
        for (const msg of delivered) {
          const env = JSON.parse(msg.Body ?? '{}') as Json;
          if (walletIdSet.has(env.aggregateId)) {
            const list = byWallet.get(env.aggregateId) ?? [];
            list.push(env.eventId);
            byWallet.set(env.aggregateId, list);
          }
        }

        expect(byWallet.size).toBe(wallets.length);
        for (const w of wallets) {
          const expectedRows = await db`
            select id from outbox_messages
            where aggregate_id = ${w.id}
            order by sequence_number asc
          `;
          expect(byWallet.get(w.id)).toEqual(expectedRows.map((r: Json) => r.id));
        }
      } finally {
        await Promise.all(pubApps.map((a) => a.close()));
      }
    });
  });

  test('deterministically skips locked aggregate head (SKIP LOCKED) without bypassing it to publish later events of the same aggregate', async () => {
    await publishAllDueOutbox(publisher1);
    await drainQueue(sqs, eventsQueueUrl);

    const walletA = await openWallet('60.00'); // 2 events: seqA1 (Processed), seqA2 (BalanceChanged)
    const walletB = await openWallet('70.00'); // 2 events: seqB1 (Processed), seqB2 (BalanceChanged)

    const walletARows = await db`
      select id, sequence_number from outbox_messages where aggregate_id = ${walletA.id} order by sequence_number asc
    `;
    const walletBRows = await db`
      select id, sequence_number from outbox_messages where aggregate_id = ${walletB.id} order by sequence_number asc
    `;
    expect(walletARows).toHaveLength(2);
    expect(walletBRows).toHaveLength(2);

    let releasePub1!: () => void;
    let resolveLocked!: () => void;
    const pub1HoldingLock = new Promise<void>((resolve) => {
      resolveLocked = resolve;
    });
    const pub1Finished = publisher1.publishBatch({
      batchSize: 1,
      beforeCommit: async () => {
        resolveLocked();
        await new Promise<void>((r) => {
          releasePub1 = r;
        });
      },
    });

    // Wait until publisher1 holds the PostgreSQL FOR UPDATE lock on walletA's head (seqA1).
    await pub1HoldingLock;

    // While publisher1 holds seqA1 locked (uncommitted):
    // publisher2 MUST skip seqA1 (SKIP LOCKED), MUST NOT select seqA2 (NOT EXISTS unpublished predecessor),
    // and MUST immediately publish walletB's seqB1 and seqB2 without blocking!
    const pub2Pass1 = await publisher2.publishBatch({ batchSize: 10 });
    expect(pub2Pass1.publishedIds).toEqual([walletBRows[0].id]);

    const pub2Pass2 = await publisher2.publishBatch({ batchSize: 10 });
    expect(pub2Pass2.publishedIds).toEqual([walletBRows[1].id]);

    const pub2Pass3WhileBlocked = await publisher2.publishBatch({ batchSize: 10 });
    expect(pub2Pass3WhileBlocked.selected).toBe(0);

    // Release publisher1 -> seqA1 commits, unblocking seqA2 for publisher2!
    releasePub1();
    const pub1Stats = await pub1Finished;
    expect(pub1Stats.publishedIds).toEqual([walletARows[0].id]);

    const pub2Pass4 = await publisher2.publishBatch({ batchSize: 10 });
    expect(pub2Pass4.publishedIds).toEqual([walletARows[1].id]);
    await drainQueue(sqs, eventsQueueUrl);
  });

  test('tolerates SQS send success followed by crash before Outbox DB commit (at-least-once with immutable eventId)', async () => {
    await publishAllDueOutbox(publisher1);
    await drainQueue(sqs, eventsQueueUrl);

    await withIsolatedFifoQueues({}, async (queues) => {
      const pubApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      try {
        const publisher = pubApp.get(OutboxPublisherService);
        const wallet = await openWallet('40.00'); // 2 outbox events

        const [headBefore] = await db`
          select id, payload, published_at
          from outbox_messages
          where aggregate_id = ${wallet.id}
          order by sequence_number asc
          limit 1
        `;
        expect(headBefore.published_at).toBeNull();

        // Cycle 1: SQS SendMessageBatch SUCCEEDS against LocalStack, then crash right before PostgreSQL COMMIT!
        let publishedIdsBeforeCrash: readonly string[] = [];
        await expect(
          publisher.publishBatch({
            beforeCommit: (_txEm, publishedIds) => {
              publishedIdsBeforeCrash = publishedIds;
              throw new Error('simulated crash after SQS 200 OK before Outbox published_at commit');
            },
          }),
        ).rejects.toThrow('simulated crash after SQS 200 OK');

        expect(publishedIdsBeforeCrash).toContain(headBefore.id);

        // PostgreSQL rolled back published_at -> event is still pending in Outbox (never silently lost!).
        const [headAfterCrash] = await db`
          select id, payload, published_at, attempts
          from outbox_messages
          where id = ${headBefore.id}
        `;
        expect(headAfterCrash.published_at).toBeNull();
        expect(headAfterCrash.payload).toEqual(headBefore.payload);

        // Cycle 2: Publisher re-sends with the exact same eventId (MessageDeduplicationId) and commits cleanly!
        const totalPublished = await publishAllDueOutbox(publisher);
        expect(totalPublished).toBe(2);

        const [headAfterRecovery] = await db`
          select published_at, payload from outbox_messages where id = ${headBefore.id}
        `;
        expect(headAfterRecovery.published_at).not.toBeNull();
        expect(headAfterRecovery.payload).toEqual(headBefore.payload);

        // FIFO deduplication window on SQS deduplicated the duplicate send within 5 minutes -> exact 2 events!
        const delivered = await drainQueue(sqs, queues.eventsQueueUrl);
        const walletDelivered = delivered
          .map((m) => JSON.parse(m.Body ?? '{}') as Json)
          .filter((e) => e.aggregateId === wallet.id);
        expect(walletDelivered).toHaveLength(2);
        expect(walletDelivered[0]?.eventId).toBe(headBefore.id);
      } finally {
        await pubApp.close();
      }
    });
  });

  test('handles SendMessageBatch partial success after GetQueueUrl is resolved: commits Successful, backs off Failed without unblocking its aggregate, and lets other aggregates advance', async () => {
    await publishAllDueOutbox(publisher1);
    await drainQueue(sqs, eventsQueueUrl);

    await withIsolatedFifoQueues({}, async (queues) => {
      const pubApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      const pubSqs = pubApp.get(SQSClient);
      const realSend = pubSqs.send.bind(pubSqs);

      try {
        const publisher = pubApp.get(OutboxPublisherService);

        // Warm up publisher so GetQueueUrlCommand is ALREADY resolved and cached before testing SendMessageBatch!
        const warmupStats = await publisher.publishBatch();
        expect(warmupStats.selected).toBe(0);

        const walletA = await openWallet('50.00'); // 2 events: seqA1 (Processed), seqA2 (BalanceChanged)
        const walletB = await openWallet('60.00'); // 2 events: seqB1 (Processed), seqB2 (BalanceChanged)

        const rowsA = await db`
          select id, sequence_number from outbox_messages where aggregate_id = ${walletA.id} order by sequence_number asc
        `;
        const rowsB = await db`
          select id, sequence_number from outbox_messages where aggregate_id = ${walletB.id} order by sequence_number asc
        `;
        expect(rowsA).toHaveLength(2);
        expect(rowsB).toHaveLength(2);

        let getQueueUrlCallsDuringTest = 0;
        let batchCalls = 0;
        const sendSpy = spyOn(pubSqs, 'send').mockImplementation(async (command: any, options?: any) => {
          if (command instanceof GetQueueUrlCommand) {
            getQueueUrlCallsDuringTest += 1;
            return realSend(command, options);
          }
          if (command instanceof SendMessageBatchCommand && batchCalls === 0) {
            batchCalls += 1;
            const entries = command.input.Entries ?? [];
            const onlyWalletA = entries.filter((e) => e.MessageGroupId === walletA.id);
            const failedWalletB = entries.filter((e) => e.MessageGroupId === walletB.id);
            expect(onlyWalletA).toHaveLength(1);
            expect(failedWalletB).toHaveLength(1);

            // Actually deliver walletA's entry to LocalStack SQS, and report walletB's entry in Failed!
            const realRes = await realSend(
              new SendMessageBatchCommand({
                QueueUrl: command.input.QueueUrl,
                Entries: onlyWalletA,
              }),
              options,
            );
            return {
              ...realRes,
              Successful: realRes.Successful ?? [],
              Failed: failedWalletB.map((e) => ({
                Id: e.Id,
                Code: 'InternalError',
                SenderFault: false,
                Message: 'simulated per-entry SQS InternalError for walletB',
              })),
            };
          }
          return realSend(command, options);
        });

        try {
          const baseNow = new Date();
          const partialStats = await publisher.publishBatch({ now: baseNow, batchSize: 10 });

          // Confirmed: GetQueueUrl was already cached (0 calls), and SendMessageBatch partial path ran!
          expect(getQueueUrlCallsDuringTest).toBe(0);
          expect(batchCalls).toBe(1);
          expect(partialStats).toEqual({
            selected: 2,
            published: 1,
            failed: 1,
            quarantined: 0,
            publishedIds: [rowsA[0].id],
          });

          // Verify PostgreSQL state right after partial SendMessageBatch:
          const [a1After, a2After] = await db`
            select id, published_at, attempts, next_attempt_at
            from outbox_messages
            where aggregate_id = ${walletA.id}
            order by sequence_number asc
          `;
          expect(a1After.published_at).not.toBeNull();
          expect(a1After.attempts).toBe(0);
          expect(a2After.published_at).toBeNull();

          const [b1After, b2After] = await db`
            select id, published_at, attempts, next_attempt_at
            from outbox_messages
            where aggregate_id = ${walletB.id}
            order by sequence_number asc
          `;
          expect(b1After.published_at).toBeNull();
          expect(b1After.attempts).toBe(1);
          expect(new Date(b1After.next_attempt_at).getTime()).toBe(baseNow.getTime() + 1_000);
          // b2 remains untouched and strictly blocked behind b1!
          expect(b2After.published_at).toBeNull();
          expect(b2After.attempts).toBe(0);
          expect(b2After.next_attempt_at).toBeNull();

          // Before walletB's 1000ms backoff elapses (at +500ms):
          // walletA's second event (a2) AND a brand-new walletC advance and publish normally,
          // while walletB's b2 MUST remain blocked behind b1!
          const walletC = await openWallet('35.00');
          const midNow = new Date(baseNow.getTime() + 500);
          const midPublished = await publishAllDueOutbox(publisher, midNow);
          // a2 (1) + walletC OPENING (2) = 3 events published; 0 from walletB!
          expect(midPublished).toBe(3);

          const [b1StillPending, b2StillBlocked] = await db`
            select published_at, attempts from outbox_messages where aggregate_id = ${walletB.id} order by sequence_number asc
          `;
          expect(b1StillPending.published_at).toBeNull();
          expect(b2StillBlocked.published_at).toBeNull();

          // Once walletB's backoff expires (+1500ms), b1 and then b2 publish in strict sequence_number order!
          const recoveredPublished = await publishAllDueOutbox(publisher, new Date(baseNow.getTime() + 1_500));
          expect(recoveredPublished).toBe(2);

          const delivered = await drainQueue(sqs, queues.eventsQueueUrl);
          const deliveredA = delivered
            .map((m) => JSON.parse(m.Body ?? '{}') as Json)
            .filter((e) => e.aggregateId === walletA.id);
          const deliveredB = delivered
            .map((m) => JSON.parse(m.Body ?? '{}') as Json)
            .filter((e) => e.aggregateId === walletB.id);
          const deliveredC = delivered
            .map((m) => JSON.parse(m.Body ?? '{}') as Json)
            .filter((e) => e.aggregateId === walletC.id);

          expect(deliveredA.map((e) => e.eventId)).toEqual([rowsA[0].id, rowsA[1].id]);
          expect(deliveredB.map((e) => e.eventId)).toEqual([rowsB[0].id, rowsB[1].id]);
          expect(deliveredC).toHaveLength(2);
        } finally {
          sendSpy.mockRestore();
        }
      } finally {
        await pubApp.close();
      }
    });
  });

  test('handles SendMessageBatch transport failure after GetQueueUrl is resolved: preserves event integrity, schedules retry/backoff, and publishes in order upon recovery', async () => {
    await publishAllDueOutbox(publisher1);
    await drainQueue(sqs, eventsQueueUrl);

    await withIsolatedFifoQueues({}, async (queues) => {
      const pubApp = await startAppInstance({
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
      });

      const pubSqs = pubApp.get(SQSClient);
      const realSend = pubSqs.send.bind(pubSqs);

      try {
        const publisher = pubApp.get(OutboxPublisherService);

        // Resolve and cache GetQueueUrl BEFORE simulating SendMessageBatch transport error.
        expect((await publisher.publishBatch()).selected).toBe(0);

        const walletA = await openWallet('45.00'); // 2 events
        const walletB = await openWallet('55.00'); // 2 events

        const beforeA = await db`
          select id, sequence_number, payload from outbox_messages where aggregate_id = ${walletA.id} order by sequence_number asc
        `;
        const beforeB = await db`
          select id, sequence_number, payload from outbox_messages where aggregate_id = ${walletB.id} order by sequence_number asc
        `;

        let failTransportOnce = true;
        let sendBatchThrows = 0;
        const sendSpy = spyOn(pubSqs, 'send').mockImplementation(async (command: any, options?: any) => {
          if (command instanceof SendMessageBatchCommand && failTransportOnce) {
            failTransportOnce = false;
            sendBatchThrows += 1;
            throw Object.assign(new Error('connect ECONNRESET 127.0.0.1:4566'), { code: 'ECONNRESET' });
          }
          return realSend(command, options);
        });

        try {
          const baseNow = new Date();
          const failStats = await publisher.publishBatch({ now: baseNow, batchSize: 10 });

          expect(sendBatchThrows).toBe(1);
          expect(failStats).toEqual({
            selected: 2,
            published: 0,
            failed: 2,
            quarantined: 0,
            publishedIds: [],
          });

          // Verify payload integrity, attempts=1, backoff=1000ms on heads, and attempts=0 on tail events:
          for (const [wallet, beforeRows] of [
            [walletA, beforeA],
            [walletB, beforeB],
          ] as const) {
            const afterRows = await db`
              select id, sequence_number, payload, attempts, next_attempt_at, published_at
              from outbox_messages
              where aggregate_id = ${wallet.id}
              order by sequence_number asc
            `;
            expect(afterRows).toHaveLength(2);
            expect(afterRows[0].id).toBe(beforeRows[0].id);
            expect(afterRows[0].payload).toEqual(beforeRows[0].payload);
            expect(afterRows[0].published_at).toBeNull();
            expect(afterRows[0].attempts).toBe(1);
            expect(new Date(afterRows[0].next_attempt_at).getTime()).toBe(baseNow.getTime() + 1_000);

            expect(afterRows[1].id).toBe(beforeRows[1].id);
            expect(afterRows[1].payload).toEqual(beforeRows[1].payload);
            expect(afterRows[1].published_at).toBeNull();
            expect(afterRows[1].attempts).toBe(0);
          }

          // Premature poll before backoff expires (+400ms) must NOT bypass heads to publish tail events!
          const earlyRetry = await publisher.publishBatch({ now: new Date(baseNow.getTime() + 400), batchSize: 10 });
          expect(earlyRetry.selected).toBe(0);
          expect(earlyRetry.published).toBe(0);

          // After backoff expires (+1500ms), all 4 events publish without loss or per-wallet inversion!
          const recovered = await publishAllDueOutbox(publisher, new Date(baseNow.getTime() + 1_500));
          expect(recovered).toBe(4);

          const delivered = await drainQueue(sqs, queues.eventsQueueUrl);
          const deliveredA = delivered
            .map((m) => JSON.parse(m.Body ?? '{}') as Json)
            .filter((e) => e.aggregateId === walletA.id);
          const deliveredB = delivered
            .map((m) => JSON.parse(m.Body ?? '{}') as Json)
            .filter((e) => e.aggregateId === walletB.id);

          expect(deliveredA.map((e) => e.eventId)).toEqual([beforeA[0].id, beforeA[1].id]);
          expect(deliveredB.map((e) => e.eventId)).toEqual([beforeB[0].id, beforeB[1].id]);
        } finally {
          sendSpy.mockRestore();
        }
      } finally {
        await pubApp.close();
      }
    });
  });
});

describe('4.4 — PendingReferenceWorker (out-of-order resolution, backoff, TTL, concurrency, chained reversals)', () => {
  test('parks missing reference, applies exponential backoff while unavailable, and resolves atomically once reference arrives', async () => {
    const wallet = await openWallet('100.00');
    const providerId = `prov-pend-${newId().slice(-8)}`;
    const betExtId = `bet-${newId()}`;
    const refundExtId = `refund-${newId()}`;

    // 1. REFUND arrives BEFORE BET -> 202 PENDING_REFERENCE, attempt_count = 1, balance still 100.00.
    const refundRes = await submitHttpWager(
      {
        providerId,
        externalTransactionId: refundExtId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-pend-1',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '25.00', currency: 'BRL' },
        referenceExternalTransactionId: betExtId,
      },
      undefined,
      { 'x-correlation-id': 'corr-pending-refund-1' },
    );
    expect(refundRes.status).toBe(202);
    expect(refundRes.body.status).toBe(WagerTransactionStatus.PendingReference);

    const [pendingRow1] = await db`
      select id, attempt_count, next_attempt_at, status
      from wager_transactions
      where id = ${refundRes.body.transactionId}
    `;
    expect(pendingRow1.attempt_count).toBe(1);
    const firstDueAt = new Date(pendingRow1.next_attempt_at);

    // 2. Worker runs BEFORE next_attempt_at -> does not touch the row.
    const earlyStats = await worker1.processDueBatch({ now: new Date(firstDueAt.getTime() - 100) });
    expect(earlyStats.resolved).toBe(0);

    // 3. Worker runs AT firstDueAt while BET is STILL missing -> increments attempt_count to 2, schedules +2000ms backoff, 0 new events!
    const outboxCountBeforeRetry = await db`
      select count(*)::int as cnt from outbox_messages where aggregate_id = ${wallet.id}
    `;
    const retryStats = await worker1.processDueBatch({ now: firstDueAt });
    expect(retryStats.rescheduled).toBe(1);
    expect(retryStats.resolved).toBe(0);

    const [pendingRow2] = await db`
      select attempt_count, next_attempt_at, status
      from wager_transactions
      where id = ${refundRes.body.transactionId}
    `;
    expect(pendingRow2.status).toBe(WagerTransactionStatus.PendingReference);
    expect(pendingRow2.attempt_count).toBe(2);
    const secondDueAt = new Date(pendingRow2.next_attempt_at);
    expect(secondDueAt.getTime() - firstDueAt.getTime()).toBe(2_000);

    const outboxCountAfterRetry = await db`
      select count(*)::int as cnt from outbox_messages where aggregate_id = ${wallet.id}
    `;
    expect(outboxCountAfterRetry[0].cnt).toBe(outboxCountBeforeRetry[0].cnt);

    // 4. Now the referenced BET arrives (100.00 -> 75.00).
    const betRes = await submitHttpWager({
      providerId,
      externalTransactionId: betExtId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-pend-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });
    expect(betRes.status).toBe(200);
    expect(betRes.body.balance.amount).toBe('75.00');

    // 5. Worker runs at secondDueAt -> resolves REFUND (75.00 -> 100.00, version 3, PROCESSED)!
    const resolvedStats = await worker1.processDueBatch({ now: secondDueAt });
    expect(resolvedStats.resolved).toBe(1);

    const [resolvedTx] = await db`
      select status, reference_transaction_id, observed_balance_amount, next_attempt_at, processed_at
      from wager_transactions
      where id = ${refundRes.body.transactionId}
    `;
    expect(resolvedTx.status).toBe(WagerTransactionStatus.Processed);
    expect(resolvedTx.reference_transaction_id).toBe(betRes.body.transactionId);
    expect(resolvedTx.observed_balance_amount).toBe('100.00');
    expect(resolvedTx.next_attempt_at).toBeNull();
    expect(resolvedTx.processed_at).not.toBeNull();

    const [walletAfter] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
    expect(walletAfter).toEqual({ balance_amount: '100.00', version: 3 });

    const refundOutbox = await db`
      select event_type, payload
      from outbox_messages
      where payload->'data'->>'transactionId' = ${refundRes.body.transactionId}
      order by sequence_number asc
    `;
    expect(refundOutbox.map((r: Json) => r.event_type)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
    ]);
    // Preserved original correlationId and linked causationId to the PendingReference event!
    expect(refundOutbox[1].payload.correlationId).toBe('corr-pending-refund-1');
    expect(refundOutbox[1].payload.causationId).toBe(refundOutbox[0].payload.eventId);
  });

  test('expires unresolved PENDING_REFERENCE to REJECTED (REFERENCE_NOT_FOUND) and emits WagerTransactionRejected when maxAttempts or TTL is exhausted', async () => {
    const wallet = await openWallet('80.00');
    const providerId = `prov-exp-${newId().slice(-8)}`;

    const rollbackByAttempts = await submitHttpWager({
      providerId,
      externalTransactionId: `rb-attempts-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-exp-1',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'never-arrives-1',
    });
    expect(rollbackByAttempts.status).toBe(202);

    // Exhaust maxAttempts = 2: initial attemptCount is 1 -> 1st worker retry brings attemptCount to 2 -> 2nd worker check rejects!
    let tick = new Date(Date.now() + 2_000);
    await worker1.processDueBatch({ now: tick, policy: { maxAttempts: 2, ttlMs: 600_000 } });
    tick = new Date(tick.getTime() + 5_000);
    await worker1.processDueBatch({ now: tick, policy: { maxAttempts: 2, ttlMs: 600_000 } });

    const [expiredByAttempts] = await db`
      select status, failure_code, observed_balance_amount, attempt_count, next_attempt_at
      from wager_transactions
      where id = ${rollbackByAttempts.body.transactionId}
    `;
    expect(expiredByAttempts).toEqual({
      status: WagerTransactionStatus.Rejected,
      failure_code: FailureCode.ReferenceNotFound,
      observed_balance_amount: '80.00',
      attempt_count: 2,
      next_attempt_at: null,
    });

    // Also test expiration by TTL (60s):
    const refundByTtl = await submitHttpWager({
      providerId,
      externalTransactionId: `rf-ttl-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-exp-2',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '15.00', currency: 'BRL' },
      referenceExternalTransactionId: 'never-arrives-2',
    });
    expect(refundByTtl.status).toBe(202);

    await worker1.processDueBatch({
      now: new Date(Date.now() + 61_000),
      policy: { maxAttempts: 10, ttlMs: 60_000 },
    });

    const [expiredByTtl] = await db`
      select status, failure_code, observed_balance_amount, next_attempt_at
      from wager_transactions
      where id = ${refundByTtl.body.transactionId}
    `;
    expect(expiredByTtl).toEqual({
      status: WagerTransactionStatus.Rejected,
      failure_code: FailureCode.ReferenceNotFound,
      observed_balance_amount: '80.00',
      next_attempt_at: null,
    });

    const rejectedEvents = await db`
      select event_type, payload
      from outbox_messages
      where payload->'data'->>'transactionId' in (${rollbackByAttempts.body.transactionId}, ${refundByTtl.body.transactionId})
        and event_type = 'WagerTransactionRejected'
    `;
    expect(rejectedEvents).toHaveLength(2);
    for (const ev of rejectedEvents) {
      expect(ev.payload.data.failureCode).toBe(FailureCode.ReferenceNotFound);
    }
  });

  test('prevents duplicate financial movements when multiple PendingReferenceWorkers run concurrently on the same due references', async () => {
    const app3 = await startAppInstance();
    try {
      const worker3 = app3.get(PendingReferenceWorker);
      const wallet = await openWallet('200.00');
      const providerId = `prov-wconc-${newId().slice(-8)}`;

      // Park 4 dependent operations (2 REFUNDs, 1 WIN, 1 ROLLBACK) on the SAME wallet before their BETs arrive!
      const betIds = Array.from({ length: 4 }, (_, i) => `bet-conc-${i}-${newId()}`);
      const pendingResponses = await Promise.all([
        submitHttpWager({
          providerId,
          externalTransactionId: `dep-rf-0-${newId()}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'r-wconc-0',
          gameId: 'fortune-chimp',
          kind: 'REFUND',
          money: { amount: '10.00', currency: 'BRL' },
          referenceExternalTransactionId: betIds[0],
        }),
        submitHttpWager({
          providerId,
          externalTransactionId: `dep-rf-1-${newId()}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'r-wconc-1',
          gameId: 'fortune-chimp',
          kind: 'REFUND',
          money: { amount: '20.00', currency: 'BRL' },
          referenceExternalTransactionId: betIds[1],
        }),
        submitHttpWager({
          providerId,
          externalTransactionId: `dep-win-2-${newId()}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'r-wconc-2',
          gameId: 'fortune-chimp',
          kind: 'WIN',
          money: { amount: '35.00', currency: 'BRL' },
          referenceExternalTransactionId: betIds[2],
        }),
        submitHttpWager({
          providerId,
          externalTransactionId: `dep-rb-3-${newId()}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'r-wconc-3',
          gameId: 'fortune-chimp',
          kind: 'ROLLBACK',
          money: { amount: '15.00', currency: 'BRL' },
          referenceExternalTransactionId: betIds[3],
        }),
      ]);
      for (const res of pendingResponses) {
        expect(res.status).toBe(202);
      }

      // Now submit all 4 BETs (-10 -20 -15 -15 = -60 -> balance 140.00).
      const amounts = ['10.00', '20.00', '15.00', '15.00'];
      for (let i = 0; i < 4; i += 1) {
        const betRes = await submitHttpWager({
          providerId,
          externalTransactionId: betIds[i],
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: `r-wconc-${i}`,
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: amounts[i], currency: 'BRL' },
        });
        expect(betRes.status).toBe(200);
      }

      // Fire 3 PendingReferenceWorker instances simultaneously at future due timestamp!
      const dueNow = new Date(Date.now() + 5_000);
      const [s1, s2, s3] = await Promise.all([
        worker1.processDueBatch({ now: dueNow }),
        worker2.processDueBatch({ now: dueNow }),
        worker3.processDueBatch({ now: dueNow }),
      ]);

      expect(s1.failed + s2.failed + s3.failed).toBe(0);
      expect(s1.resolved + s2.resolved + s3.resolved).toBeGreaterThanOrEqual(4);

      // Expected balance: 200 - 60 (4 BETs) + 10 (rf0) + 20 (rf1) + 35 (win2) + 15 (rb3) = 220.00!
      // Expected version: 1 (OPENING) + 4 (BETs) + 4 (resolved dependents) = 9!
      const [walletRow] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
      expect(walletRow).toEqual({ balance_amount: '220.00', version: 9 });

      const recon = await call('POST', `/wallets/${wallet.id}/reconciliation`);
      expect(recon.status).toBe(200);
      expect(recon.body.consistent).toBe(true);
      expect(recon.body.checkedEntries).toBe(9);
    } finally {
      await app3.close();
    }
  });

  test('resolves out-of-order chained reversals (ROLLBACK referencing WIN in PENDING_REFERENCE) while immediately rejecting invalid chained claims', async () => {
    const wallet = await openWallet('100.00');
    const providerId = `prov-chain-${newId().slice(-8)}`;
    const betExt = `chain-bet-${newId()}`;
    const winExt = `chain-win-${newId()}`;
    const rbExt = `chain-rb-${newId()}`;

    // 1. WIN (50.00) referencing delayed BET arrives first -> parked in PENDING_REFERENCE.
    const winRes = await submitHttpWager({
      providerId,
      externalTransactionId: winExt,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-chain-1',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: betExt,
    });
    expect(winRes.status).toBe(202);
    expect(winRes.body.status).toBe(WagerTransactionStatus.PendingReference);

    // 2. Invalid operations referencing the PENDING_REFERENCE WIN are STILL rejected immediately (422):
    const wrongAmountRb = await submitHttpWager({
      providerId,
      externalTransactionId: `bad-amt-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-chain-1',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '49.99', currency: 'BRL' },
      referenceExternalTransactionId: winExt,
    });
    expect(wrongAmountRb.status).toBe(422);
    expect(wrongAmountRb.body.failureCode).toBe(FailureCode.AmountMismatch);

    const wrongKindRefund = await submitHttpWager({
      providerId,
      externalTransactionId: `bad-kind-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-chain-1',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: winExt,
    });
    expect(wrongKindRefund.status).toBe(422);
    expect(wrongKindRefund.body.failureCode).toBe(FailureCode.ReferenceKindNotAllowed);

    const wrongRoundRb = await submitHttpWager({
      providerId,
      externalTransactionId: `bad-scope-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'other-round',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: winExt,
    });
    expect(wrongRoundRb.status).toBe(422);
    expect(wrongRoundRb.body.failureCode).toBe(FailureCode.ReferenceScopeMismatch);

    // 3. Valid ROLLBACK (50.00) referencing the PENDING_REFERENCE WIN is parked in PENDING_REFERENCE (202)!
    const rbRes = await submitHttpWager({
      providerId,
      externalTransactionId: rbExt,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-chain-1',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: winExt,
    });
    expect(rbRes.status).toBe(202);
    expect(rbRes.body.status).toBe(WagerTransactionStatus.PendingReference);

    // A duplicate second ROLLBACK on that same PENDING_REFERENCE WIN is immediately rejected (REFERENCE_ALREADY_REVERSED)!
    const dupRb = await submitHttpWager({
      providerId,
      externalTransactionId: `dup-rb-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-chain-1',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: winExt,
    });
    expect(dupRb.status).toBe(422);
    expect(dupRb.body.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);

    // 4. Delayed root BET (20.00) arrives -> balance 100.00 -> 80.00.
    const betRes = await submitHttpWager({
      providerId,
      externalTransactionId: betExt,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-chain-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '20.00', currency: 'BRL' },
    });
    expect(betRes.status).toBe(200);
    expect(betRes.body.balance.amount).toBe('80.00');

    // 5. Worker settles the chain: WIN credits +50.00 (130.00) and ROLLBACK debits -50.00 (back to 80.00)!
    let tick = new Date(Date.now() + 2_000);
    await worker1.processDueBatch({ now: tick });
    tick = new Date(tick.getTime() + 5_000);
    await worker1.processDueBatch({ now: tick });

    const [winAfter] = await db`select status from wager_transactions where id = ${winRes.body.transactionId}`;
    const [rbAfter] = await db`
      select status, reference_transaction_id from wager_transactions where id = ${rbRes.body.transactionId}
    `;
    expect(winAfter.status).toBe(WagerTransactionStatus.Processed);
    expect(rbAfter.status).toBe(WagerTransactionStatus.Processed);
    expect(rbAfter.reference_transaction_id).toBe(winRes.body.transactionId);

    const [walletAfter] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
    expect(walletAfter).toEqual({ balance_amount: '80.00', version: 4 });

    // 6. Chained failure path: if root BET never arrives and WIN expires (REFERENCE_NOT_FOUND),
    // the dependent ROLLBACK resolves to REJECTED (REFERENCE_NOT_PROCESSED)!
    const orphanWinExt = `orphan-win-${newId()}`;
    const orphanWin = await submitHttpWager({
      providerId,
      externalTransactionId: orphanWinExt,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-chain-orphan',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '30.00', currency: 'BRL' },
      referenceExternalTransactionId: 'missing-root-bet',
    });
    const orphanRb = await submitHttpWager({
      providerId,
      externalTransactionId: `orphan-rb-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-chain-orphan',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '30.00', currency: 'BRL' },
      referenceExternalTransactionId: orphanWinExt,
    });
    expect(orphanWin.status).toBe(202);
    expect(orphanRb.status).toBe(202);

    const expireAt = new Date(Date.now() + 65_000);
    await worker1.processDueBatch({ now: expireAt, policy: { maxAttempts: 10, ttlMs: 60_000 } });
    await worker1.processDueBatch({
      now: new Date(expireAt.getTime() + 5_000),
      policy: { maxAttempts: 10, ttlMs: 60_000 },
    });

    const [orphanWinRow] = await db`
      select status, failure_code from wager_transactions where id = ${orphanWin.body.transactionId}
    `;
    const [orphanRbRow] = await db`
      select status, failure_code from wager_transactions where id = ${orphanRb.body.transactionId}
    `;
    expect(orphanWinRow).toEqual({
      status: WagerTransactionStatus.Rejected,
      failure_code: FailureCode.ReferenceNotFound,
    });
    expect(orphanRbRow).toEqual({
      status: WagerTransactionStatus.Rejected,
      failure_code: FailureCode.ReferenceNotProcessed,
    });
  });

  test('avoids querying outbox_messages on rescheduled retries and prefetches originating context before acquiring wallets FOR UPDATE on resolution', async () => {
    const wallet = await openWallet('100.00');
    const orm = app1.get(MikroORM);
    const processor = app1.get(WagerProcessorService);
    const conn = orm.em.getConnection() as any;
    const providerId = `prov-p2-${newId().slice(-8)}`;
    const betExt = `p2-bet-${newId()}`;
    const refundExt = `p2-rf-${newId()}`;

    const refundRes = await submitHttpWager(
      {
        providerId,
        externalTransactionId: refundExt,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-p2-1',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '20.00', currency: 'BRL' },
        referenceExternalTransactionId: betExt,
      },
      undefined,
      { 'x-correlation-id': 'corr-p2-trace-check' },
    );
    expect(refundRes.status).toBe(202);
    const txId = refundRes.body.transactionId as string;

    // 1. On `rescheduled` (reference still missing), `outbox_messages` MUST NOT be queried at all!
    const retryTime = new Date(Date.now() + 2_000);
    let outboxSelectsDuringReschedule = 0;
    const rescheduleOutcome = await orm.em.fork().transactional(
      async (txEm) => {
        const realConnExec = conn.execute.bind(conn);
        const connSpy = spyOn(conn, 'execute').mockImplementation((sql: any, params?: any, method?: any, ctx?: any) => {
          if (typeof sql === 'string' && sql.includes('from outbox_messages')) {
            outboxSelectsDuringReschedule += 1;
          }
          return realConnExec(sql, params, method, ctx);
        });
        try {
          return await processor.resolvePendingReferenceInTransaction(txEm, txId, wallet.id, { now: retryTime });
        } finally {
          connSpy.mockRestore();
        }
      },
      { isolationLevel: IsolationLevel.READ_COMMITTED },
    );

    expect(rescheduleOutcome.outcome).toBe('rescheduled');
    expect(outboxSelectsDuringReschedule).toBe(0);

    // 2. Submit the referenced BET so the next pass resolves to PROCESSED.
    const betRes = await submitHttpWager({
      providerId,
      externalTransactionId: betExt,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-p2-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '20.00', currency: 'BRL' },
    });
    expect(betRes.status).toBe(200);

    // 3. On `resolved`, `findOriginatingEventContext` runs BEFORE `SAVEPOINT` and `wallets FOR UPDATE`
    // (0 outbox queries under wallet lock) and preserves correlationId + causationId!
    const resolveTime = new Date(retryTime.getTime() + 5_000);
    const operationsOrder: string[] = [];
    const resolveOutcome = await orm.em.fork().transactional(
      async (txEm) => {
        const realLogQuery = conn.logQuery.bind(conn);
        const logSpy = spyOn(conn, 'logQuery').mockImplementation((sql: string, loggerCtx?: any) => {
          const normalized = sql.toLowerCase();
          if (normalized.includes('left join wager_transactions r')) {
            operationsOrder.push('probe_select');
          } else if (normalized.includes('from outbox_messages')) {
            operationsOrder.push('outbox_context_select');
          } else if (normalized.startsWith('savepoint')) {
            operationsOrder.push('savepoint');
          } else if (normalized.includes('from "wallets"') && normalized.includes('for update')) {
            operationsOrder.push('for_update:wallets');
          } else if (normalized.includes('from "wager_transactions"') && normalized.includes('for update')) {
            operationsOrder.push('for_update:wager_transactions');
          } else if (normalized.startsWith('rollback to savepoint')) {
            operationsOrder.push('rollback_to_savepoint');
          } else if (normalized.startsWith('release savepoint')) {
            operationsOrder.push('release_savepoint');
          }
          return realLogQuery(sql, loggerCtx);
        });

        try {
          return await processor.resolvePendingReferenceInTransaction(txEm, txId, wallet.id, { now: resolveTime });
        } finally {
          logSpy.mockRestore();
        }
      },
      { isolationLevel: IsolationLevel.READ_COMMITTED },
    );

    expect(resolveOutcome.outcome).toBe('resolved');
    expect(operationsOrder).toEqual([
      'probe_select',
      'outbox_context_select',
      'savepoint',
      'for_update:wallets',
      'for_update:wager_transactions',
      'release_savepoint',
    ]);

    const txEvents = await db`
      select event_type, payload
      from outbox_messages
      where payload->'data'->>'transactionId' = ${txId}
      order by sequence_number asc
    `;
    expect(txEvents.map((r: Json) => r.event_type)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
    ]);
    expect(txEvents[1].payload.correlationId).toBe('corr-p2-trace-check');
    expect(txEvents[1].payload.causationId).toBe(txEvents[0].payload.eventId);
    expect(txEvents[2].payload.correlationId).toBe('corr-p2-trace-check');
    expect(txEvents[2].payload.causationId).toBe(txEvents[0].payload.eventId);
  });

  test('recovers from concurrent probe-to-lock race via SAVEPOINT rollback, proving locks are released before querying outbox_messages', async () => {
    const wallet = await openWallet('100.00');
    const ormA = app1.get(MikroORM);
    const processorA = app1.get(WagerProcessorService);
    const ormB = app2.get(MikroORM);
    const processorB = app2.get(WagerProcessorService);
    const probeSql = createTestSql('app');

    const providerId = `prov-p2-race-${newId().slice(-8)}`;
    const betExt = `race-bet-${newId()}`;
    const refundExt = `race-refund-${newId()}`;

    try {
      // 1. Park REFUND (25.00) in PENDING_REFERENCE with correlationId 'corr-p2-savepoint-race'.
      const refundRes = await submitHttpWager(
        {
          providerId,
          externalTransactionId: refundExt,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-p2-race',
          gameId: 'fortune-chimp',
          kind: 'REFUND',
          money: { amount: '25.00', currency: 'BRL' },
          referenceExternalTransactionId: betExt,
        },
        undefined,
        { 'x-correlation-id': 'corr-p2-savepoint-race' },
      );
      expect(refundRes.status).toBe(202);
      const refundTxId = refundRes.body.transactionId as string;

      // Publish existing outbox rows so WagerTransactionPendingReference is already published (`published_at IS NOT NULL`).
      await publishAllDueOutbox(publisher1);

      // 2. Transaction B (on app2): acquires `wallets FOR UPDATE`, inserts & processes the referenced BET,
      // and waits on an explicit barrier BEFORE committing!
      let signalTxBHoldingLock!: () => void;
      const txBHoldingLock = new Promise<void>((resolve) => {
        signalTxBHoldingLock = resolve;
      });
      let allowTxBToCommit!: () => void;
      const txBCommitGate = new Promise<void>((resolve) => {
        allowTxBToCommit = resolve;
      });

      const parsedBet = parseWagerRequest(
        {
          providerId,
          externalTransactionId: betExt,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-p2-race',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '25.00', currency: 'BRL' },
        },
        `${providerId}:${betExt}`,
      );

      const txBPromise = ormB.em.fork().transactional(
        async (txEmB) => {
          const res = await processorB.processInTransaction(txEmB, {
            ...parsedBet,
            correlationId: 'corr-tx-b-bet',
          });
          signalTxBHoldingLock();
          await txBCommitGate;
          return res;
        },
        { isolationLevel: IsolationLevel.READ_COMMITTED },
      );

      // Wait until Transaction B definitely holds `wallets FOR UPDATE` with uncommitted BET.
      await txBHoldingLock;

      // 3. Transaction A (on app1):
      // - Runs `shouldPrefetchOriginatingContext` while B is uncommitted -> sees NULL reference -> NO prefetch!
      // - Enters SAVEPOINT 1 and blocks on `wallets FOR UPDATE` -> triggers `allowTxBToCommit()` so B commits!
      // - Reads fresh BET='PROCESSED', throws sentinel -> `ROLLBACK TO SAVEPOINT` releases BOTH row locks!
      // - At the exact moment `findOriginatingEventContext` queries `outbox_messages`, an independent
      //   `wagering_app` connection proves via `FOR UPDATE NOWAIT` that NEITHER `wallets` NOR `wager_transactions` is locked!
      // - Re-runs SAVEPOINT 2 once, resolves REFUND -> PROCESSED, and commits!
      const connA = ormA.em.getConnection() as any;
      const sqlTrace: string[] = [];
      let outboxQueriedBeforeFirstLock = false;
      let walletNowaitDuringOutboxLookup = false;
      let txNowaitDuringOutboxLookup = false;
      let walletLocksSeen = 0;

      class ProbeRollbackSignal extends Error {}

      const dueAt = new Date(Date.now() + 5_000);
      const resolveOutcome = await ormA.em.fork().transactional(
        async (txEmA) => {
          const realLogQuery = connA.logQuery.bind(connA);
          const realConnExec = connA.execute.bind(connA);

          const logSpy = spyOn(connA, 'logQuery').mockImplementation((sql: string, loggerCtx?: any) => {
            const normalized = sql.toLowerCase();
            if (normalized.includes('left join wager_transactions r')) {
              sqlTrace.push('probe_select');
            } else if (normalized.startsWith('savepoint')) {
              sqlTrace.push('savepoint');
            } else if (normalized.startsWith('rollback to savepoint')) {
              sqlTrace.push('rollback_to_savepoint');
            } else if (normalized.startsWith('release savepoint')) {
              sqlTrace.push('release_savepoint');
            }
            return realLogQuery(sql, loggerCtx);
          });

          const execSpy = spyOn(connA, 'execute').mockImplementation(
            async (sql: any, params?: any, method?: any, ctx?: any, loggerCtx?: any) => {
              const normalized = typeof sql === 'string' ? sql.toLowerCase() : '';

              if (normalized.includes('from "wallets"') && normalized.includes('for update')) {
                walletLocksSeen += 1;
                sqlTrace.push(`for_update:wallets#${walletLocksSeen}`);
                if (walletLocksSeen === 1) {
                  // Confirm zero outbox queries happened during initial probe!
                  outboxQueriedBeforeFirstLock = sqlTrace.includes('outbox_context_select');
                  // Start SQL execution (which queues behind Tx B's wallet lock) and release Tx B to commit!
                  const pendingLockPromise = realConnExec(sql, params, method, ctx, loggerCtx);
                  allowTxBToCommit();
                  return await pendingLockPromise;
                }
              } else if (normalized.includes('from "wager_transactions"') && normalized.includes('for update')) {
                sqlTrace.push(`for_update:wager_transactions#${walletLocksSeen}`);
              } else if (normalized.includes('from outbox_messages') && normalized.startsWith('select')) {
                sqlTrace.push('outbox_context_select');
                // Empirical PostgreSQL lock-freedom proof right while `findOriginatingEventContext` runs:
                // An independent `wagering_app` connection acquires `FOR UPDATE NOWAIT` on BOTH rows!
                await probeSql
                  .begin(async (txProbe) => {
                    const wRows = await txProbe`select id from wallets where id = ${wallet.id} for update nowait`;
                    const tRows = await txProbe`
                      select id from wager_transactions where id = ${refundTxId} for update nowait
                    `;
                    walletNowaitDuringOutboxLookup = wRows.length === 1;
                    txNowaitDuringOutboxLookup = tRows.length === 1;
                    throw new ProbeRollbackSignal();
                  })
                  .catch((err) => {
                    if (!(err instanceof ProbeRollbackSignal)) {
                      throw err;
                    }
                  });
              }

              return realConnExec(sql, params, method, ctx, loggerCtx);
            },
          );

          try {
            return await processorA.resolvePendingReferenceInTransaction(txEmA, refundTxId, wallet.id, {
              now: dueAt,
            });
          } finally {
            execSpy.mockRestore();
            logSpy.mockRestore();
          }
        },
        { isolationLevel: IsolationLevel.READ_COMMITTED },
      );

      const betOutcome = await txBPromise;
      expect(betOutcome.transaction.status).toBe(WagerTransactionStatus.Processed);

      // Verify exact SAVEPOINT -> rollback -> unlocked outbox lookup -> single retry SAVEPOINT trace:
      expect(outboxQueriedBeforeFirstLock).toBe(false);
      expect(walletNowaitDuringOutboxLookup).toBe(true);
      expect(txNowaitDuringOutboxLookup).toBe(true);
      expect(sqlTrace).toEqual([
        'probe_select',
        'savepoint',
        'for_update:wallets#1',
        'for_update:wager_transactions#1',
        'rollback_to_savepoint',
        'outbox_context_select',
        'savepoint',
        'for_update:wallets#2',
        'for_update:wager_transactions#2',
        'release_savepoint',
      ]);

      expect(resolveOutcome.outcome).toBe('resolved');
      expect(
        (resolveOutcome as Extract<typeof resolveOutcome, { outcome: 'resolved' }>).transaction.status,
      ).toBe(WagerTransactionStatus.Processed);

      // Verify wallet balance (100 - 25 BET + 25 REFUND = 100.00, version 3) and single terminal event emission:
      const [walletRow] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
      expect(walletRow).toEqual({ balance_amount: '100.00', version: 3 });

      const refundEvents = await db`
        select event_type, payload
        from outbox_messages
        where payload->'data'->>'transactionId' = ${refundTxId}
        order by sequence_number asc
      `;
      expect(refundEvents.map((r: Json) => r.event_type)).toEqual([
        'WagerTransactionPendingReference',
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
      ]);
      expect(refundEvents[1].payload.correlationId).toBe('corr-p2-savepoint-race');
      expect(refundEvents[1].payload.causationId).toBe(refundEvents[0].payload.eventId);
      expect(refundEvents[2].payload.correlationId).toBe('corr-p2-savepoint-race');
      expect(refundEvents[2].payload.causationId).toBe(refundEvents[0].payload.eventId);
    } finally {
      await probeSql.close();
    }
  });

  test('prefetches originating context before SAVEPOINT on invalid chained PENDING_REFERENCE violations while skipping outbox on valid chained waits', async () => {
    const wallet = await openWallet('100.00');
    const orm = app1.get(MikroORM);
    const processor = app1.get(WagerProcessorService);
    const conn = orm.em.getConnection() as any;
    const providerId = `prov-p2-chain-${newId().slice(-8)}`;

    const pendingWinKindExt = `p2c-win-kind-${newId()}`;
    const pendingWinAmtExt = `p2c-win-amt-${newId()}`;
    const pendingWinScopeExt = `p2c-win-scope-${newId()}`;
    const pendingWinValidExt = `p2c-win-valid-${newId()}`;

    // 1. Submit 3 invalid dependents + 1 valid dependent BEFORE their referenced WINs arrive -> all 4 park in PENDING_REFERENCE!
    const badKindRefund = await submitHttpWager(
      {
        providerId,
        externalTransactionId: `p2c-rf-kind-${newId()}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-chain-kind',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: pendingWinKindExt,
      },
      undefined,
      { 'x-correlation-id': 'corr-chain-kind' },
    );
    const badAmtRollback = await submitHttpWager(
      {
        providerId,
        externalTransactionId: `p2c-rb-amt-${newId()}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-chain-amt',
        gameId: 'fortune-chimp',
        kind: 'ROLLBACK',
        money: { amount: '25.00', currency: 'BRL' },
        referenceExternalTransactionId: pendingWinAmtExt,
      },
      undefined,
      { 'x-correlation-id': 'corr-chain-amt' },
    );
    const badScopeRollback = await submitHttpWager(
      {
        providerId,
        externalTransactionId: `p2c-rb-scope-${newId()}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-chain-scope-A',
        gameId: 'fortune-chimp',
        kind: 'ROLLBACK',
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: pendingWinScopeExt,
      },
      undefined,
      { 'x-correlation-id': 'corr-chain-scope' },
    );
    const validChainedRollback = await submitHttpWager(
      {
        providerId,
        externalTransactionId: `p2c-rb-valid-${newId()}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-chain-valid',
        gameId: 'fortune-chimp',
        kind: 'ROLLBACK',
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: pendingWinValidExt,
      },
      undefined,
      { 'x-correlation-id': 'corr-chain-valid' },
    );

    expect(badKindRefund.status).toBe(202);
    expect(badAmtRollback.status).toBe(202);
    expect(badScopeRollback.status).toBe(202);
    expect(validChainedRollback.status).toBe(202);

    // 2. Now submit the 4 referenced WINs referencing missing BETs -> all 4 WINs are ALSO in PENDING_REFERENCE!
    for (const [extId, roundId] of [
      [pendingWinKindExt, 'round-chain-kind'],
      [pendingWinAmtExt, 'round-chain-amt'],
      [pendingWinScopeExt, 'round-chain-scope-B'],
      [pendingWinValidExt, 'round-chain-valid'],
    ] as const) {
      const winRes = await submitHttpWager({
        providerId,
        externalTransactionId: extId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId,
        gameId: 'fortune-chimp',
        kind: 'WIN',
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: `missing-root-${extId}`,
      });
      expect(winRes.status).toBe(202);
    }

    const dueAt = new Date(Date.now() + 3_000);
    const runTracedResolution = async (txId: string) => {
      const trace: string[] = [];
      const result = await orm.em.fork().transactional(
        async (txEm) => {
          const realLogQuery = conn.logQuery.bind(conn);
          const logSpy = spyOn(conn, 'logQuery').mockImplementation((sql: string, loggerCtx?: any) => {
            const normalized = sql.toLowerCase();
            if (normalized.includes('left join wager_transactions r')) {
              trace.push('probe_select');
            } else if (normalized.includes('from outbox_messages') && normalized.startsWith('select')) {
              trace.push('outbox_context_select');
            } else if (normalized.startsWith('savepoint')) {
              trace.push('savepoint');
            } else if (normalized.includes('from "wallets"') && normalized.includes('for update')) {
              trace.push('for_update:wallets');
            } else if (normalized.startsWith('rollback to savepoint')) {
              trace.push('rollback_to_savepoint');
            }
            return realLogQuery(sql, loggerCtx);
          });
          try {
            return await processor.resolvePendingReferenceInTransaction(txEm, txId, wallet.id, { now: dueAt });
          } finally {
            logSpy.mockRestore();
          }
        },
        { isolationLevel: IsolationLevel.READ_COMMITTED },
      );
      return { result, trace };
    };

    // 3. All 3 invalid chained PENDING_REFERENCE dependents prefetch BEFORE savepoint/wallet lock (0 savepoint rollbacks!):
    for (const [resBody, expectedCode, expectedCorr] of [
      [badKindRefund.body, FailureCode.ReferenceKindNotAllowed, 'corr-chain-kind'],
      [badAmtRollback.body, FailureCode.AmountMismatch, 'corr-chain-amt'],
      [badScopeRollback.body, FailureCode.ReferenceScopeMismatch, 'corr-chain-scope'],
    ] as const) {
      const txId = resBody.transactionId as string;
      const { result, trace } = await runTracedResolution(txId);
      expect(result.outcome).toBe('resolved');
      const resolvedTx = (result as Extract<typeof result, { outcome: 'resolved' }>).transaction;
      expect(resolvedTx.status).toBe(WagerTransactionStatus.Rejected);
      expect(resolvedTx.failureCode).toBe(expectedCode);
      expect(trace).toEqual(['probe_select', 'outbox_context_select', 'savepoint', 'for_update:wallets']);

      const events = await db`
        select event_type, payload
        from outbox_messages
        where payload->'data'->>'transactionId' = ${txId}
        order by sequence_number asc
      `;
      expect(events.map((r: Json) => r.event_type)).toEqual([
        'WagerTransactionPendingReference',
        'WagerTransactionRejected',
      ]);
      expect(events[1].payload.correlationId).toBe(expectedCorr);
      expect(events[1].payload.causationId).toBe(events[0].payload.eventId);
    }

    // 4. Valid chained ROLLBACK -> PENDING_REFERENCE WIN reschedules with ZERO outbox queries!
    const validTrace = await runTracedResolution(validChainedRollback.body.transactionId as string);
    expect(validTrace.result.outcome).toBe('rescheduled');
    expect(validTrace.trace).toEqual(['probe_select', 'savepoint', 'for_update:wallets']);
  });
});

describe('End-to-End Live Pipeline (WORKERS_ENABLED=true: SQS -> Consumer/Inbox -> Worker -> Outbox Publisher -> SQS)', () => {
  test('processes out-of-order SQS messages and redeliveries end-to-end across live background loops and reconciles every wallet', async () => {
    await publishAllDueOutbox(publisher1);
    await drainQueue(sqs, eventsQueueUrl);

    await withIsolatedFifoQueues({}, async (queues) => {
      const liveApp = await startAppInstance({
        WORKERS_ENABLED: 'true',
        SQS_WAGER_TRANSACTIONS_QUEUE: queues.txQueueName,
        SQS_WAGER_TRANSACTIONS_DLQ: queues.dlqQueueName,
        SQS_INTEGRATION_EVENTS_QUEUE: queues.eventsQueueName,
        SQS_WAIT_TIME_SECONDS: '1',
        OUTBOX_POLL_INTERVAL_MS: '50',
        PENDING_REFERENCE_POLL_INTERVAL_MS: '100',
      });

      try {
        const liveUrl = await liveApp.getUrl();
        const playerId = newId();
        const openRes = await fetch(`${liveUrl}/wallets`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ playerId, initialBalance: { amount: '150.00', currency: 'BRL' } }),
        });
        expect(openRes.status).toBe(201);
        const wallet = (await openRes.json()) as OpenedWallet;
        createdWalletIds.push(wallet.id);

        const providerId = `prov-e2e-${newId().slice(-8)}`;
        const betExt = `e2e-bet-${newId()}`;
        const refundExt = `e2e-refund-${newId()}`;

        // 1. Send REFUND FIRST on SQS (out-of-order: BET hasn't arrived yet!).
        const refundEnv = buildWagerEnvelope(wallet, {
          providerId,
          externalTransactionId: refundExt,
          roundId: 'round-e2e-1',
          kind: WagerTransactionKind.Refund,
          money: { amount: '40.00', currency: 'BRL' },
          referenceExternalTransactionId: betExt,
        });
        // 2. Send BET SECOND on SQS.
        const betEnv = buildWagerEnvelope(wallet, {
          providerId,
          externalTransactionId: betExt,
          roundId: 'round-e2e-1',
          kind: WagerTransactionKind.Bet,
          money: { amount: '40.00', currency: 'BRL' },
        });

        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, refundEnv)));
        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(queues.txQueueUrl, betEnv)));
        // 3. Send a redelivery of BET (same messageId, distinct SQS DeduplicationId).
        await sqs.send(
          new SendMessageCommand({
            QueueUrl: queues.txQueueUrl,
            MessageBody: JSON.stringify(betEnv),
            MessageGroupId: wallet.id,
            MessageDeduplicationId: `e2e-redelivery-${newId()}`,
          }),
        );

        // Poll PostgreSQL until REFUND transitions PENDING_REFERENCE -> PROCESSED and all Outbox rows are published!
        const deadline = Date.now() + 8_000;
        let settled = false;
        while (Date.now() < deadline) {
          const [state] = await db`
            select
              (select count(*)::int from wager_transactions where wallet_id = ${wallet.id} and status = 'PROCESSED') as processed_tx,
              (select count(*)::int from outbox_messages where aggregate_id = ${wallet.id} and published_at is null) as unpublished_outbox,
              (select count(*)::int from outbox_messages where aggregate_id = ${wallet.id} and published_at is not null) as published_outbox
          `;
          // 3 PROCESSED transactions: OPENING + BET + REFUND; 7 published outbox events: OPENING(2) + PENDING(1) + BET(2) + REFUND(2)
          if (state.processed_tx === 3 && state.unpublished_outbox === 0 && state.published_outbox === 7) {
            settled = true;
            break;
          }
          await Bun.sleep(100);
        }
        expect(settled).toBe(true);

        const [walletFinal] = await db`select balance_amount, version from wallets where id = ${wallet.id}`;
        expect(walletFinal).toEqual({ balance_amount: '150.00', version: 3 });

        const inboxRows = await db`
          select message_id, processed_at
          from inbox_messages
          where consumer_name = ${WAGER_CONSUMER_NAME}
            and message_id in (${refundEnv.messageId}, ${betEnv.messageId})
        `;
        expect(inboxRows).toHaveLength(2);
        for (const r of inboxRows) {
          expect(r.processed_at).not.toBeNull();
        }

        const dbOutboxRows = await db`
          select id, sequence_number, event_type
          from outbox_messages
          where aggregate_id = ${wallet.id}
          order by sequence_number asc
        `;
        const publishedMessages = await drainQueue(sqs, queues.eventsQueueUrl);
        const walletEvents = publishedMessages
          .map((m) => ({
            sqsSeq: BigInt(m.Attributes?.SequenceNumber ?? '0'),
            env: JSON.parse(m.Body ?? '{}') as Json,
          }))
          .filter((e) => e.env.aggregateId === wallet.id);

        expect(walletEvents.map((e) => e.env.eventId)).toEqual(dbOutboxRows.map((r: Json) => r.id));
        for (let i = 1; i < walletEvents.length; i += 1) {
          expect(walletEvents[i]!.sqsSeq > walletEvents[i - 1]!.sqsSeq).toBe(true);
        }

        expect(walletEvents.map((e) => e.env.eventType)).toEqual([
          'WagerTransactionProcessed', // OPENING
          'WalletBalanceChanged', // OPENING
          'WagerTransactionPendingReference', // REFUND initially parked
          'WagerTransactionProcessed', // BET
          'WalletBalanceChanged', // BET (-40.00 -> 110.00)
          'WagerTransactionProcessed', // REFUND resolved by PendingReferenceWorker
          'WalletBalanceChanged', // REFUND (+40.00 -> 150.00)
        ]);
      } finally {
        await liveApp.close();
      }
    });

    // Global reconciliation across EVERY wallet created in this integration suite!
    for (const walletId of createdWalletIds) {
      const recon = await call('POST', `/wallets/${walletId}/reconciliation`);
      expect(recon.status).toBe(200);
      expect(recon.body.consistent).toBe(true);
      expect(recon.body.difference.amount).toBe('0.00');
    }
  }, 15_000);
});
