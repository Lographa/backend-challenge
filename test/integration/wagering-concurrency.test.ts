import 'reflect-metadata';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { DomainError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { canonicalHash } from '../../src/common/hashing/canonical-hash';
import { newId } from '../../src/common/ids';
import { InboxMessageRecord } from '../../src/database/entities';
import { parseWagerRequest } from '../../src/wagering/wager-payload';
import { WagerProcessorService } from '../../src/wagering/wager-processor.service';
import { WagerTransactionStatus } from '../../src/wagering/wager-transaction';
import { createTestOrm, createTestSql, resetSchema, testDatabaseUrl, testRoleName } from '../support/database';

type Json = Record<string, any>;

interface HttpResult {
  status: number;
  body: Json;
  headers: Headers;
}

const INSTANCE_COUNT = 3;
const POOL_PER_INSTANCE = 25;
const BARRIER_TIMEOUT_MS = 8_000;
const BARRIER_POLL_MS = 10;

const apps: INestApplication[] = [];
const baseUrls: string[] = [];
let ormWinner: MikroORM;
let ormLoser: MikroORM;
let winnerProcessor: WagerProcessorService;
let loserProcessor: WagerProcessorService;
let db: SQL;
let gateSql: SQL;
const trackedWalletIds = new Set<string>();

beforeAll(async () => {
  const migrator = await createTestOrm('migrator');
  try {
    await resetSchema(migrator);
  } finally {
    await migrator.close();
  }

  for (let i = 0; i < INSTANCE_COUNT; i++) {
    const config = AppConfig.load({
      ...process.env,
      DATABASE_URL: testDatabaseUrl('app'),
      DATABASE_POOL_MAX: String(POOL_PER_INSTANCE),
    });
    const instance = await NestFactory.create(AppModule.forRoot(config), { logger: false });
    await instance.listen(0);
    apps.push(instance);
    baseUrls.push(await instance.getUrl());
  }

  ormWinner = await createTestOrm('app');
  ormLoser = await createTestOrm('app');
  winnerProcessor = new WagerProcessorService(ormWinner);
  loserProcessor = new WagerProcessorService(ormLoser);
  db = createTestSql('app');
  gateSql = createTestSql('app');
});

afterAll(async () => {
  await gateSql?.close();
  await db?.close();
  await ormLoser?.close();
  await ormWinner?.close();
  for (const instance of apps) {
    await instance.close();
  }
});

async function callInstance(
  instanceIndex: number,
  method: 'GET' | 'POST',
  path: string,
  body?: unknown,
  headers: Record<string, string> = {},
): Promise<HttpResult> {
  const targetUrl = baseUrls[instanceIndex % baseUrls.length]!;
  const response = await fetch(`${targetUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as Json, headers: response.headers };
}

async function openWallet(amount: string, playerId = newId(), currency = 'BRL'): Promise<Json> {
  const response = await callInstance(0, 'POST', '/wallets', {
    playerId,
    initialBalance: { amount, currency },
  });
  expect(response.status).toBe(201);
  trackedWalletIds.add(response.body.id);
  return response.body;
}

async function submitWagerOn(
  instanceIndex: number,
  payload: Record<string, unknown>,
  idempotencyKey?: string,
  extraHeaders: Record<string, string> = {},
): Promise<HttpResult> {
  const key =
    idempotencyKey ??
    `${String(payload.providerId ?? 'provider-a')}:${String(payload.externalTransactionId ?? newId())}`;
  return callInstance(instanceIndex, 'POST', '/wagering/transactions', payload, {
    'idempotency-key': key,
    ...extraHeaders,
  });
}

async function outboxEventsForTransaction(transactionId: string): Promise<Json[]> {
  return db`
    select id, sequence_number, event_type, aggregate_id, payload
    from outbox_messages
    where payload -> 'data' ->> 'transactionId' = ${transactionId}
    order by sequence_number asc
  `;
}

/**
 * Deterministic PostgreSQL row-lock barrier:
 * 1. An independent connection (`gateSql`) locks the wallet row with `SELECT ... FOR UPDATE`.
 * 2. All `expectedWaiters` concurrent tasks start and reach PostgreSQL's lock queue on independent connections.
 * 3. `db` inspects `pg_stat_activity` until `count(distinct pid) === expectedWaiters` backends are actively
 *    blocked on `wait_event_type = 'Lock'`.
 * 4. Only then is the gate released so all contenders race inside PostgreSQL simultaneously.
 */
async function withWalletLockGate<T>(
  walletId: string,
  expectedWaiters: number,
  launchContenders: () => Promise<T>,
): Promise<{ result: T; waitingPids: number[]; gatePid: number }> {
  const gateLocked = Promise.withResolvers<number>();
  const releaseGate = Promise.withResolvers<void>();

  const gateTransaction = gateSql.begin(async (tx) => {
    const [{ pid }] = await tx`select pg_backend_pid()::int as pid`;
    await tx`select id from wallets where id = ${walletId} for update`;
    gateLocked.resolve(pid);
    await releaseGate.promise;
  });

  const gatePid = await gateLocked.promise;
  let contendersPromise: Promise<T> | undefined;
  let waitingPids: number[] = [];

  try {
    contendersPromise = launchContenders();
    waitingPids = await waitForLockWaiters(gatePid, expectedWaiters);
  } finally {
    releaseGate.resolve();
    await gateTransaction;
  }

  return { result: await contendersPromise!, waitingPids, gatePid };
}

async function waitForLockWaiters(excludedPid: number, expectedWaiters: number): Promise<number[]> {
  const deadline = Date.now() + BARRIER_TIMEOUT_MS;
  let lastPids: number[] = [];
  while (Date.now() < deadline) {
    const rows: Array<{ pid: number }> = await db`
      select distinct pid::int as pid
      from pg_stat_activity
      where datname = current_database()
        and usename = current_user
        and pid <> pg_backend_pid()
        and pid <> ${excludedPid}
        and state = 'active'
        and wait_event_type = 'Lock'
      order by pid asc
    `;
    lastPids = rows.map((r) => r.pid);
    if (lastPids.length >= expectedWaiters) {
      return lastPids;
    }
    await Bun.sleep(BARRIER_POLL_MS);
  }
  const snapshot = await db`
    select pid, state, wait_event_type, wait_event, left(query, 90) as query
    from pg_stat_activity
    where datname = current_database() and usename = current_user
  `;
  throw new Error(
    `Timed out waiting for ${expectedWaiters} PostgreSQL lock waiter(s) (saw ${lastPids.length}): ${JSON.stringify(snapshot)}`,
  );
}

/**
 * Deterministic barrier for SAVEPOINT unique-index collisions (`23505`):
 * Confirms `expectedWaiters` distinct PostgreSQL backends have ALREADY bypassed `wallets FOR UPDATE`,
 * `findCollisions`, and `hasActiveReversal`, and are blocked inside `SAVEPOINT` executing
 * `insert into "wager_transactions"` waiting on the uncommitted winner's `transactionid` lock.
 */
async function waitForUniqueIndexInsertWaiters(winnerPid: number, expectedWaiters: number): Promise<number[]> {
  const deadline = Date.now() + BARRIER_TIMEOUT_MS;
  let lastPids: number[] = [];
  while (Date.now() < deadline) {
    const rows: Array<{ pid: number }> = await db`
      select distinct pid::int as pid
      from pg_stat_activity
      where datname = current_database()
        and usename = current_user
        and pid <> pg_backend_pid()
        and pid <> ${winnerPid}
        and state = 'active'
        and wait_event_type = 'Lock'
        and wait_event = 'transactionid'
        and lower(query) like 'insert into "wager_transactions"%'
      order by pid asc
    `;
    lastPids = rows.map((r) => r.pid);
    if (lastPids.length >= expectedWaiters) {
      return lastPids;
    }
    await Bun.sleep(BARRIER_POLL_MS);
  }
  const snapshot = await db`
    select pid, state, wait_event_type, wait_event, left(query, 120) as query
    from pg_stat_activity
    where datname = current_database() and usename = current_user
  `;
  throw new Error(
    `Timed out waiting for ${expectedWaiters} unique-index insert waiter(s) (saw ${lastPids.length}): ${JSON.stringify(snapshot)}`,
  );
}

describe('Database isolation guard', () => {
  test('connects exclusively to wagering_test as wagering_app', async () => {
    const [session] = await db`select current_database() as database_name, current_user as role_name`;
    expect(session!.database_name).toBe('wagering_test');
    expect(session!.database_name).not.toBe('wagering');
    expect(session!.role_name).toBe(testRoleName('app'));
  });
});

describe('Scenario A — 50 identical concurrent requests on the same wallet', () => {
  test('50 simultaneous identical BETs across 3 instances execute on 50 connections, apply 1 debit, and preserve historical observedBalance on replay', async () => {
    const wallet = await openWallet('250.00');
    const providerId = `prov-a-${newId()}`;
    const externalTransactionId = 'bet-identical-50';
    const idempotencyKey = `${providerId}:${externalTransactionId}`;
    const payload = {
      providerId,
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-scenario-a',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '35.00', currency: 'BRL' },
    };

    const { result: responses, waitingPids, gatePid } = await withWalletLockGate(wallet.id, 50, () =>
      Promise.all(
        Array.from({ length: 50 }, (_, index) =>
          submitWagerOn(index, payload, idempotencyKey, { 'x-correlation-id': `corr-a-${index}` }),
        ),
      ),
    );

    // 50 independent PostgreSQL backend connections contended simultaneously on the wallet row lock
    expect(waitingPids).toHaveLength(50);
    expect(new Set(waitingPids).size).toBe(50);
    expect(waitingPids.includes(gatePid)).toBe(false);
    expect(responses).toHaveLength(50);

    for (const res of responses) {
      expect(res.status).toBe(200);
      expect(res.body.status).toBe('PROCESSED');
      expect(res.body.balance).toEqual({ amount: '215.00', currency: 'BRL' });
    }

    const uniqueTxIds = new Set(responses.map((r) => r.body.transactionId as string));
    expect(uniqueTxIds.size).toBe(1);
    const [betTransactionId] = [...uniqueTxIds];

    const originals = responses.filter((r) => r.body.idempotentReplay === false);
    const replays = responses.filter((r) => r.body.idempotentReplay === true);
    expect(originals).toHaveLength(1);
    expect(replays).toHaveLength(49);

    // Direct PostgreSQL verification: 1 debit, 1 BET wager_transaction, 1 BET ledger entry, 2 BET outbox events
    const [walletRow] = await db`
      select balance_amount::text as balance, version
      from wallets
      where id = ${wallet.id}
    `;
    expect(walletRow).toEqual({ balance: '215.00', version: 2 });

    const wagerRows = await db`
      select id, kind, status, amount::text as amount, observed_balance_amount::text as observed
      from wager_transactions
      where wallet_id = ${wallet.id}
      order by created_at asc
    `;
    expect(wagerRows).toHaveLength(2); // OPENING + 1 BET
    const betRows = wagerRows.filter((r: Json) => r.kind === 'BET');
    expect(betRows).toEqual([
      {
        id: betTransactionId,
        kind: 'BET',
        status: 'PROCESSED',
        amount: '35.00',
        observed: '215.00',
      },
    ]);

    const ledgerRows = await db`
      select transaction_id, direction, amount::text as amount,
             balance_before::text as before, balance_after::text as after, wallet_version
      from wallet_ledger_entries
      where wallet_id = ${wallet.id}
      order by wallet_version asc
    `;
    expect(ledgerRows).toEqual([
      {
        transaction_id: expect.any(String),
        direction: 'CREDIT',
        amount: '250.00',
        before: '0.00',
        after: '250.00',
        wallet_version: 1,
      },
      {
        transaction_id: betTransactionId,
        direction: 'DEBIT',
        amount: '35.00',
        before: '250.00',
        after: '215.00',
        wallet_version: 2,
      },
    ]);

    const betOutbox = await outboxEventsForTransaction(betTransactionId!);
    expect(betOutbox.map((e) => e.event_type)).toEqual([
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
    ]);
    expect(BigInt(betOutbox[0]!.sequence_number)).toBeLessThan(BigInt(betOutbox[1]!.sequence_number));

    const [{ totalWalletEvents }] = await db`
      select count(*)::int as "totalWalletEvents"
      from outbox_messages
      where aggregate_id = ${wallet.id}
    `;
    expect(totalWalletEvents).toBe(4); // 2 OPENING + 2 BET

    // Criterion 7: Move balance to 265.00 via WIN, then send 15 concurrent replays of the original BET.
    // Replays must still return historical observedBalance = 215.00 without touching 265.00.
    const laterWin = await submitWagerOn(1, {
      providerId,
      externalTransactionId: 'win-after-50',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-scenario-a',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: externalTransactionId,
    });
    expect(laterWin.status).toBe(200);
    expect(laterWin.body.balance).toEqual({ amount: '265.00', currency: 'BRL' });

    const { result: lateReplays, waitingPids: replayPids } = await withWalletLockGate(wallet.id, 15, () =>
      Promise.all(Array.from({ length: 15 }, (_, idx) => submitWagerOn(idx, payload, idempotencyKey))),
    );
    expect(new Set(replayPids).size).toBe(15);
    for (const replay of lateReplays) {
      expect(replay.status).toBe(200);
      expect(replay.body).toEqual({
        transactionId: betTransactionId,
        status: 'PROCESSED',
        balance: { amount: '215.00', currency: 'BRL' },
        idempotentReplay: true,
      });
    }

    const walletGet = await callInstance(2, 'GET', `/wallets/${wallet.id}`);
    expect(walletGet.status).toBe(200);
    expect(walletGet.body).toEqual({
      id: wallet.id,
      playerId: wallet.playerId,
      balance: { amount: '265.00', currency: 'BRL' },
      version: 3,
    });

    const reconciliation = await callInstance(0, 'POST', `/wallets/${wallet.id}/reconciliation`);
    expect(reconciliation.status).toBe(200);
    expect(reconciliation.body).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '265.00', currency: 'BRL' },
      calculatedBalance: { amount: '265.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 3,
    });
  });
});

describe('Scenario B — Two concurrent 80.00 bets competing for a 100.00 balance', () => {
  test('processes exactly one 80.00 BET, rejects the other with INSUFFICIENT_FUNDS, leaves 20.00, and keeps ledger/outbox consistent', async () => {
    const wallet = await openWallet('100.00');
    const providerId = `prov-b-${newId()}`;

    const betAPayload = {
      providerId,
      externalTransactionId: 'bet-80-alpha',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-b-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '80.00', currency: 'BRL' },
    };
    const betBPayload = {
      providerId,
      externalTransactionId: 'bet-80-beta',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-b-2',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '80.00', currency: 'BRL' },
    };

    const keyA = `${providerId}:bet-80-alpha`;
    const keyB = `${providerId}:bet-80-beta`;

    const { result: responses, waitingPids, gatePid } = await withWalletLockGate(wallet.id, 2, () =>
      Promise.all([
        submitWagerOn(0, betAPayload, keyA, { 'x-correlation-id': 'corr-b-alpha' }),
        submitWagerOn(1, betBPayload, keyB, { 'x-correlation-id': 'corr-b-beta' }),
      ]),
    );

    expect(new Set(waitingPids).size).toBe(2);
    expect(waitingPids.includes(gatePid)).toBe(false);

    const processedResponses = responses.filter((r) => r.status === 200);
    const rejectedResponses = responses.filter((r) => r.status === 422);
    expect(processedResponses).toHaveLength(1);
    expect(rejectedResponses).toHaveLength(1);

    const winner = processedResponses[0]!;
    const loser = rejectedResponses[0]!;

    expect(winner.body).toEqual({
      transactionId: expect.any(String),
      status: 'PROCESSED',
      balance: { amount: '20.00', currency: 'BRL' },
      idempotentReplay: false,
    });
    expect(loser.body).toEqual({
      transactionId: expect.any(String),
      status: 'REJECTED',
      failureCode: FailureCode.InsufficientFunds,
      balance: { amount: '20.00', currency: 'BRL' },
      idempotentReplay: false,
    });
    expect(winner.body.transactionId).not.toBe(loser.body.transactionId);

    // Direct PostgreSQL inspection of wallet, wager_transactions, wallet_ledger_entries, and outbox_messages
    const [walletRow] = await db`
      select balance_amount::text as balance, version
      from wallets
      where id = ${wallet.id}
    `;
    expect(walletRow).toEqual({ balance: '20.00', version: 2 });

    const txRows = await db`
      select id, external_transaction_id, kind, status, failure_code,
             amount::text as amount, observed_balance_amount::text as observed,
             processed_at is not null as has_processed_at
      from wager_transactions
      where wallet_id = ${wallet.id} and kind = 'BET'
      order by status asc
    `;
    expect(txRows).toEqual([
      {
        id: winner.body.transactionId,
        external_transaction_id: expect.stringMatching(/^bet-80-(alpha|beta)$/),
        kind: 'BET',
        status: 'PROCESSED',
        failure_code: null,
        amount: '80.00',
        observed: '20.00',
        has_processed_at: true,
      },
      {
        id: loser.body.transactionId,
        external_transaction_id: expect.stringMatching(/^bet-80-(alpha|beta)$/),
        kind: 'BET',
        status: 'REJECTED',
        failure_code: 'INSUFFICIENT_FUNDS',
        amount: '80.00',
        observed: '20.00',
        has_processed_at: false,
      },
    ]);

    const ledgerRows = await db`
      select transaction_id, direction, amount::text as amount,
             balance_before::text as before, balance_after::text as after, wallet_version
      from wallet_ledger_entries
      where wallet_id = ${wallet.id}
      order by wallet_version asc
    `;
    expect(ledgerRows).toHaveLength(2);
    expect(ledgerRows.filter((r: Json) => r.direction === 'DEBIT')).toEqual([
      {
        transaction_id: winner.body.transactionId,
        direction: 'DEBIT',
        amount: '80.00',
        before: '100.00',
        after: '20.00',
        wallet_version: 2,
      },
    ]);

    const winnerEvents = await outboxEventsForTransaction(winner.body.transactionId);
    expect(winnerEvents.map((e) => e.event_type)).toEqual([
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
    ]);
    expect(BigInt(winnerEvents[0]!.sequence_number)).toBeLessThan(BigInt(winnerEvents[1]!.sequence_number));

    const loserEvents = await outboxEventsForTransaction(loser.body.transactionId);
    expect(loserEvents.map((e) => e.event_type)).toEqual(['WagerTransactionRejected']);
    expect(loserEvents[0]!.payload.data).toMatchObject({
      transactionId: loser.body.transactionId,
      failureCode: 'INSUFFICIENT_FUNDS',
      observedBalance: { amount: '20.00', currency: 'BRL' },
    });
    expect(BigInt(winnerEvents[1]!.sequence_number)).toBeLessThan(BigInt(loserEvents[0]!.sequence_number));

    const allWalletEvents = await db`
      select sequence_number, event_type from outbox_messages
      where aggregate_id = ${wallet.id}
      order by sequence_number asc
    `;
    expect(allWalletEvents.map((e: Json) => e.event_type)).toEqual([
      'WagerTransactionProcessed', // OPENING
      'WalletBalanceChanged',      // OPENING
      'WagerTransactionProcessed', // Winning 80.00 BET
      'WalletBalanceChanged',      // Winning 80.00 BET
      'WagerTransactionRejected',  // Rejected 80.00 BET
    ]);

    // Concurrent retries of both transactions never duplicate debits or alter state
    const [retryA, retryB] = await Promise.all([
      submitWagerOn(2, betAPayload, keyA),
      submitWagerOn(0, betBPayload, keyB),
    ]);
    expect([retryA.status, retryB.status].sort()).toEqual([200, 422]);
    expect(retryA.body.idempotentReplay).toBe(true);
    expect(retryB.body.idempotentReplay).toBe(true);

    const reconciliation = await callInstance(1, 'POST', `/wallets/${wallet.id}/reconciliation`);
    expect(reconciliation.status).toBe(200);
    expect(reconciliation.body).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '20.00', currency: 'BRL' },
      calculatedBalance: { amount: '20.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 2,
    });
  });
});

describe('Complementary 7.1 — Concurrent post-SAVEPOINT recovery across all three unique indexes', () => {
  test('recovers from 23505 on wager_transactions_idempotency_key_uq via ROLLBACK TO SAVEPOINT without aborting outer transaction', async () => {
    const walletA = await openWallet('100.00');
    const walletB = await openWallet('100.00');
    const walletC = await openWallet('100.00');
    const providerId = `prov-sp-key-${newId()}`;
    const sharedKey = `${providerId}:shared-idem-key`;

    const cmdA = parseWagerRequest(
      {
        providerId,
        externalTransactionId: 'ext-key-a',
        playerId: walletA.playerId,
        walletId: walletA.id,
        roundId: 'round-sp-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '20.00', currency: 'BRL' },
      },
      sharedKey,
    );
    const cmdB = parseWagerRequest(
      {
        providerId,
        externalTransactionId: 'ext-key-b',
        playerId: walletB.playerId,
        walletId: walletB.id,
        roundId: 'round-sp-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '20.00', currency: 'BRL' },
      },
      sharedKey,
    );
    const payloadC = {
      providerId,
      externalTransactionId: 'ext-key-c',
      playerId: walletC.playerId,
      walletId: walletC.id,
      roundId: 'round-sp-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '20.00', currency: 'BRL' },
    };

    const winnerMsgId = `msg-win-key-${newId()}`;
    const loserMsgId = `msg-lose-key-${newId()}`;
    const winnerInserted = Promise.withResolvers<number>();
    const releaseWinnerCommit = Promise.withResolvers<void>();

    // Connection 1 (ormWinner): inserts into wager_transactions and holds outer SQL transaction uncommitted
    const winnerTask = ormWinner.em.fork().transactional(async (txEm) => {
      const pidRows = await txEm.execute<Array<{ pid: number }>>('select pg_backend_pid()::int as pid');
      const pid = pidRows[0]!.pid;
      await txEm.insert(InboxMessageRecord, {
        consumerName: 'wager-transactions',
        messageId: winnerMsgId,
        payloadHash: canonicalHash({ winnerMsgId }),
        receivedAt: new Date(),
        processedAt: null,
      });
      const res = await winnerProcessor.processInTransaction(txEm, {
        ...cmdA,
        correlationId: 'corr-sp-key-win',
      });
      await txEm.nativeUpdate(
        InboxMessageRecord,
        { consumerName: 'wager-transactions', messageId: winnerMsgId },
        { processedAt: new Date() },
      );
      winnerInserted.resolve(pid);
      await releaseWinnerCommit.promise;
      return res;
    });

    const winnerPid = await winnerInserted.promise;

    // Connection 2 (ormLoser outer transaction with inbox) + Connection 3 (HTTP on app2):
    // Both lock their own wallets (walletB, walletC), see findCollisions = [], enter SAVEPOINT,
    // and block on `insert into "wager_transactions"` waiting on winnerPid's transactionid lock.
    const loserTxTask = ormLoser.em.fork().transactional(async (txEm) => {
      const isolationRows = await txEm.execute<Array<{ isolation: string }>>(
        "select current_setting('transaction_isolation') as isolation",
      );
      expect(isolationRows[0]!.isolation).toBe('read committed');

      await txEm.insert(InboxMessageRecord, {
        consumerName: 'wager-transactions',
        messageId: loserMsgId,
        payloadHash: canonicalHash({ loserMsgId }),
        receivedAt: new Date(),
        processedAt: null,
      });

      let caught: unknown;
      try {
        await loserProcessor.processInTransaction(txEm, {
          ...cmdB,
          correlationId: 'corr-sp-key-lose-tx',
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(DomainError);
      expect((caught as DomainError).code).toBe(FailureCode.IdempotencyPayloadConflict);

      // Proves outer SQL transaction is NOT aborted (no 25P02) after 23505 + ROLLBACK TO SAVEPOINT
      await txEm.nativeUpdate(
        InboxMessageRecord,
        { consumerName: 'wager-transactions', messageId: loserMsgId },
        { processedAt: new Date() },
      );
    });

    const loserHttpTask = submitWagerOn(1, payloadC, sharedKey, { 'x-correlation-id': 'corr-sp-key-lose-http' });

    try {
      const blockedPids = await waitForUniqueIndexInsertWaiters(winnerPid, 2);
      expect(blockedPids).toHaveLength(2);
    } finally {
      releaseWinnerCommit.resolve();
    }

    const [winnerResult, , loserHttpResult] = await Promise.all([winnerTask, loserTxTask, loserHttpTask]);

    expect(winnerResult.idempotentReplay).toBe(false);
    expect(winnerResult.transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(winnerResult.transaction.observedBalance?.toJSON()).toEqual({ amount: '80.00', currency: 'BRL' });

    expect(loserHttpResult.status).toBe(409);
    expect(loserHttpResult.body.code).toBe(FailureCode.IdempotencyPayloadConflict);

    // Outer transaction of loser persisted inbox processed_at cleanly
    const [loserInbox] = await db`
      select processed_at from inbox_messages
      where consumer_name = 'wager-transactions' and message_id = ${loserMsgId}
    `;
    expect(loserInbox!.processed_at).not.toBeNull();

    // Winner walletA moved once; walletB and walletC suffered zero financial, ledger, or outbox side effects
    const walletStates = await db`
      select w.id, w.balance_amount::text as balance, w.version,
             (select count(*)::int from wallet_ledger_entries e where e.wallet_id = w.id) as entries,
             (select count(*)::int from outbox_messages o where o.aggregate_id = w.id) as events
      from wallets w
      where w.id in (${walletA.id}, ${walletB.id}, ${walletC.id})
    `;
    const byId = new Map(walletStates.map((w: Json) => [w.id, w]));
    expect(byId.get(walletA.id)).toEqual({ id: walletA.id, balance: '80.00', version: 2, entries: 2, events: 4 });
    expect(byId.get(walletB.id)).toEqual({ id: walletB.id, balance: '100.00', version: 1, entries: 1, events: 2 });
    expect(byId.get(walletC.id)).toEqual({ id: walletC.id, balance: '100.00', version: 1, entries: 1, events: 2 });

    const keyRows = await db`
      select id, wallet_id, status from wager_transactions where idempotency_key = ${sharedKey}
    `;
    expect(keyRows).toEqual([{ id: winnerResult.transaction.id, wallet_id: walletA.id, status: 'PROCESSED' }]);
  });

  test('recovers from 23505 on wager_transactions_provider_external_uq via ROLLBACK TO SAVEPOINT without aborting outer transaction', async () => {
    const walletA = await openWallet('100.00');
    const walletB = await openWallet('100.00');
    const walletC = await openWallet('100.00');
    const providerId = `prov-sp-ext-${newId()}`;
    const sharedExternalId = 'ext-shared-tx';

    const cmdA = parseWagerRequest(
      {
        providerId,
        externalTransactionId: sharedExternalId,
        playerId: walletA.playerId,
        walletId: walletA.id,
        roundId: 'round-sp-2',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '30.00', currency: 'BRL' },
      },
      `${providerId}:key-a`,
    );
    const cmdB = parseWagerRequest(
      {
        providerId,
        externalTransactionId: sharedExternalId,
        playerId: walletB.playerId,
        walletId: walletB.id,
        roundId: 'round-sp-2',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '15.00', currency: 'BRL' },
      },
      `${providerId}:key-b`,
    );
    const payloadC = {
      providerId,
      externalTransactionId: sharedExternalId,
      playerId: walletC.playerId,
      walletId: walletC.id,
      roundId: 'round-sp-2',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };

    const winnerMsgId = `msg-win-ext-${newId()}`;
    const loserMsgId = `msg-lose-ext-${newId()}`;
    const winnerInserted = Promise.withResolvers<number>();
    const releaseWinnerCommit = Promise.withResolvers<void>();

    const winnerTask = ormWinner.em.fork().transactional(async (txEm) => {
      const pidRows = await txEm.execute<Array<{ pid: number }>>('select pg_backend_pid()::int as pid');
      const pid = pidRows[0]!.pid;
      await txEm.insert(InboxMessageRecord, {
        consumerName: 'wager-transactions',
        messageId: winnerMsgId,
        payloadHash: canonicalHash({ winnerMsgId }),
        receivedAt: new Date(),
        processedAt: null,
      });
      const res = await winnerProcessor.processInTransaction(txEm, {
        ...cmdA,
        correlationId: 'corr-sp-ext-win',
      });
      await txEm.nativeUpdate(
        InboxMessageRecord,
        { consumerName: 'wager-transactions', messageId: winnerMsgId },
        { processedAt: new Date() },
      );
      winnerInserted.resolve(pid);
      await releaseWinnerCommit.promise;
      return res;
    });

    const winnerPid = await winnerInserted.promise;

    const loserTxTask = ormLoser.em.fork().transactional(async (txEm) => {
      await txEm.insert(InboxMessageRecord, {
        consumerName: 'wager-transactions',
        messageId: loserMsgId,
        payloadHash: canonicalHash({ loserMsgId }),
        receivedAt: new Date(),
        processedAt: null,
      });

      let caught: unknown;
      try {
        await loserProcessor.processInTransaction(txEm, {
          ...cmdB,
          correlationId: 'corr-sp-ext-lose-tx',
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(DomainError);
      expect((caught as DomainError).code).toBe(FailureCode.IdempotencyPayloadConflict);

      await txEm.nativeUpdate(
        InboxMessageRecord,
        { consumerName: 'wager-transactions', messageId: loserMsgId },
        { processedAt: new Date() },
      );
    });

    const loserHttpTask = submitWagerOn(2, payloadC, `${providerId}:key-c`);

    try {
      const blockedPids = await waitForUniqueIndexInsertWaiters(winnerPid, 2);
      expect(blockedPids).toHaveLength(2);
    } finally {
      releaseWinnerCommit.resolve();
    }

    const [winnerResult, , loserHttpResult] = await Promise.all([winnerTask, loserTxTask, loserHttpTask]);

    expect(winnerResult.transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(winnerResult.transaction.observedBalance?.toJSON()).toEqual({ amount: '70.00', currency: 'BRL' });

    expect(loserHttpResult.status).toBe(409);
    expect(loserHttpResult.body.code).toBe(FailureCode.IdempotencyPayloadConflict);

    const [loserInbox] = await db`
      select processed_at from inbox_messages
      where consumer_name = 'wager-transactions' and message_id = ${loserMsgId}
    `;
    expect(loserInbox!.processed_at).not.toBeNull();

    const extRows = await db`
      select id, wallet_id, idempotency_key, status
      from wager_transactions
      where provider_id = ${providerId} and external_transaction_id = ${sharedExternalId}
    `;
    expect(extRows).toEqual([
      {
        id: winnerResult.transaction.id,
        wallet_id: walletA.id,
        idempotency_key: `${providerId}:key-a`,
        status: 'PROCESSED',
      },
    ]);

    const walletBAndC = await db`
      select id, balance_amount::text as balance, version,
             (select count(*)::int from wallet_ledger_entries e where e.wallet_id = w.id) as entries,
             (select count(*)::int from outbox_messages o where o.aggregate_id = w.id) as events
      from wallets w
      where w.id in (${walletB.id}, ${walletC.id})
    `;
    expect(walletBAndC).toEqual([
      { id: expect.any(String), balance: '100.00', version: 1, entries: 1, events: 2 },
      { id: expect.any(String), balance: '100.00', version: 1, entries: 1, events: 2 },
    ]);
  });

  test('recovers from 23505 on wager_transactions_single_active_reversal_uq via ROLLBACK TO SAVEPOINT and persists REJECTED + Outbox event', async () => {
    const walletA = await openWallet('100.00');
    const walletB = await openWallet('60.00');
    const walletC = await openWallet('75.00');
    const providerId = `prov-sp-rev-${newId()}`;
    const missingBetExternalId = 'bet-not-arrived-sp';

    const cmdWinnerRefund = parseWagerRequest(
      {
        providerId,
        externalTransactionId: 'refund-sp-winner',
        playerId: walletA.playerId,
        walletId: walletA.id,
        roundId: 'round-sp-3',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '25.00', currency: 'BRL' },
        referenceExternalTransactionId: missingBetExternalId,
      },
      `${providerId}:refund-sp-winner`,
    );
    const cmdLoserTxRefund = parseWagerRequest(
      {
        providerId,
        externalTransactionId: 'refund-sp-loser-tx',
        playerId: walletB.playerId,
        walletId: walletB.id,
        roundId: 'round-sp-3',
        gameId: 'fortune-chimp',
        kind: 'REFUND',
        money: { amount: '25.00', currency: 'BRL' },
        referenceExternalTransactionId: missingBetExternalId,
      },
      `${providerId}:refund-sp-loser-tx`,
    );
    const payloadLoserHttpRollback = {
      providerId,
      externalTransactionId: 'rollback-sp-loser-http',
      playerId: walletC.playerId,
      walletId: walletC.id,
      roundId: 'round-sp-3',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '25.00', currency: 'BRL' },
      referenceExternalTransactionId: missingBetExternalId,
    };

    const winnerMsgId = `msg-win-rev-${newId()}`;
    const loserMsgId = `msg-lose-rev-${newId()}`;
    const winnerInserted = Promise.withResolvers<number>();
    const releaseWinnerCommit = Promise.withResolvers<void>();

    const winnerTask = ormWinner.em.fork().transactional(async (txEm) => {
      const pidRows = await txEm.execute<Array<{ pid: number }>>('select pg_backend_pid()::int as pid');
      const pid = pidRows[0]!.pid;
      await txEm.insert(InboxMessageRecord, {
        consumerName: 'wager-transactions',
        messageId: winnerMsgId,
        payloadHash: canonicalHash({ winnerMsgId }),
        receivedAt: new Date(),
        processedAt: null,
      });
      const res = await winnerProcessor.processInTransaction(txEm, {
        ...cmdWinnerRefund,
        correlationId: 'corr-sp-rev-win',
      });
      await txEm.nativeUpdate(
        InboxMessageRecord,
        { consumerName: 'wager-transactions', messageId: winnerMsgId },
        { processedAt: new Date() },
      );
      winnerInserted.resolve(pid);
      await releaseWinnerCommit.promise;
      return res;
    });

    const winnerPid = await winnerInserted.promise;

    const loserTxTask = ormLoser.em.fork().transactional(async (txEm) => {
      await txEm.insert(InboxMessageRecord, {
        consumerName: 'wager-transactions',
        messageId: loserMsgId,
        payloadHash: canonicalHash({ loserMsgId }),
        receivedAt: new Date(),
        processedAt: null,
      });
      const res = await loserProcessor.processInTransaction(txEm, {
        ...cmdLoserTxRefund,
        correlationId: 'corr-sp-rev-lose-tx',
      });
      // Outer SQL transaction remains healthy after 23505 -> ROLLBACK TO SAVEPOINT -> second SAVEPOINT write
      await txEm.nativeUpdate(
        InboxMessageRecord,
        { consumerName: 'wager-transactions', messageId: loserMsgId },
        { processedAt: new Date() },
      );
      return res;
    });

    const loserHttpTask = submitWagerOn(1, payloadLoserHttpRollback, `${providerId}:rollback-sp-loser-http`, {
      'x-correlation-id': 'corr-sp-rev-lose-http',
    });

    try {
      const blockedPids = await waitForUniqueIndexInsertWaiters(winnerPid, 2);
      expect(blockedPids).toHaveLength(2);
    } finally {
      releaseWinnerCommit.resolve();
    }

    const [winnerResult, loserTxResult, loserHttpResult] = await Promise.all([
      winnerTask,
      loserTxTask,
      loserHttpTask,
    ]);

    expect(winnerResult.idempotentReplay).toBe(false);
    expect(winnerResult.transaction.status).toBe(WagerTransactionStatus.PendingReference);

    expect(loserTxResult.idempotentReplay).toBe(false);
    expect(loserTxResult.transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(loserTxResult.transaction.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);
    expect(loserTxResult.transaction.observedBalance?.toJSON()).toEqual({ amount: '60.00', currency: 'BRL' });

    expect(loserHttpResult.status).toBe(422);
    expect(loserHttpResult.body).toEqual({
      transactionId: expect.any(String),
      status: 'REJECTED',
      failureCode: FailureCode.ReferenceAlreadyReversed,
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: false,
    });

    // Verify inbox atomicity for both outer transactions
    const inboxRows = await db`
      select message_id, processed_at is not null as processed
      from inbox_messages
      where consumer_name = 'wager-transactions' and message_id in (${winnerMsgId}, ${loserMsgId})
      order by message_id asc
    `;
    expect(inboxRows).toHaveLength(2);
    expect(inboxRows.every((r: Json) => r.processed === true)).toBe(true);

    // Verify persisted reversal rows and Outbox events: 1 PENDING_REFERENCE + 2 REJECTED
    const reversalRows = await db`
      select id, external_transaction_id, kind, status, failure_code, observed_balance_amount::text as observed
      from wager_transactions
      where provider_id = ${providerId} and reference_external_transaction_id = ${missingBetExternalId}
      order by external_transaction_id asc
    `;
    expect(reversalRows).toEqual([
      {
        id: loserTxResult.transaction.id,
        external_transaction_id: 'refund-sp-loser-tx',
        kind: 'REFUND',
        status: 'REJECTED',
        failure_code: 'REFERENCE_ALREADY_REVERSED',
        observed: '60.00',
      },
      {
        id: winnerResult.transaction.id,
        external_transaction_id: 'refund-sp-winner',
        kind: 'REFUND',
        status: 'PENDING_REFERENCE',
        failure_code: null,
        observed: null,
      },
      {
        id: loserHttpResult.body.transactionId,
        external_transaction_id: 'rollback-sp-loser-http',
        kind: 'ROLLBACK',
        status: 'REJECTED',
        failure_code: 'REFERENCE_ALREADY_REVERSED',
        observed: '75.00',
      },
    ]);

    const winnerOutbox = await outboxEventsForTransaction(winnerResult.transaction.id);
    expect(winnerOutbox.map((e) => e.event_type)).toEqual(['WagerTransactionPendingReference']);

    const loserTxOutbox = await outboxEventsForTransaction(loserTxResult.transaction.id);
    expect(loserTxOutbox.map((e) => e.event_type)).toEqual(['WagerTransactionRejected']);

    const loserHttpOutbox = await outboxEventsForTransaction(loserHttpResult.body.transactionId);
    expect(loserHttpOutbox.map((e) => e.event_type)).toEqual(['WagerTransactionRejected']);

    // Replaying the post-SAVEPOINT REJECTED reversal returns idempotentReplay: true with historical balance
    const replayLoserHttp = await submitWagerOn(
      0,
      payloadLoserHttpRollback,
      `${providerId}:rollback-sp-loser-http`,
    );
    expect(replayLoserHttp.status).toBe(422);
    expect(replayLoserHttp.body).toEqual({
      transactionId: loserHttpResult.body.transactionId,
      status: 'REJECTED',
      failureCode: FailureCode.ReferenceAlreadyReversed,
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: true,
    });

    // Zero unintended financial movements on any of the three wallets
    const walletsCheck = await db`
      select id, balance_amount::text as balance, version
      from wallets
      where id in (${walletA.id}, ${walletB.id}, ${walletC.id})
    `;
    const map = new Map(walletsCheck.map((w: Json) => [w.id, w]));
    expect(map.get(walletA.id)).toEqual({ id: walletA.id, balance: '100.00', version: 1 });
    expect(map.get(walletB.id)).toEqual({ id: walletB.id, balance: '60.00', version: 1 });
    expect(map.get(walletC.id)).toEqual({ id: walletC.id, balance: '75.00', version: 1 });
  });

  test('same-wallet concurrent REFUND and ROLLBACK on a PROCESSED BET: credits once and rejects the second reversal', async () => {
    const wallet = await openWallet('100.00');
    const providerId = `prov-same-rev-${newId()}`;

    const bet = await submitWagerOn(0, {
      providerId,
      externalTransactionId: 'bet-to-reverse',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-rev-same',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '40.00', currency: 'BRL' },
    });
    expect(bet.status).toBe(200);
    expect(bet.body.balance).toEqual({ amount: '60.00', currency: 'BRL' });

    const { result: reversals, waitingPids } = await withWalletLockGate(wallet.id, 2, () =>
      Promise.all([
        submitWagerOn(1, {
          providerId,
          externalTransactionId: 'refund-race',
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-rev-same',
          gameId: 'fortune-chimp',
          kind: 'REFUND',
          money: { amount: '40.00', currency: 'BRL' },
          referenceExternalTransactionId: 'bet-to-reverse',
        }),
        submitWagerOn(2, {
          providerId,
          externalTransactionId: 'rollback-race',
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-rev-same',
          gameId: 'fortune-chimp',
          kind: 'ROLLBACK',
          money: { amount: '40.00', currency: 'BRL' },
          referenceExternalTransactionId: 'bet-to-reverse',
        }),
      ]),
    );

    expect(new Set(waitingPids).size).toBe(2);
    expect(reversals.map((r) => r.status).sort()).toEqual([200, 422]);
    const winner = reversals.find((r) => r.status === 200)!;
    const loser = reversals.find((r) => r.status === 422)!;
    expect(winner.body.balance).toEqual({ amount: '100.00', currency: 'BRL' });
    expect(loser.body.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);
    expect(loser.body.balance).toEqual({ amount: '100.00', currency: 'BRL' });

    const [walletRow] = await db`select balance_amount::text as balance, version from wallets where id = ${wallet.id}`;
    expect(walletRow).toEqual({ balance: '100.00', version: 3 });
  });
});

describe('Complementary 7.2 — AMOUNT_OUT_OF_RANGE integration tests on credits near numeric(19,2)', () => {
  test('rejects WIN that would overflow numeric(19,2) with 422 AMOUNT_OUT_OF_RANGE, preserving wallet and ledger', async () => {
    const wallet = await openWallet('99999999999999990.00');
    const providerId = `prov-ovf-win-${newId()}`;

    const overflowWin = await submitWagerOn(0, {
      providerId,
      externalTransactionId: 'win-overflow',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-ovf-win',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '10.00', currency: 'BRL' },
    });

    expect(overflowWin.status).toBe(422);
    expect(overflowWin.body).toEqual({
      transactionId: expect.any(String),
      status: 'REJECTED',
      failureCode: FailureCode.AmountOutOfRange,
      balance: { amount: '99999999999999990.00', currency: 'BRL' },
      idempotentReplay: false,
    });

    const [txRow] = await db`
      select status, failure_code, observed_balance_amount::text as observed, processed_at
      from wager_transactions
      where id = ${overflowWin.body.transactionId}
    `;
    expect(txRow).toEqual({
      status: 'REJECTED',
      failure_code: 'AMOUNT_OUT_OF_RANGE',
      observed: '99999999999999990.00',
      processed_at: null,
    });

    const events = await outboxEventsForTransaction(overflowWin.body.transactionId);
    expect(events.map((e) => e.event_type)).toEqual(['WagerTransactionRejected']);
    expect(events[0]!.payload.data).toMatchObject({
      transactionId: overflowWin.body.transactionId,
      failureCode: 'AMOUNT_OUT_OF_RANGE',
      observedBalance: { amount: '99999999999999990.00', currency: 'BRL' },
    });

    const [state] = await db`
      select w.balance_amount::text as balance, w.version,
             (select count(*)::int from wallet_ledger_entries e where e.wallet_id = w.id) as entries
      from wallets w where w.id = ${wallet.id}
    `;
    expect(state).toEqual({ balance: '99999999999999990.00', version: 1, entries: 1 });

    const reconciliation = await callInstance(1, 'POST', `/wallets/${wallet.id}/reconciliation`);
    expect(reconciliation.status).toBe(200);
    expect(reconciliation.body).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '99999999999999990.00', currency: 'BRL' },
      calculatedBalance: { amount: '99999999999999990.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 1,
    });
  });

  test('rejects REFUND that would overflow numeric(19,2) with 422 AMOUNT_OUT_OF_RANGE, preserving wallet and ledger', async () => {
    const wallet = await openWallet('99999999999999950.00');
    const providerId = `prov-ovf-refund-${newId()}`;

    // 1. BET 50.00 -> 99999999999999900.00 (version 2)
    const bet = await submitWagerOn(0, {
      providerId,
      externalTransactionId: 'bet-before-max',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-ovf-refund',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '50.00', currency: 'BRL' },
    });
    expect(bet.status).toBe(200);

    // 2. WIN 99.99 -> 99999999999999999.99 (exact numeric(19,2) ceiling, version 3)
    const winToMax = await submitWagerOn(1, {
      providerId,
      externalTransactionId: 'win-to-ceiling',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-ovf-refund',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '99.99', currency: 'BRL' },
    });
    expect(winToMax.status).toBe(200);
    expect(winToMax.body.balance).toEqual({ amount: '99999999999999999.99', currency: 'BRL' });

    // 3. REFUND 50.00 of bet-before-max -> overflows 99999999999999999.99 -> REJECTED (AMOUNT_OUT_OF_RANGE)
    const overflowRefund = await submitWagerOn(2, {
      providerId,
      externalTransactionId: 'refund-overflow',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-ovf-refund',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-before-max',
    });

    expect(overflowRefund.status).toBe(422);
    expect(overflowRefund.body).toEqual({
      transactionId: expect.any(String),
      status: 'REJECTED',
      failureCode: FailureCode.AmountOutOfRange,
      balance: { amount: '99999999999999999.99', currency: 'BRL' },
      idempotentReplay: false,
    });

    const [txRow] = await db`
      select status, failure_code, observed_balance_amount::text as observed, processed_at
      from wager_transactions
      where id = ${overflowRefund.body.transactionId}
    `;
    expect(txRow).toEqual({
      status: 'REJECTED',
      failure_code: 'AMOUNT_OUT_OF_RANGE',
      observed: '99999999999999999.99',
      processed_at: null,
    });

    const events = await outboxEventsForTransaction(overflowRefund.body.transactionId);
    expect(events.map((e) => e.event_type)).toEqual(['WagerTransactionRejected']);
    expect(events[0]!.payload.data.failureCode).toBe('AMOUNT_OUT_OF_RANGE');

    const [state] = await db`
      select w.balance_amount::text as balance, w.version,
             (select count(*)::int from wallet_ledger_entries e where e.wallet_id = w.id) as entries
      from wallets w where w.id = ${wallet.id}
    `;
    expect(state).toEqual({ balance: '99999999999999999.99', version: 3, entries: 3 });

    const reconciliation = await callInstance(0, 'POST', `/wallets/${wallet.id}/reconciliation`);
    expect(reconciliation.status).toBe(200);
    expect(reconciliation.body).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '99999999999999999.99', currency: 'BRL' },
      calculatedBalance: { amount: '99999999999999999.99', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 3,
    });
  });

  test('rejects ROLLBACK of BET that would overflow numeric(19,2) with 422 AMOUNT_OUT_OF_RANGE, preserving wallet and ledger', async () => {
    const wallet = await openWallet('99999999999999950.00');
    const providerId = `prov-ovf-rb-${newId()}`;

    // 1. BET 50.00 -> 99999999999999900.00 (version 2)
    const bet = await submitWagerOn(1, {
      providerId,
      externalTransactionId: 'bet-for-rb-ovf',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-ovf-rb',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '50.00', currency: 'BRL' },
    });
    expect(bet.status).toBe(200);

    // 2. WIN 80.00 -> 99999999999999980.00 (version 3)
    const win = await submitWagerOn(2, {
      providerId,
      externalTransactionId: 'win-near-max',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-ovf-rb',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '80.00', currency: 'BRL' },
    });
    expect(win.status).toBe(200);
    expect(win.body.balance).toEqual({ amount: '99999999999999980.00', currency: 'BRL' });

    // 3. ROLLBACK 50.00 of bet-for-rb-ovf -> 99999999999999980.00 + 50.00 > max -> REJECTED (AMOUNT_OUT_OF_RANGE)
    const overflowRollback = await submitWagerOn(0, {
      providerId,
      externalTransactionId: 'rollback-overflow',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-ovf-rb',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-for-rb-ovf',
    });

    expect(overflowRollback.status).toBe(422);
    expect(overflowRollback.body).toEqual({
      transactionId: expect.any(String),
      status: 'REJECTED',
      failureCode: FailureCode.AmountOutOfRange,
      balance: { amount: '99999999999999980.00', currency: 'BRL' },
      idempotentReplay: false,
    });

    const [txRow] = await db`
      select status, failure_code, observed_balance_amount::text as observed, processed_at
      from wager_transactions
      where id = ${overflowRollback.body.transactionId}
    `;
    expect(txRow).toEqual({
      status: 'REJECTED',
      failure_code: 'AMOUNT_OUT_OF_RANGE',
      observed: '99999999999999980.00',
      processed_at: null,
    });

    const events = await outboxEventsForTransaction(overflowRollback.body.transactionId);
    expect(events.map((e) => e.event_type)).toEqual(['WagerTransactionRejected']);
    expect(events[0]!.payload.data.failureCode).toBe('AMOUNT_OUT_OF_RANGE');

    const [state] = await db`
      select w.balance_amount::text as balance, w.version,
             (select count(*)::int from wallet_ledger_entries e where e.wallet_id = w.id) as entries
      from wallets w where w.id = ${wallet.id}
    `;
    expect(state).toEqual({ balance: '99999999999999980.00', version: 3, entries: 3 });

    const reconciliation = await callInstance(2, 'POST', `/wallets/${wallet.id}/reconciliation`);
    expect(reconciliation.status).toBe(200);
    expect(reconciliation.body).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '99999999999999980.00', currency: 'BRL' },
      calculatedBalance: { amount: '99999999999999980.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 3,
    });
  });
});

describe('Scenario C — Post-concurrency global financial, ledger, and Outbox integrity', () => {
  test('mixed concurrent workload on hot wallet + parallel independent wallets preserves per-wallet isolation', async () => {
    const hotWallet = await openWallet('120.00');
    const parallelWallet = await openWallet('90.00');
    const providerId = `prov-c-${newId()}`;

    // Lock ONLY hotWallet and verify parallelWallet is NOT blocked (proves lock is per-wallet, never global)
    const gateLocked = Promise.withResolvers<void>();
    const releaseGate = Promise.withResolvers<void>();
    const hotLockTask = gateSql.begin(async (tx) => {
      await tx`select id from wallets where id = ${hotWallet.id} for update`;
      gateLocked.resolve();
      await releaseGate.promise;
    });
    await gateLocked.promise;

    try {
      const parallelBet = await submitWagerOn(2, {
        providerId,
        externalTransactionId: 'parallel-wallet-bet',
        playerId: parallelWallet.playerId,
        walletId: parallelWallet.id,
        roundId: 'round-parallel',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '25.00', currency: 'BRL' },
      });
      expect(parallelBet.status).toBe(200);
      expect(parallelBet.body.balance).toEqual({ amount: '65.00', currency: 'BRL' });
    } finally {
      releaseGate.resolve();
      await hotLockTask;
    }

    // Mixed concurrent burst on hotWallet (120.00):
    // - 4 distinct BETs of 40.00 (up to 3 can debit if no WIN runs first, or all 4 if WIN 30.00 runs early!)
    // - 2 duplicate copies of bet-mix-0 (same key & payload)
    // - 1 WIN of 30.00
    // - 2 LOSSes (0.00 and 15.00 -> neither moves balance nor bumps version)
    const sharedBet0Payload = {
      providerId,
      externalTransactionId: 'bet-mix-0',
      playerId: hotWallet.playerId,
      walletId: hotWallet.id,
      roundId: 'round-mix-0',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '40.00', currency: 'BRL' },
    };
    const sharedBet0Key = `${providerId}:bet-mix-0`;

    const requests: Array<() => Promise<HttpResult>> = [
      () => submitWagerOn(0, sharedBet0Payload, sharedBet0Key),
      () => submitWagerOn(1, sharedBet0Payload, sharedBet0Key),
      () => submitWagerOn(2, sharedBet0Payload, sharedBet0Key),
      ...[1, 2, 3].map(
        (i) => () =>
          submitWagerOn(i, {
            providerId,
            externalTransactionId: `bet-mix-${i}`,
            playerId: hotWallet.playerId,
            walletId: hotWallet.id,
            roundId: `round-mix-${i}`,
            gameId: 'fortune-chimp',
            kind: 'BET',
            money: { amount: '40.00', currency: 'BRL' },
          }),
      ),
      () =>
        submitWagerOn(0, {
          providerId,
          externalTransactionId: 'win-mix-1',
          playerId: hotWallet.playerId,
          walletId: hotWallet.id,
          roundId: 'round-mix-win',
          gameId: 'fortune-chimp',
          kind: 'WIN',
          money: { amount: '30.00', currency: 'BRL' },
        }),
      () =>
        submitWagerOn(1, {
          providerId,
          externalTransactionId: 'loss-mix-zero',
          playerId: hotWallet.playerId,
          walletId: hotWallet.id,
          roundId: 'round-mix-loss-1',
          gameId: 'fortune-chimp',
          kind: 'LOSS',
          money: { amount: '0.00', currency: 'BRL' },
        }),
      () =>
        submitWagerOn(2, {
          providerId,
          externalTransactionId: 'loss-mix-pos',
          playerId: hotWallet.playerId,
          walletId: hotWallet.id,
          roundId: 'round-mix-loss-2',
          gameId: 'fortune-chimp',
          kind: 'LOSS',
          money: { amount: '15.00', currency: 'BRL' },
        }),
    ];

    const { result: mixResults, waitingPids } = await withWalletLockGate(hotWallet.id, requests.length, () =>
      Promise.all(requests.map((fn) => fn())),
    );
    expect(new Set(waitingPids).size).toBe(requests.length);
    expect(mixResults.every((r) => r.status === 200 || r.status === 422)).toBe(true);

    // Exactly 2 of the 3 sharedBet0 copies must be idempotent replays
    const bet0Responses = mixResults.slice(0, 3);
    expect(bet0Responses.filter((r) => r.body.idempotentReplay === false)).toHaveLength(1);
    expect(bet0Responses.filter((r) => r.body.idempotentReplay === true)).toHaveLength(2);
  });

  test('verifies directly in PostgreSQL and via reconciliation that every wallet, transaction, ledger chain, and Outbox event across all scenarios is 100% atomic and consistent', async () => {
    const wallets = await db`
      select id, player_id, currency, balance_amount::text as balance, version
      from wallets
      order by created_at asc
    `;
    expect(wallets.length).toBe(trackedWalletIds.size);
    expect(wallets.length).toBeGreaterThanOrEqual(12);

    // 1. Zero negative balances in wallets, ledger, or observed_balance_amount
    const [negatives] = await db`
      select
        (select count(*)::int from wallets where balance_amount < 0) as neg_wallets,
        (select count(*)::int from wallet_ledger_entries where balance_before < 0 or balance_after < 0) as neg_ledger,
        (select count(*)::int from wager_transactions where observed_balance_amount < 0) as neg_observed
    `;
    expect(negatives).toEqual({ neg_wallets: 0, neg_ledger: 0, neg_observed: 0 });

    // 2. Zero duplicates across idempotency_key, (provider_id, external_transaction_id), active reversals, ledger tx, and outbox
    const [duplicates] = await db`
      select
        (select count(*)::int from (
          select idempotency_key from wager_transactions group by idempotency_key having count(*) > 1
        ) d1) as dup_idempotency_keys,
        (select count(*)::int from (
          select provider_id, external_transaction_id from wager_transactions
          group by provider_id, external_transaction_id having count(*) > 1
        ) d2) as dup_external_ids,
        (select count(*)::int from (
          select provider_id, reference_external_transaction_id from wager_transactions
          where kind in ('REFUND', 'ROLLBACK') and status in ('PENDING_REFERENCE', 'PROCESSED')
          group by provider_id, reference_external_transaction_id having count(*) > 1
        ) d3) as dup_active_reversals,
        (select count(*)::int from (
          select transaction_id from wallet_ledger_entries group by transaction_id having count(*) > 1
        ) d4) as dup_ledger_tx,
        (select count(*)::int from (
          select payload -> 'data' ->> 'transactionId' as tx_id, event_type
          from outbox_messages
          where payload -> 'data' ->> 'transactionId' is not null
          group by tx_id, event_type having count(*) > 1
        ) d5) as dup_outbox_per_tx_event
    `;
    expect(duplicates).toEqual({
      dup_idempotency_keys: 0,
      dup_external_ids: 0,
      dup_active_reversals: 0,
      dup_ledger_tx: 0,
      dup_outbox_per_tx_event: 0,
    });

    // 3. Atomic 1-to-1 / 1-to-2 invariants between every wager_transaction, wallet_ledger_entries, and outbox_messages
    const allTransactions = await db`
      select t.id, t.wallet_id, t.kind, t.status, t.failure_code,
             t.amount::text as amount, t.observed_balance_amount::text as observed,
             (select count(*)::int from wallet_ledger_entries e where e.transaction_id = t.id) as ledger_count,
             (select array_agg(o.event_type order by o.sequence_number asc)
              from outbox_messages o
              where o.payload -> 'data' ->> 'transactionId' = t.id::text) as event_types
      from wager_transactions t
    `;
    expect(allTransactions.length).toBeGreaterThan(20);

    for (const tx of allTransactions as Json[]) {
      if (tx.status === 'PROCESSED' && tx.kind !== 'LOSS') {
        expect(tx.ledger_count).toBe(1);
        expect(tx.event_types).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
        expect(tx.observed).not.toBeNull();
        expect(tx.failure_code).toBeNull();
      } else if (tx.status === 'PROCESSED' && tx.kind === 'LOSS') {
        expect(tx.ledger_count).toBe(0);
        expect(tx.event_types).toEqual(['WagerTransactionProcessed']);
        expect(tx.observed).not.toBeNull();
        expect(tx.failure_code).toBeNull();
      } else if (tx.status === 'REJECTED') {
        expect(tx.ledger_count).toBe(0);
        expect(tx.event_types).toEqual(['WagerTransactionRejected']);
        expect(tx.observed).not.toBeNull();
        expect(tx.failure_code).not.toBeNull();
      } else if (tx.status === 'PENDING_REFERENCE') {
        expect(tx.ledger_count).toBe(0);
        expect(tx.event_types).toEqual(['WagerTransactionPendingReference']);
        expect(tx.observed).toBeNull();
        expect(tx.failure_code).toBeNull();
      } else {
        throw new Error(`Unexpected transaction status ${String(tx.status)} on ${String(tx.id)}`);
      }
    }

    // 4. Every outbox message belongs to a real wallet and a real wager_transaction (zero orphans from rolled-back attempts)
    const [{ orphanOutbox }] = await db`
      select count(*)::int as "orphanOutbox"
      from outbox_messages o
      where not exists (select 1 from wallets w where w.id = o.aggregate_id)
         or not exists (
           select 1 from wager_transactions t
           where t.id::text = o.payload -> 'data' ->> 'transactionId'
         )
    `;
    expect(orphanOutbox).toBe(0);

    // 5. Per-wallet SQL ledger chain verification + HTTP POST /wallets/:walletId/reconciliation
    for (const wallet of wallets as Json[]) {
      const entries = await db`
        select direction, amount::text as amount,
               balance_before::text as before, balance_after::text as after, wallet_version
        from wallet_ledger_entries
        where wallet_id = ${wallet.id}
        order by wallet_version asc
      `;
      const [netRow] = await db`
        select coalesce(sum(case direction when 'CREDIT' then amount else -amount end), 0)::numeric(19,2)::text as net
        from wallet_ledger_entries
        where wallet_id = ${wallet.id}
      `;
      expect(netRow!.net).toBe(wallet.balance);

      if (entries.length > 0) {
        expect(entries.at(-1)!.wallet_version).toBe(wallet.version);
        expect(entries.at(-1)!.after).toBe(wallet.balance);
        for (let i = 1; i < entries.length; i++) {
          expect(entries[i]!.wallet_version).toBe(entries[i - 1]!.wallet_version + 1);
          expect(entries[i]!.before).toBe(entries[i - 1]!.after);
        }
      }

      const reconciliation = await callInstance(0, 'POST', `/wallets/${wallet.id}/reconciliation`);
      expect(reconciliation.status).toBe(200);
      expect(reconciliation.body).toEqual({
        walletId: wallet.id,
        storedBalance: { amount: wallet.balance, currency: wallet.currency },
        calculatedBalance: { amount: wallet.balance, currency: wallet.currency },
        difference: { amount: '0.00', currency: wallet.currency },
        consistent: true,
        checkedEntries: entries.length,
      });
    }
  });
});
