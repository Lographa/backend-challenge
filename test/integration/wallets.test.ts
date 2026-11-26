import 'reflect-metadata';
import { LockMode } from '@mikro-orm/core';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { canonicalHash } from '../../src/common/hashing/canonical-hash';
import { isUuid, newId } from '../../src/common/ids';
import { Money } from '../../src/common/money/money';
import { WagerTransactionRecord, WalletLedgerEntryRecord, WalletRecord } from '../../src/database/entities';
import { WagerTransaction, WagerTransactionKind } from '../../src/wagering/wager-transaction';
import { Wallet } from '../../src/wallets/wallet';
import { createTestOrm, createTestSql, resetSchema, testDatabaseUrl } from '../support/database';

type Movement = [credit: boolean, amount: string];
type Json = Record<string, any>;

let app: INestApplication;
let baseUrl: string;
let orm: MikroORM;
let db: SQL;

beforeAll(async () => {
  const migrator = await createTestOrm('migrator');
  try {
    await resetSchema(migrator);
  } finally {
    await migrator.close();
  }
  const config = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app') });
  app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
  await app.listen(0);
  baseUrl = await app.getUrl();
  orm = await createTestOrm('app');
  db = createTestSql('app');
});

afterAll(async () => {
  await app?.close();
  await orm?.close();
  await db?.close();
});

async function call(
  method: 'GET' | 'POST',
  path: string,
  body?: unknown,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: Json }> {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as Json };
}

async function openWallet(amount: string, playerId = newId(), currency = 'BRL'): Promise<Json> {
  const response = await call('POST', '/wallets', { playerId, initialBalance: { amount, currency } });
  expect(response.status).toBe(201);
  return response.body;
}

/**
 * Stands in for the wager processor (increment 3.3): each movement is one SQL transaction that
 * locks the wallet, moves the balance and appends its ledger entry, as the application user.
 */
async function applyMovements(walletId: string, movements: readonly Movement[]): Promise<void> {
  for (const [credit, amount] of movements) {
    await orm.em.fork().transactional(async (em) => {
      const record = await em.findOneOrFail(WalletRecord, walletId, { lockMode: LockMode.PESSIMISTIC_WRITE });
      const wallet = Wallet.rehydrate(record);
      const money = Money.from({ amount, currency: wallet.currency });
      const now = new Date();
      const id = newId();
      const transaction = WagerTransaction.create({
        id,
        providerId: 'provider-test',
        externalTransactionId: `ext-${id}`,
        idempotencyKey: `idem-${id}`,
        payloadHash: canonicalHash({ id }),
        walletId,
        playerId: wallet.playerId,
        roundId: 'round-1',
        gameId: 'game-1',
        kind: credit ? WagerTransactionKind.Win : WagerTransactionKind.Bet,
        money,
        now,
      });
      const entry = credit ? wallet.credit(money, id, now) : wallet.debit(money, id, now);
      transaction.markProcessed(undefined, wallet.balance, now);
      em.assign(record, wallet.toState());
      em.create(WagerTransactionRecord, transaction.toState());
      em.create(WalletLedgerEntryRecord, entry.toState());
    });
  }
}

/** Every page of the ledger, following nextCursor until it is null. */
async function readWholeLedger(walletId: string, limit: number): Promise<{ pages: Json[][]; items: Json[] }> {
  const pages: Json[][] = [];
  let cursor: string | null = null;
  do {
    const query = new URLSearchParams({ limit: String(limit), ...(cursor === null ? {} : { cursor }) });
    const response = await call('GET', `/wallets/${walletId}/ledger?${query}`);
    expect(response.status).toBe(200);
    pages.push(response.body.items);
    cursor = response.body.nextCursor;
  } while (cursor !== null);
  return { pages, items: pages.flat() };
}

const versions = (items: Json[]) => items.map((item) => item.walletVersion);
const range = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, i) => from + i);

/** 59 movements over a 10.00 opening: 60 entries, final balance 10 + 30 × 1.10 - 29 × 0.35 = 32.85. */
const BUSY_MOVEMENTS: Movement[] = Array.from({ length: 59 }, (_, i) => (i % 2 === 0 ? [true, '1.10'] : [false, '0.35']));

describe('POST /wallets', () => {
  test('a positive balance opens the wallet with its OPENING credit, ledger entry and two ordered events', async () => {
    const playerId = newId();
    const response = await call(
      'POST',
      '/wallets',
      { playerId, initialBalance: { amount: '100.00', currency: 'BRL' } },
      { 'X-Correlation-Id': 'corr-open-1' },
    );

    expect(response.status).toBe(201);
    const walletId = response.body.id;
    expect(isUuid(walletId)).toBe(true);
    expect(response.body).toEqual({ id: walletId, playerId, balance: { amount: '100.00', currency: 'BRL' }, version: 1 });

    const wallets = await db`
      select player_id, currency, balance_amount::text as balance, version from wallets where id = ${walletId}`;
    expect(wallets).toEqual([{ player_id: playerId, currency: 'BRL', balance: '100.00', version: 1 }]);

    const transactions = await db`
      select id, provider_id, kind, status, amount::text as amount, observed_balance_amount::text as observed
      from wager_transactions where wallet_id = ${walletId}`;
    expect(transactions).toEqual([
      {
        id: expect.any(String),
        provider_id: 'internal',
        kind: 'OPENING',
        status: 'PROCESSED',
        amount: '100.00',
        observed: '100.00',
      },
    ]);
    const openingId = transactions[0].id;

    const entries = await db`
      select transaction_id, direction, amount::text as amount, balance_before::text as before,
             balance_after::text as after, wallet_version
      from wallet_ledger_entries where wallet_id = ${walletId}`;
    expect(entries).toEqual([
      { transaction_id: openingId, direction: 'CREDIT', amount: '100.00', before: '0.00', after: '100.00', wallet_version: 1 },
    ]);

    const events = await db`
      select event_type, payload from outbox_messages where aggregate_id = ${walletId} order by sequence_number`;
    expect(events.map((event: Json) => event.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
    const [processed, balanceChanged] = events.map((event: Json) => event.payload);
    expect(processed).toMatchObject({
      aggregateId: walletId,
      correlationId: 'corr-open-1',
      version: 1,
      data: { transactionId: openingId, kind: 'OPENING', observedBalance: { amount: '100.00', currency: 'BRL' } },
    });
    expect(balanceChanged).toMatchObject({
      aggregateId: walletId,
      correlationId: 'corr-open-1',
      data: {
        walletId,
        transactionId: openingId,
        direction: 'CREDIT',
        money: { amount: '100.00', currency: 'BRL' },
        balanceBefore: { amount: '0.00', currency: 'BRL' },
        balanceAfter: { amount: '100.00', currency: 'BRL' },
        walletVersion: 1,
      },
    });
  });

  test('a zero balance opens only the wallet, at version 1, with no ledger entry nor event', async () => {
    const wallet = await openWallet('0.00');
    expect(wallet).toMatchObject({ balance: { amount: '0.00', currency: 'BRL' }, version: 1 });

    const [counts] = await db`
      select (select count(*)::int from wager_transactions where wallet_id = ${wallet.id}) as transactions,
             (select count(*)::int from wallet_ledger_entries where wallet_id = ${wallet.id}) as entries,
             (select count(*)::int from outbox_messages where aggregate_id = ${wallet.id}) as events`;
    expect(counts).toEqual({ transactions: 0, entries: 0, events: 0 });
  });

  test('a short amount is normalized to two decimals', async () => {
    expect((await openWallet('7')).balance).toEqual({ amount: '7.00', currency: 'BRL' });
  });

  test.each<[string, (playerId: string) => unknown, string]>([
    ['no body fields', () => ({}), 'INVALID_PAYLOAD'],
    ['a JSON array', () => [], 'INVALID_PAYLOAD'],
    ['a playerId that is not a UUID', () => ({ playerId: 'player-1', initialBalance: { amount: '1.00', currency: 'BRL' } }), 'INVALID_PAYLOAD'],
    ['no initialBalance', (playerId) => ({ playerId }), 'INVALID_PAYLOAD'],
    ['initialBalance as a string', (playerId) => ({ playerId, initialBalance: '10.00' }), 'INVALID_PAYLOAD'],
    ['the amount as a JSON number', (playerId) => ({ playerId, initialBalance: { amount: 10, currency: 'BRL' } }), 'INVALID_AMOUNT'],
    ['three decimals', (playerId) => ({ playerId, initialBalance: { amount: '1.234', currency: 'BRL' } }), 'INVALID_AMOUNT'],
    ['a negative amount', (playerId) => ({ playerId, initialBalance: { amount: '-5.00', currency: 'BRL' } }), 'INVALID_AMOUNT'],
    ['an exponent', (playerId) => ({ playerId, initialBalance: { amount: '1e3', currency: 'BRL' } }), 'INVALID_AMOUNT'],
    ['a lowercase currency', (playerId) => ({ playerId, initialBalance: { amount: '1.00', currency: 'brl' } }), 'INVALID_CURRENCY'],
  ])('%s is a 400 and persists nothing', async (_case, body, code) => {
    const playerId = newId();
    const response = await call('POST', '/wallets', body(playerId));

    expect(response.status).toBe(400);
    expect(response.body).toMatchObject({ code, correlationId: expect.any(String) });
    const [{ wallets }] = await db`select count(*)::int as wallets from wallets where player_id = ${playerId}`;
    expect(wallets).toBe(0);
  });

  test('a second wallet for the same player and currency is a 409; another currency is a new wallet', async () => {
    const playerId = newId();
    const first = await openWallet('10.00', playerId);

    const duplicate = await call('POST', '/wallets', { playerId, initialBalance: { amount: '5.00', currency: 'BRL' } });
    expect(duplicate.status).toBe(409);
    expect(duplicate.body).toMatchObject({ code: 'WALLET_ALREADY_EXISTS' });

    const usd = await openWallet('5.00', playerId, 'USD');
    expect(usd.id).not.toBe(first.id);
    expect(usd.balance).toEqual({ amount: '5.00', currency: 'USD' });

    const rows = await db`
      select currency, balance_amount::text as balance from wallets where player_id = ${playerId} order by currency`;
    expect(rows).toEqual([
      { currency: 'BRL', balance: '10.00' },
      { currency: 'USD', balance: '5.00' },
    ]);
  });

  test('concurrent openings for the same player and currency: one 201, the others 409, one wallet', async () => {
    const playerId = newId();
    const attempts = Array.from({ length: 10 }, (_, i) =>
      call('POST', '/wallets', { playerId, initialBalance: { amount: `${i + 1}.00`, currency: 'BRL' } }),
    );
    const responses = await Promise.all(attempts);

    const statuses = responses.map((response) => response.status).sort();
    expect(statuses).toEqual([201, ...Array<number>(9).fill(409)]);
    for (const response of responses.filter((r) => r.status === 409)) {
      expect(response.body).toMatchObject({ code: 'WALLET_ALREADY_EXISTS' });
    }
    const winner = responses.find((response) => response.status === 201)!.body;

    const [state] = await db`
      select (select count(*)::int from wallets where player_id = ${playerId}) as wallets,
             (select count(*)::int from wager_transactions where player_id = ${playerId}) as transactions,
             (select count(*)::int from wallet_ledger_entries where wallet_id = ${winner.id}) as entries,
             (select count(*)::int from outbox_messages where aggregate_id = ${winner.id}) as events,
             (select balance_amount::text from wallets where player_id = ${playerId}) as balance`;
    expect(state).toEqual({ wallets: 1, transactions: 1, entries: 1, events: 2, balance: winner.balance.amount });

    // The losers' rolled back transactions left nothing behind.
    const [{ orphans }] = await db`
      select count(*)::int as orphans from outbox_messages o
      where not exists (select 1 from wallets w where w.id = o.aggregate_id)`;
    expect(orphans).toBe(0);
  });
});

describe('GET /wallets/:walletId', () => {
  test('returns the stored balance and version', async () => {
    const opened = await openWallet('20.00');
    expect((await call('GET', `/wallets/${opened.id}`)).body).toEqual(opened);

    await applyMovements(opened.id, [
      [false, '5.25'],
      [true, '0.75'],
    ]);
    const response = await call('GET', `/wallets/${opened.id}`);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ ...opened, balance: { amount: '15.50', currency: 'BRL' }, version: 3 });
  });

  test('an unknown wallet is a 404 and a malformed id a 400', async () => {
    expect((await call('GET', `/wallets/${newId()}`)).body).toMatchObject({ code: 'WALLET_NOT_FOUND' });
    expect((await call('GET', `/wallets/${newId()}`)).status).toBe(404);
    const malformed = await call('GET', '/wallets/not-a-uuid');
    expect(malformed.status).toBe(400);
    expect(malformed.body).toMatchObject({ code: 'INVALID_PAYLOAD' });
  });
});

describe('GET /wallets/:walletId/ledger', () => {
  let busy: Json;

  beforeAll(async () => {
    busy = await openWallet('10.00');
    await applyMovements(busy.id, BUSY_MOVEMENTS);
  });

  test('pages through every entry in version order, without gaps or repeats', async () => {
    const { pages, items } = await readWholeLedger(busy.id, 7);

    expect(pages.map((page) => page.length)).toEqual([7, 7, 7, 7, 7, 7, 7, 7, 4]);
    expect(versions(items)).toEqual(range(1, 60));
    expect(new Set(items.map((item) => item.id)).size).toBe(60);
    for (let i = 1; i < items.length; i++) {
      expect(items[i]!.balanceBefore).toEqual(items[i - 1]!.balanceAfter);
    }
    expect(items.at(-1)!.balanceAfter).toEqual({ amount: '32.85', currency: 'BRL' });

    const stored = await db`select id from wallet_ledger_entries where wallet_id = ${busy.id} order by wallet_version`;
    expect(items.map((item) => item.id)).toEqual(stored.map((row: Json) => row.id));
  });

  test('each item carries the entry; the default page is 50', async () => {
    const first = await call('GET', `/wallets/${busy.id}/ledger`);
    expect(first.status).toBe(200);
    expect(first.body.items).toHaveLength(50);
    expect(first.body.items[0]).toEqual({
      id: expect.any(String),
      transactionId: expect.any(String),
      direction: 'CREDIT',
      money: { amount: '10.00', currency: 'BRL' },
      balanceBefore: { amount: '0.00', currency: 'BRL' },
      balanceAfter: { amount: '10.00', currency: 'BRL' },
      walletVersion: 1,
      createdAt: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/),
    });
    expect(typeof first.body.nextCursor).toBe('string');

    const rest = await call('GET', `/wallets/${busy.id}/ledger?cursor=${first.body.nextCursor}`);
    expect(versions(rest.body.items)).toEqual(range(51, 60));
    expect(rest.body.nextCursor).toBeNull();
  });

  test('entries appended between pages show up on a later page', async () => {
    const wallet = await openWallet('10.00');
    await applyMovements(wallet.id, [
      [true, '1.00'],
      [true, '1.00'],
      [true, '1.00'],
    ]);
    const first = await call('GET', `/wallets/${wallet.id}/ledger?limit=2`);
    expect(versions(first.body.items)).toEqual([1, 2]);

    await applyMovements(wallet.id, [
      [false, '2.00'],
      [true, '0.50'],
    ]);
    const seen = [...first.body.items];
    let cursor: string | null = first.body.nextCursor;
    while (cursor !== null) {
      const page = await call('GET', `/wallets/${wallet.id}/ledger?limit=2&cursor=${cursor}`);
      seen.push(...page.body.items);
      cursor = page.body.nextCursor;
    }
    expect(versions(seen)).toEqual(range(1, 6));
  });

  test('a wallet opened with zero balance starts empty; its first movement is version 2', async () => {
    const wallet = await openWallet('0.00');
    expect((await call('GET', `/wallets/${wallet.id}/ledger`)).body).toEqual({ items: [], nextCursor: null });

    await applyMovements(wallet.id, [[true, '3.00']]);
    const { items } = await readWholeLedger(wallet.id, 50);
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({ walletVersion: 2, balanceBefore: { amount: '0.00', currency: 'BRL' } });
  });

  test.each([
    ['limit=0', 400, 'INVALID_PAYLOAD'],
    ['limit=201', 400, 'INVALID_PAYLOAD'],
    ['limit=ten', 400, 'INVALID_PAYLOAD'],
    ['limit=1&limit=2', 400, 'INVALID_PAYLOAD'],
    ['cursor=not-a-cursor', 400, 'INVALID_PAYLOAD'],
    [`cursor=${Buffer.from('{"v":0}').toString('base64url')}`, 400, 'INVALID_PAYLOAD'],
  ])('%s is a %d %s', async (query, status, code) => {
    const response = await call('GET', `/wallets/${busy.id}/ledger?${query}`);
    expect(response.status).toBe(status);
    expect(response.body).toMatchObject({ code });
  });

  test('an unknown wallet is a 404 and a malformed id a 400', async () => {
    expect((await call('GET', `/wallets/${newId()}/ledger`)).status).toBe(404);
    expect((await call('GET', '/wallets/nope/ledger')).status).toBe(400);
  });
});

describe('POST /wallets/:walletId/reconciliation', () => {
  test('a wallet with dozens of entries matches its ledger, and checking writes nothing', async () => {
    const wallet = await openWallet('10.00');
    await applyMovements(wallet.id, BUSY_MOVEMENTS);
    const countRows = () => db`
      select (select count(*)::int from wallets) as wallets,
             (select count(*)::int from wager_transactions) as transactions,
             (select count(*)::int from wallet_ledger_entries) as entries,
             (select count(*)::int from outbox_messages) as events`;
    const before = await countRows();

    const response = await call('POST', `/wallets/${wallet.id}/reconciliation`);

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '32.85', currency: 'BRL' },
      calculatedBalance: { amount: '32.85', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 60,
    });
    expect(await countRows()).toEqual(before);
  });

  test('a wallet opened with zero balance has nothing to check', async () => {
    const wallet = await openWallet('0.00');
    expect((await call('POST', `/wallets/${wallet.id}/reconciliation`)).body).toMatchObject({
      calculatedBalance: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 0,
    });
  });

  test('reconciliations running alongside new movements read wallet and ledger from one snapshot', async () => {
    const wallet = await openWallet('50.00');
    const movements: Movement[] = Array.from({ length: 30 }, (_, i) => [i % 3 !== 0, '0.40']);
    const reports: Json[] = [];
    let moving = true;

    const writer = applyMovements(wallet.id, movements).finally(() => {
      moving = false;
    });
    const reader = (async () => {
      while (moving) {
        reports.push((await call('POST', `/wallets/${wallet.id}/reconciliation`)).body);
      }
    })();
    await Promise.all([writer, reader]);

    expect(reports.length).toBeGreaterThan(0);
    expect(reports.filter((report) => report.consistent !== true)).toEqual([]);
    const final = await call('POST', `/wallets/${wallet.id}/reconciliation`);
    expect(final.body).toMatchObject({ consistent: true, checkedEntries: 31 });
  });

  test('an unknown wallet is a 404 and a malformed id a 400', async () => {
    expect((await call('POST', `/wallets/${newId()}/reconciliation`)).body).toMatchObject({ code: 'WALLET_NOT_FOUND' });
    expect((await call('POST', '/wallets/nope/reconciliation')).status).toBe(400);
  });

  test('every wallet opened in this suite reconciles: balance equals the ledger', async () => {
    const wallets = await db`select id from wallets`;
    expect(wallets.length).toBeGreaterThan(5);
    for (const { id } of wallets as Json[]) {
      const response = await call('POST', `/wallets/${id}/reconciliation`);
      expect(response.body).toMatchObject({ walletId: id, consistent: true });
    }
  });
});
