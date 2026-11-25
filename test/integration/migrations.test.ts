import type { MikroORM } from '@mikro-orm/postgresql';
import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { createTestOrm, createTestSql } from '../support/database';

const TABLES = ['inbox_messages', 'outbox_messages', 'wager_transactions', 'wallet_ledger_entries', 'wallets'];
const FUNCTIONS = [
  'inbox_messages_guard',
  'outbox_messages_guard',
  'wager_transactions_consistency',
  'wager_transactions_guard',
  'wallet_ledger_entries_append_only',
  'wallet_ledger_entries_chain',
  'wallet_ledger_entries_consistency',
  'wallets_balance_matches_ledger',
  'wallets_guard',
];

let orm: MikroORM;
let sql: SQL;

async function tables(): Promise<string[]> {
  const rows: Array<{ name: string }> = await sql`
    select table_name as name from information_schema.tables
    where table_schema = 'public' and table_name <> 'mikro_orm_migrations'
    order by table_name`;
  return rows.map((row) => row.name);
}

async function functions(): Promise<string[]> {
  const rows: Array<{ name: string }> = await sql`
    select proname as name from pg_proc
    where pronamespace = 'public'::regnamespace
    order by proname`;
  return rows.map((row) => row.name);
}

beforeAll(async () => {
  orm = await createTestOrm();
  sql = createTestSql();
  await orm.migrator.down({ to: 0 });
});

afterAll(async () => {
  await orm.migrator.up();
  await sql.close();
  await orm.close();
});

describe('initial schema migration', () => {
  test('down to zero leaves no tables or functions behind', async () => {
    expect(await tables()).toEqual([]);
    expect(await functions()).toEqual([]);
    expect((await orm.migrator.getPending()).map((m) => m.name)).toEqual(['Migration20261125140000InitialSchema']);
  });

  test('up creates the five tables and their functions', async () => {
    await orm.migrator.up();

    expect(await tables()).toEqual(TABLES);
    expect(await functions()).toEqual(FUNCTIONS);
    expect(await orm.migrator.getPending()).toEqual([]);
  });

  test('up is a no-op once applied', async () => {
    expect(await orm.migrator.up()).toEqual([]);
  });

  test('down and up can be repeated', async () => {
    for (let cycle = 0; cycle < 2; cycle++) {
      await orm.migrator.down();
      expect(await tables()).toEqual([]);
      expect(await functions()).toEqual([]);
      await orm.migrator.up();
      expect(await tables()).toEqual(TABLES);
    }
  });

  test('down also works with data in the tables, including the append-only ledger', async () => {
    await sql.begin(async (tx) => {
      const walletId = Bun.randomUUIDv7();
      const playerId = Bun.randomUUIDv7();
      const openingId = Bun.randomUUIDv7();
      const now = new Date();
      await tx`insert into wallets ${tx({
        id: walletId,
        player_id: playerId,
        currency: 'BRL',
        balance_amount: '10.00',
        version: 1,
        created_at: now,
        updated_at: now,
      })}`;
      await tx`insert into wager_transactions ${tx({
        id: openingId,
        provider_id: 'internal',
        external_transaction_id: `opening:${walletId}`,
        idempotency_key: `opening:${walletId}`,
        payload_hash: 'a'.repeat(64),
        wallet_id: walletId,
        player_id: playerId,
        round_id: 'opening',
        game_id: 'wallet-opening',
        kind: 'OPENING',
        amount: '10.00',
        currency: 'BRL',
        status: 'PROCESSED',
        observed_balance_amount: '10.00',
        attempt_count: 0,
        processed_at: now,
        created_at: now,
      })}`;
      await tx`insert into wallet_ledger_entries ${tx({
        id: Bun.randomUUIDv7(),
        wallet_id: walletId,
        transaction_id: openingId,
        direction: 'CREDIT',
        amount: '10.00',
        currency: 'BRL',
        balance_before: '0.00',
        balance_after: '10.00',
        wallet_version: 1,
        created_at: now,
      })}`;
    });

    await orm.migrator.down();
    expect(await tables()).toEqual([]);
    await orm.migrator.up();
    expect(await tables()).toEqual(TABLES);
  });
});
