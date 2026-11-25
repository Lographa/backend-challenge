import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { newId } from '../../src/common/ids';
import { APPLICATION_ROLE } from '../../src/database/provision';
import {
  capturePostgresError,
  createTestOrm,
  createTestSql,
  expectPermissionDenied,
  resetSchema,
  testRoleName,
} from '../support/database';

const FINANCIAL_TABLES = ['inbox_messages', 'outbox_messages', 'wager_transactions', 'wallet_ledger_entries', 'wallets'];
const HASH = 'b'.repeat(64);

let app: SQL;
let migrator: SQL;
let appUser: string;
let migratorUser: string;

beforeAll(async () => {
  const orm = await createTestOrm('migrator');
  try {
    await resetSchema(orm);
  } finally {
    await orm.close();
  }
  app = createTestSql('app');
  migrator = createTestSql('migrator');
  appUser = testRoleName('app');
  migratorUser = testRoleName('migrator');
});

afterAll(async () => {
  await app.close();
  await migrator.close();
});

/** Opens a wallet with a 100.00 OPENING entry as the application user. */
async function openWalletAsApp(): Promise<{ walletId: string; playerId: string; openingId: string }> {
  const walletId = newId();
  const playerId = newId();
  const openingId = newId();
  const now = new Date();
  await app.begin(async (tx) => {
    await tx`insert into wallets ${tx({
      id: walletId,
      player_id: playerId,
      currency: 'BRL',
      balance_amount: '100.00',
      version: 1,
      created_at: now,
      updated_at: now,
    })}`;
    await tx`insert into wager_transactions ${tx({
      id: openingId,
      provider_id: 'internal',
      external_transaction_id: `opening:${walletId}`,
      idempotency_key: `opening:${walletId}`,
      payload_hash: HASH,
      wallet_id: walletId,
      player_id: playerId,
      currency: 'BRL',
      round_id: 'opening',
      game_id: 'wallet-opening',
      kind: 'OPENING',
      amount: '100.00',
      status: 'PROCESSED',
      observed_balance_amount: '100.00',
      attempt_count: 0,
      processed_at: now,
      created_at: now,
    })}`;
    await tx`insert into wallet_ledger_entries ${tx({
      id: newId(),
      wallet_id: walletId,
      transaction_id: openingId,
      direction: 'CREDIT',
      amount: '100.00',
      currency: 'BRL',
      balance_before: '0.00',
      balance_after: '100.00',
      wallet_version: 1,
      created_at: now,
    })}`;
  });
  return { walletId, playerId, openingId };
}

describe('role catalog', () => {
  test('the application user is a plain login role with no elevated attributes', async () => {
    const [role] = await migrator`
      select rolsuper, rolbypassrls, rolcreatedb, rolcreaterole, rolreplication, rolcanlogin
      from pg_roles where rolname = ${appUser}`;
    expect(role).toEqual({
      rolsuper: false,
      rolbypassrls: false,
      rolcreatedb: false,
      rolcreaterole: false,
      rolreplication: false,
      rolcanlogin: true,
    });
  });

  test('the application user only inherits the privilege group, which cannot log in', async () => {
    const memberships = await migrator`
      select g.rolname as group_name, g.rolcanlogin, g.rolsuper, g.rolbypassrls
      from pg_auth_members m
      join pg_roles g on g.oid = m.roleid
      join pg_roles u on u.oid = m.member
      where u.rolname = ${appUser}`;
    expect(memberships).toEqual([
      { group_name: APPLICATION_ROLE, rolcanlogin: false, rolsuper: false, rolbypassrls: false },
    ]);
  });

  test('the migration user is not a superuser and cannot bypass row security', async () => {
    const [role] = await migrator`select rolsuper, rolbypassrls from pg_roles where rolname = ${migratorUser}`;
    expect(role).toEqual({ rolsuper: false, rolbypassrls: false });
  });

  test('the migration user owns the schema, every table, sequence and function; the application owns nothing', async () => {
    const owners = await migrator`
      select distinct pg_get_userbyid(relowner) as owner from pg_class
      where relnamespace = 'public'::regnamespace
      union
      select distinct pg_get_userbyid(proowner) from pg_proc where pronamespace = 'public'::regnamespace
      union
      select pg_get_userbyid(nspowner) from pg_namespace where nspname = 'public'`;
    expect(owners).toEqual([{ owner: migratorUser }]);

    const [{ owned }] = await migrator`
      select (select count(*) from pg_class where relowner in (select oid from pg_roles where rolname in (${appUser}, ${APPLICATION_ROLE})))
           + (select count(*) from pg_proc where proowner in (select oid from pg_roles where rolname in (${appUser}, ${APPLICATION_ROLE})))
           + (select count(*) from pg_namespace where nspowner in (select oid from pg_roles where rolname in (${appUser}, ${APPLICATION_ROLE})))
           as owned`;
    expect(Number(owned)).toBe(0);
  });

  test('effective table privileges of the application user', async () => {
    const privileges = await app`
      select t.name,
             has_table_privilege(t.name, 'SELECT') as select,
             has_table_privilege(t.name, 'INSERT') as insert,
             has_table_privilege(t.name, 'UPDATE') as update,
             has_any_column_privilege(t.name, 'UPDATE') as update_some_columns,
             has_table_privilege(t.name, 'DELETE') as delete,
             has_table_privilege(t.name, 'TRUNCATE') as truncate,
             has_table_privilege(t.name, 'TRIGGER') as trigger,
             has_table_privilege(t.name, 'REFERENCES') as references
      from unnest(string_to_array(${FINANCIAL_TABLES.join(',')}, ',')) as t(name) order by t.name`;
    const row = (update: boolean) => ({
      select: true,
      insert: true,
      update: false,
      update_some_columns: update,
      delete: false,
      truncate: false,
      trigger: false,
      references: false,
    });
    expect(privileges).toEqual([
      { name: 'inbox_messages', ...row(true) },
      { name: 'outbox_messages', ...row(true) },
      { name: 'wager_transactions', ...row(true) },
      { name: 'wallet_ledger_entries', ...row(false) },
      { name: 'wallets', ...row(true) },
    ]);
  });

  test('the application user cannot create in the schema or database, nor use temporary tables', async () => {
    const [privileges] = await app`
      select has_schema_privilege('public', 'CREATE') as schema_create,
             has_schema_privilege('public', 'USAGE') as schema_usage,
             has_database_privilege(current_database(), 'CREATE') as database_create,
             has_database_privilege(current_database(), 'TEMP') as database_temp`;
    expect(privileges).toEqual({ schema_create: false, schema_usage: true, database_create: false, database_temp: false });
  });

  test('trigger functions are not executable by PUBLIC and pin their search_path', async () => {
    const functions = await migrator`
      select proname, has_function_privilege(${appUser}, oid, 'EXECUTE') as app_execute,
             prosecdef, array_to_string(proconfig, ',') as config
      from pg_proc where pronamespace = 'public'::regnamespace order by proname`;
    expect(functions.length).toBe(9);
    for (const fn of functions) {
      expect(fn).toMatchObject({ app_execute: false, prosecdef: false, config: 'search_path=public, pg_temp' });
    }
  });
});

describe('the application user can do its job', () => {
  test('opens a wallet, reads and appends to the ledger, and records inbox and outbox messages', async () => {
    const { walletId, playerId } = await openWalletAsApp();
    const betId = newId();
    const now = new Date();

    await app.begin(async (tx) => {
      const [wallet] = await tx`select balance_amount, version from wallets where id = ${walletId} for update`;
      expect(wallet).toEqual({ balance_amount: '100.00', version: 1 });
      await tx`insert into inbox_messages ${tx({
        consumer_name: 'wager-processor',
        message_id: newId(),
        payload_hash: HASH,
        received_at: now,
      })}`;
      await tx`insert into wager_transactions ${tx({
        id: betId,
        provider_id: 'provider-a',
        external_transaction_id: `bet-${betId}`,
        idempotency_key: `bet-${betId}`,
        payload_hash: HASH,
        wallet_id: walletId,
        player_id: playerId,
        currency: 'BRL',
        round_id: 'round-1',
        game_id: 'game-1',
        kind: 'BET',
        amount: '30.00',
        status: 'PROCESSED',
        observed_balance_amount: '70.00',
        attempt_count: 0,
        processed_at: now,
        created_at: now,
      })}`;
      await tx`insert into wallet_ledger_entries ${tx({
        id: newId(),
        wallet_id: walletId,
        transaction_id: betId,
        direction: 'DEBIT',
        amount: '30.00',
        currency: 'BRL',
        balance_before: '100.00',
        balance_after: '70.00',
        wallet_version: 2,
        created_at: now,
      })}`;
      await tx`update wallets set balance_amount = ${'70.00'}, version = 2, updated_at = ${now} where id = ${walletId}`;
      await tx`insert into outbox_messages ${tx({
        id: newId(),
        aggregate_id: betId,
        event_type: 'wager.processed',
        payload: { transactionId: betId },
        occurred_at: now,
        attempts: 0,
      })}`;
    });

    const ledger = await app`
      select wallet_version, direction, balance_after from wallet_ledger_entries
      where wallet_id = ${walletId} order by wallet_version`;
    expect(ledger).toEqual([
      { wallet_version: 1, direction: 'CREDIT', balance_after: '100.00' },
      { wallet_version: 2, direction: 'DEBIT', balance_after: '70.00' },
    ]);
  });

  test('marks outbox and inbox messages as processed', async () => {
    const outboxId = newId();
    const messageId = newId();
    const now = new Date();
    await app`insert into outbox_messages ${app({
      id: outboxId,
      aggregate_id: newId(),
      event_type: 'wager.processed',
      payload: { ok: true },
      occurred_at: now,
      attempts: 0,
    })}`;
    await app`insert into inbox_messages ${app({
      consumer_name: 'wager-processor',
      message_id: messageId,
      payload_hash: HASH,
      received_at: now,
    })}`;

    await app`update outbox_messages set attempts = 1, published_at = ${now} where id = ${outboxId}`;
    await app`update inbox_messages set processed_at = ${now} where message_id = ${messageId}`;

    const [outbox] = await app`select attempts from outbox_messages where id = ${outboxId}`;
    expect(outbox).toEqual({ attempts: 1 });
  });
});

describe('the application user is denied with SQLSTATE 42501', () => {
  test('UPDATE, DELETE and TRUNCATE on the ledger', async () => {
    const { walletId } = await openWalletAsApp();
    await expectPermissionDenied(app`update wallet_ledger_entries set amount = 1 where wallet_id = ${walletId}`);
    await expectPermissionDenied(app`delete from wallet_ledger_entries where wallet_id = ${walletId}`);
    await expectPermissionDenied(app`truncate wallet_ledger_entries`);
    await expectPermissionDenied(app`truncate wallet_ledger_entries cascade`);
  });

  test.each(FINANCIAL_TABLES.filter((table) => table !== 'wallet_ledger_entries').map((table) => [table]))(
    'DELETE and TRUNCATE on %s',
    async (table) => {
      await expectPermissionDenied(app.unsafe(`delete from ${table}`));
      await expectPermissionDenied(app.unsafe(`truncate ${table}`));
    },
  );

  test('UPDATE of columns outside the granted set', async () => {
    const { walletId, openingId } = await openWalletAsApp();
    await expectPermissionDenied(app`update wallets set currency = 'USD' where id = ${walletId}`);
    await expectPermissionDenied(app`update wallets set player_id = ${newId()} where id = ${walletId}`);
    await expectPermissionDenied(app`update wager_transactions set amount = 1 where id = ${openingId}`);
    await expectPermissionDenied(app`update wager_transactions set kind = 'WIN' where id = ${openingId}`);
    await expectPermissionDenied(app`update outbox_messages set payload = '{}'::jsonb`);
    await expectPermissionDenied(app`update inbox_messages set payload_hash = ${HASH}`);
  });

  test('disabling or dropping triggers', async () => {
    await expectPermissionDenied(app`alter table wallet_ledger_entries disable trigger all`);
    await expectPermissionDenied(app`alter table wallet_ledger_entries disable trigger wallet_ledger_entries_no_update_delete`);
    await expectPermissionDenied(app`alter table wallets disable trigger user`);
    await expectPermissionDenied(app`drop trigger wallet_ledger_entries_no_update_delete on wallet_ledger_entries`);
    await expectPermissionDenied(app`drop trigger wallet_ledger_entries_chain on wallet_ledger_entries`);
    await expectPermissionDenied(app`set session_replication_role = replica`);
  });

  test('replacing trigger functions', async () => {
    await expectPermissionDenied(
      app.unsafe(`create or replace function wallet_ledger_entries_append_only() returns trigger
                  language plpgsql as $$ begin return null; end $$`),
    );
    await expectPermissionDenied(app`drop function wallet_ledger_entries_append_only() cascade`);
  });

  test('changing the schema', async () => {
    await expectPermissionDenied(app`create table attacker (id int)`);
    await expectPermissionDenied(app`create temporary table attacker (id int)`);
    await expectPermissionDenied(app`alter table wallets add column note text`);
    await expectPermissionDenied(app`alter table wallets drop constraint wallets_balance_non_negative_ck`);
    await expectPermissionDenied(app`drop table wallet_ledger_entries`);
    await expectPermissionDenied(app`create index attacker_ix on wallets (player_id)`);
    await expectPermissionDenied(app`create function public.attacker() returns int language sql as 'select 1'`);
    await expectPermissionDenied(app`alter schema public owner to ${app(appUser)}`);
  });

  test('reading or editing the migration history', async () => {
    await expectPermissionDenied(app`select * from mikro_orm_migrations`);
    await expectPermissionDenied(app`delete from mikro_orm_migrations`);
  });

  test('a rejected statement leaves no partial effect behind', async () => {
    const { walletId } = await openWalletAsApp();
    await expectPermissionDenied(
      app.begin(async (tx) => {
        await tx`select version from wallets where id = ${walletId} for update`;
        await tx`delete from wallet_ledger_entries where wallet_id = ${walletId}`;
      }),
    );
    const [{ entries }] = await app`select count(*)::int as entries from wallet_ledger_entries where wallet_id = ${walletId}`;
    expect(entries).toBe(1);
  });
});

describe('the migration user, as owner, is still bound by the ledger rules', () => {
  test('cannot use session_replication_role to skip triggers', async () => {
    await expectPermissionDenied(migrator`set session_replication_role = replica`);
  });

  test('UPDATE, DELETE and TRUNCATE on the ledger are rejected by the append-only triggers', async () => {
    const { walletId } = await openWalletAsApp();
    const update = await capturePostgresError(
      migrator`update wallet_ledger_entries set amount = 1 where wallet_id = ${walletId}`,
    );
    const remove = await capturePostgresError(migrator`delete from wallet_ledger_entries where wallet_id = ${walletId}`);
    const truncate = await capturePostgresError(migrator`truncate wallet_ledger_entries cascade`);
    for (const error of [update, remove, truncate]) {
      expect(error.message).toContain('wallet_ledger_entries is append-only');
    }
  });
});
