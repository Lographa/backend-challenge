import type { MikroORM } from '@mikro-orm/postgresql';
import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { newId } from '../../src/common/ids';
import { Money } from '../../src/common/money/money';
import { createTestOrm, createTestSql, expectConstraintViolation, resetSchema } from '../support/database';

type Row = Record<string, unknown>;
type Direction = 'DEBIT' | 'CREDIT';

interface WalletFixture {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
}

interface Movement {
  transaction: Row;
  entry: Row;
  balanceAfter: string;
  version: number;
}

const HASH = 'a'.repeat(64);
const now = new Date('2026-01-01T00:00:00.000Z');
const later = new Date('2026-01-01T00:00:30.000Z');

let orm: MikroORM;
let sql: SQL;
let sequence = 0;

beforeAll(async () => {
  orm = await createTestOrm();
  await resetSchema(orm);
  sql = createTestSql();
});

afterAll(async () => {
  await sql.close();
  await orm.close();
});

// ------------------------------------------------------------------ fixtures

async function insert(tx: SQL, table: string, row: Row): Promise<void> {
  await tx`insert into ${tx(table)} ${tx(row)}`;
}

function walletRow(overrides: Row = {}): Row {
  return {
    id: newId(),
    player_id: newId(),
    currency: 'BRL',
    balance_amount: '0.00',
    version: 1,
    created_at: now,
    updated_at: now,
    ...overrides,
  };
}

function transactionRow(wallet: WalletFixture, overrides: Row = {}): Row {
  sequence += 1;
  return {
    id: newId(),
    provider_id: 'provider-a',
    external_transaction_id: `ext-${sequence}`,
    idempotency_key: `idem-${sequence}`,
    payload_hash: HASH,
    wallet_id: wallet.id,
    player_id: wallet.playerId,
    currency: wallet.currency,
    round_id: 'round-1',
    game_id: 'game-1',
    kind: 'BET',
    amount: '10.00',
    reference_external_transaction_id: null,
    reference_transaction_id: null,
    status: 'PROCESSED',
    failure_code: null,
    observed_balance_amount: '0.00',
    attempt_count: 0,
    next_attempt_at: null,
    processed_at: now,
    created_at: now,
    ...overrides,
  };
}

function inboxRow(overrides: Row = {}): Row {
  return {
    consumer_name: 'wager-processor',
    message_id: newId(),
    payload_hash: HASH,
    received_at: now,
    processed_at: null,
    ...overrides,
  };
}

function outboxRow(overrides: Row = {}): Row {
  return {
    id: newId(),
    aggregate_id: newId(),
    event_type: 'wager.processed',
    payload: { eventId: 'x' },
    occurred_at: now,
    attempts: 0,
    next_attempt_at: null,
    published_at: null,
    ...overrides,
  };
}

/** Rows for one balance movement that satisfy every constraint; tests corrupt one piece. */
function movement(wallet: WalletFixture, direction: Direction, overrides: Row = {}): Movement {
  const transaction = transactionRow(wallet, overrides);
  const amount = Money.from({ amount: String(transaction.amount), currency: wallet.currency });
  const after = direction === 'CREDIT' ? wallet.balance.add(amount) : wallet.balance.subtract(amount);
  const balanceAfter = after.toJSON().amount;
  if (!('observed_balance_amount' in overrides)) {
    transaction.observed_balance_amount = balanceAfter;
  }
  const version = wallet.version + 1;
  return {
    transaction,
    entry: {
      id: newId(),
      wallet_id: wallet.id,
      transaction_id: transaction.id,
      direction,
      amount: transaction.amount,
      currency: wallet.currency,
      balance_before: wallet.balance.toJSON().amount,
      balance_after: balanceAfter,
      wallet_version: version,
      created_at: now,
    },
    balanceAfter,
    version,
  };
}

async function applyMovement(tx: SQL, wallet: WalletFixture, move: Movement): Promise<void> {
  await insert(tx, 'wager_transactions', move.transaction);
  await insert(tx, 'wallet_ledger_entries', move.entry);
  await tx`update wallets set balance_amount = ${move.balanceAfter}, version = ${move.version}, updated_at = ${later}
           where id = ${wallet.id}`;
}

async function commitMovement(wallet: WalletFixture, direction: Direction, overrides: Row = {}): Promise<Row> {
  const move = movement(wallet, direction, overrides);
  await sql.begin((tx) => applyMovement(tx, wallet, move));
  wallet.balance = Money.from({ amount: move.balanceAfter, currency: wallet.currency });
  wallet.version = move.version;
  return move.transaction;
}

/** A wallet opened like the domain does: version 1, credited by its OPENING transaction. */
async function seedWallet(initialBalance = '100.00'): Promise<WalletFixture> {
  const wallet: WalletFixture = {
    id: newId(),
    playerId: newId(),
    currency: 'BRL',
    balance: Money.from({ amount: initialBalance, currency: 'BRL' }),
    version: 1,
  };
  const balance = wallet.balance.toJSON().amount;
  await sql.begin(async (tx) => {
    await insert(tx, 'wallets', walletRow({ id: wallet.id, player_id: wallet.playerId, balance_amount: balance }));
    if (wallet.balance.isZero()) {
      return;
    }
    const opening = transactionRow(wallet, {
      provider_id: 'internal',
      external_transaction_id: `opening:${wallet.id}`,
      idempotency_key: `opening:${wallet.id}`,
      round_id: 'opening',
      game_id: 'wallet-opening',
      kind: 'OPENING',
      amount: balance,
      observed_balance_amount: balance,
    });
    await insert(tx, 'wager_transactions', opening);
    await insert(tx, 'wallet_ledger_entries', {
      id: newId(),
      wallet_id: wallet.id,
      transaction_id: opening.id,
      direction: 'CREDIT',
      amount: balance,
      currency: wallet.currency,
      balance_before: '0.00',
      balance_after: balance,
      wallet_version: 1,
      created_at: now,
    });
  });
  return wallet;
}

/** A row that is only checked by single-table constraints (foreign keys are deferred and the tx rolls back). */
function expectRejectedInsert(table: string, row: Row, constraint: string): Promise<void> {
  return expectConstraintViolation(
    sql.begin((tx) => insert(tx, table, row)),
    constraint,
  );
}

const detached: WalletFixture = {
  id: newId(),
  playerId: newId(),
  currency: 'BRL',
  balance: Money.zero('BRL'),
  version: 1,
};

// -------------------------------------------------------------------- tests

describe('a consistent history commits', () => {
  test('opening, bet, win, loss, refund and rollback keep wallet and ledger in sync', async () => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT', { amount: '30.00' });
    await commitMovement(wallet, 'CREDIT', {
      kind: 'WIN',
      amount: '45.50',
      reference_external_transaction_id: bet.external_transaction_id,
      reference_transaction_id: bet.id,
    });
    await sql.begin((tx) =>
      insert(tx, 'wager_transactions', transactionRow(wallet, { kind: 'LOSS', amount: '0.00', observed_balance_amount: '115.50' })),
    );
    const secondBet = await commitMovement(wallet, 'DEBIT', { amount: '20.00' });
    await commitMovement(wallet, 'CREDIT', {
      kind: 'REFUND',
      amount: '20.00',
      reference_external_transaction_id: secondBet.external_transaction_id,
      reference_transaction_id: secondBet.id,
    });
    await commitMovement(wallet, 'CREDIT', {
      kind: 'ROLLBACK',
      amount: '30.00',
      reference_external_transaction_id: bet.external_transaction_id,
      reference_transaction_id: bet.id,
    });

    const [stored] = await sql`select balance_amount, version from wallets where id = ${wallet.id}`;
    const [ledger] = await sql`
      select count(*)::int as entries,
             sum(case direction when 'CREDIT' then amount else -amount end)::text as net,
             max(wallet_version) as last_version
      from wallet_ledger_entries where wallet_id = ${wallet.id}`;
    expect(stored).toEqual({ balance_amount: '145.50', version: 6 });
    expect(ledger).toEqual({ entries: 6, net: '145.50', last_version: 6 });
  });

  test('a wallet opened with zero balance starts its ledger at version 2', async () => {
    const wallet = await seedWallet('0.00');
    await commitMovement(wallet, 'CREDIT', { kind: 'WIN', amount: '5.00' });
    expect(wallet.version).toBe(2);
  });
});

describe('CHECK constraints', () => {
  // BEFORE triggers run ahead of CHECK constraints, and CHECKs are evaluated in name order,
  // so each row below violates only the rule under test.
  test.each<[string, Row, string]>([
    ['negative balance', { balance_amount: '-0.01' }, 'wallets_balance_non_negative_ck'],
    ['lowercase currency', { currency: 'brl' }, 'wallets_currency_ck'],
    ['created at version 0', { version: 0 }, 'wallets_initial_version'],
    ['created at version 2', { version: 2 }, 'wallets_initial_version'],
  ])('wallets: %s', (_case, overrides, constraint) => expectRejectedInsert('wallets', walletRow(overrides), constraint));

  test.each<[string, Row, string]>([
    [
      'PENDING is never persisted',
      { status: 'PENDING', processed_at: null, observed_balance_amount: null },
      'wager_transactions_status_ck',
    ],
    ['unknown kind', { kind: 'CASHOUT' }, 'wager_transactions_kind_ck'],
    ['zero BET', { amount: '0.00' }, 'wager_transactions_amount_ck'],
    ['negative amount', { amount: '-1.00' }, 'wager_transactions_amount_ck'],
    ['lowercase currency', { currency: 'brl' }, 'wager_transactions_currency_ck'],
    ['uppercase hash', { payload_hash: 'A'.repeat(64) }, 'wager_transactions_payload_hash_ck'],
    ['provider id too long', { provider_id: 'p'.repeat(101) }, 'wager_transactions_text_lengths_ck'],
    ['empty round id', { round_id: '' }, 'wager_transactions_text_lengths_ck'],
    ['external provider issuing OPENING', { kind: 'OPENING' }, 'wager_transactions_internal_provider_ck'],
    ['internal provider issuing a BET', { provider_id: 'internal' }, 'wager_transactions_internal_provider_ck'],
    [
      'OPENING not processed',
      { kind: 'OPENING', provider_id: 'internal', status: 'FAILED', failure_code: 'X', processed_at: null },
      'wager_transactions_opening_processed_ck',
    ],
    ['REFUND without reference', { kind: 'REFUND' }, 'wager_transactions_reference_required_ck'],
    ['ROLLBACK without reference', { kind: 'ROLLBACK' }, 'wager_transactions_reference_required_ck'],
    [
      'BET with reference',
      { reference_external_transaction_id: 'other', reference_transaction_id: newId() },
      'wager_transactions_reference_forbidden_ck',
    ],
    [
      'self reference',
      {
        kind: 'REFUND',
        external_transaction_id: 'same',
        reference_external_transaction_id: 'same',
        reference_transaction_id: newId(),
      },
      'wager_transactions_self_reference_ck',
    ],
    [
      'resolved reference id without external id',
      { kind: 'WIN', reference_transaction_id: newId() },
      'wager_transactions_reference_resolution_ck',
    ],
    [
      'resolved reference while still pending',
      {
        kind: 'REFUND',
        status: 'PENDING_REFERENCE',
        reference_external_transaction_id: 'bet',
        reference_transaction_id: newId(),
        processed_at: null,
        observed_balance_amount: null,
        next_attempt_at: later,
      },
      'wager_transactions_reference_resolution_ck',
    ],
    [
      'processed with an unresolved reference',
      { kind: 'REFUND', reference_external_transaction_id: 'bet' },
      'wager_transactions_processed_reference_resolved_ck',
    ],
    [
      'REJECTED without failure code',
      { status: 'REJECTED', processed_at: null },
      'wager_transactions_failure_code_ck',
    ],
    ['PROCESSED with failure code', { failure_code: 'INSUFFICIENT_FUNDS' }, 'wager_transactions_failure_code_ck'],
    [
      'malformed failure code',
      { status: 'FAILED', failure_code: 'insufficient funds', processed_at: null },
      'wager_transactions_failure_code_format_ck',
    ],
    ['PROCESSED without processed_at', { processed_at: null }, 'wager_transactions_processed_at_ck'],
    [
      'REJECTED with processed_at',
      { status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' },
      'wager_transactions_processed_at_ck',
    ],
    ['negative observed balance', { observed_balance_amount: '-1.00' }, 'wager_transactions_observed_balance_ck'],
    ['PROCESSED without observed balance', { observed_balance_amount: null }, 'wager_transactions_observed_balance_ck'],
    [
      'PENDING_REFERENCE without next attempt',
      {
        kind: 'REFUND',
        status: 'PENDING_REFERENCE',
        reference_external_transaction_id: 'bet',
        processed_at: null,
        observed_balance_amount: null,
      },
      'wager_transactions_pending_reference_ck',
    ],
    [
      'PENDING_REFERENCE without reference',
      { kind: 'WIN', status: 'PENDING_REFERENCE', processed_at: null, observed_balance_amount: null, next_attempt_at: later },
      'wager_transactions_pending_reference_ck',
    ],
    ['negative attempt count', { attempt_count: -1 }, 'wager_transactions_attempt_count_ck'],
  ])('wager_transactions: %s', (_case, overrides, constraint) =>
    expectRejectedInsert('wager_transactions', transactionRow(detached, overrides), constraint),
  );

  test('wager_transactions: a zero LOSS is accepted without a ledger entry', async () => {
    const wallet = await seedWallet('10.00');
    await sql.begin((tx) =>
      insert(tx, 'wager_transactions', transactionRow(wallet, { kind: 'LOSS', amount: '0.00', observed_balance_amount: '10.00' })),
    );
  });

  // Entries continue a real opening entry (version 1, balance 100.00) so the chain trigger lets them reach the CHECKs.
  test.each<[string, Row, string]>([
    ['unknown direction', { direction: 'TRANSFER' }, 'wallet_ledger_entries_direction_ck'],
    ['zero amount', { amount: '0.00', balance_after: '100.00' }, 'wallet_ledger_entries_amount_ck'],
    ['negative balance', { amount: '110.00', balance_after: '-10.00' }, 'wallet_ledger_entries_balances_ck'],
    ['unbalanced debit', { balance_after: '91.00' }, 'wallet_ledger_entries_arithmetic_ck'],
    ['credit that subtracts', { direction: 'CREDIT' }, 'wallet_ledger_entries_arithmetic_ck'],
  ])('wallet_ledger_entries: %s', async (_case, overrides, constraint) => {
    const wallet = await seedWallet('100.00');
    await expectRejectedInsert(
      'wallet_ledger_entries',
      {
        id: newId(),
        wallet_id: wallet.id,
        transaction_id: newId(),
        direction: 'DEBIT',
        amount: '10.00',
        currency: 'BRL',
        balance_before: '100.00',
        balance_after: '90.00',
        wallet_version: 2,
        created_at: now,
        ...overrides,
      },
      constraint,
    );
  });

  test.each<[string, string, Row, string]>([
    ['inbox_messages', 'uppercase hash', { payload_hash: 'F'.repeat(64) }, 'inbox_messages_payload_hash_ck'],
    ['inbox_messages', 'empty message id', { message_id: '' }, 'inbox_messages_key_lengths_ck'],
    [
      'inbox_messages',
      'processed before received',
      { processed_at: new Date(now.getTime() - 1) },
      'inbox_messages_processed_after_received_ck',
    ],
  ])('%s: %s', (table, _case, overrides, constraint) => expectRejectedInsert(table, inboxRow(overrides), constraint));

  test.each<[string, Row, string]>([
    ['payload that is a JSON string', { payload: '{"eventId":"x"}' }, 'outbox_messages_payload_ck'],
    ['empty event type', { event_type: '' }, 'outbox_messages_event_type_ck'],
    ['negative attempts', { attempts: -1 }, 'outbox_messages_attempts_ck'],
  ])('outbox_messages: %s', (_case, overrides, constraint) =>
    expectRejectedInsert('outbox_messages', outboxRow(overrides), constraint),
  );
});

describe('UNIQUE constraints and indexes', () => {
  test('one wallet per player and currency', async () => {
    const wallet = await seedWallet('0.00');
    await expectRejectedInsert('wallets', walletRow({ player_id: wallet.playerId }), 'wallets_player_currency_uq');
    await sql.begin((tx) => insert(tx, 'wallets', walletRow({ player_id: wallet.playerId, currency: 'USD' })));
  });

  test('idempotency key and (provider, external id) are unique', async () => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT');

    await expectConstraintViolation(
      sql.begin((tx) => applyMovement(tx, wallet, movement(wallet, 'DEBIT', { idempotency_key: bet.idempotency_key }))),
      'wager_transactions_idempotency_key_uq',
    );
    await expectConstraintViolation(
      sql.begin((tx) =>
        applyMovement(tx, wallet, movement(wallet, 'DEBIT', { external_transaction_id: bet.external_transaction_id })),
      ),
      'wager_transactions_provider_external_uq',
    );
    await commitMovement(wallet, 'DEBIT', {
      provider_id: 'provider-b',
      external_transaction_id: bet.external_transaction_id,
    });
  });

  test('one OPENING per wallet', async () => {
    const wallet = await seedWallet('100.00');
    await expectConstraintViolation(
      sql.begin((tx) =>
        applyMovement(
          tx,
          wallet,
          movement(wallet, 'CREDIT', { kind: 'OPENING', provider_id: 'internal', external_transaction_id: 'opening-again' }),
        ),
      ),
      'wager_transactions_single_opening_uq',
    );
  });

  test('a transaction has at most one ledger entry', async () => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT');
    const second = movement(wallet, 'DEBIT');
    second.entry.transaction_id = bet.id;
    await expectConstraintViolation(
      sql.begin((tx) => insert(tx, 'wallet_ledger_entries', second.entry)),
      'wallet_ledger_entries_transaction_uq',
    );
  });

  test('a wallet version has at most one ledger entry', async () => {
    const wallet = await seedWallet('100.00');
    const first = movement(wallet, 'DEBIT');
    const second = movement(wallet, 'DEBIT');
    await expectConstraintViolation(
      sql.begin(async (tx) => {
        await applyMovement(tx, wallet, first);
        await insert(tx, 'wager_transactions', second.transaction);
        await insert(tx, 'wallet_ledger_entries', second.entry);
      }),
      'wallet_ledger_entries_wallet_version_uq',
    );
  });

  test('a reference is reversed at most once, by REFUND or ROLLBACK', async () => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT');
    const reversal = { reference_external_transaction_id: bet.external_transaction_id, reference_transaction_id: bet.id };
    await commitMovement(wallet, 'CREDIT', { kind: 'REFUND', ...reversal });

    await expectConstraintViolation(
      sql.begin((tx) => applyMovement(tx, wallet, movement(wallet, 'CREDIT', { kind: 'ROLLBACK', ...reversal }))),
      'wager_transactions_single_active_reversal_uq',
    );
    await expectConstraintViolation(
      sql.begin((tx) =>
        insert(
          tx,
          'wager_transactions',
          transactionRow(wallet, {
            kind: 'REFUND',
            status: 'PENDING_REFERENCE',
            reference_external_transaction_id: bet.external_transaction_id,
            processed_at: null,
            observed_balance_amount: null,
            next_attempt_at: later,
          }),
        ),
      ),
      'wager_transactions_single_active_reversal_uq',
    );
    // A rejected duplicate is kept for audit and does not count as a reversal.
    await sql.begin((tx) =>
      insert(
        tx,
        'wager_transactions',
        transactionRow(wallet, {
          kind: 'ROLLBACK',
          status: 'REJECTED',
          failure_code: 'REFERENCE_ALREADY_REVERSED',
          reference_external_transaction_id: bet.external_transaction_id,
          processed_at: null,
          observed_balance_amount: wallet.balance.toJSON().amount,
        }),
      ),
    );
  });

  test('inbox deduplicates per consumer; outbox per event id', async () => {
    const inbox = inboxRow();
    await sql.begin((tx) => insert(tx, 'inbox_messages', inbox));
    await expectRejectedInsert('inbox_messages', { ...inbox }, 'inbox_messages_pk');
    await sql.begin((tx) => insert(tx, 'inbox_messages', { ...inbox, consumer_name: 'other-consumer' }));

    const outbox = outboxRow();
    await sql.begin((tx) => insert(tx, 'outbox_messages', outbox));
    await expectRejectedInsert('outbox_messages', { ...outbox }, 'outbox_messages_pkey');
  });
});

describe('referential integrity (checked at COMMIT)', () => {
  test('a transaction needs an existing wallet', async () => {
    await expectConstraintViolation(
      sql.begin((tx) => insert(tx, 'wager_transactions', transactionRow(detached, { kind: 'LOSS' }))),
      'wager_transactions_wallet_fk',
    );
  });

  test.each<[string, (wallet: WalletFixture) => Row]>([
    ['player', () => ({ player_id: newId() })],
    ['currency', () => ({ currency: 'USD' })],
  ])("a transaction's %s must be the wallet's own", async (_field, overrides) => {
    const wallet = await seedWallet('10.00');
    await expectConstraintViolation(
      sql.begin((tx) =>
        insert(tx, 'wager_transactions', transactionRow(wallet, { kind: 'LOSS', observed_balance_amount: '10.00', ...overrides(wallet) })),
      ),
      'wager_transactions_wallet_fk',
    );
  });

  test('a ledger entry needs an existing transaction', async () => {
    const wallet = await seedWallet('100.00');
    const move = movement(wallet, 'DEBIT');
    await expectConstraintViolation(
      sql.begin(async (tx) => {
        await insert(tx, 'wallet_ledger_entries', move.entry);
        await tx`update wallets set balance_amount = ${move.balanceAfter}, version = ${move.version} where id = ${wallet.id}`;
      }),
      'wallet_ledger_entries_transaction_fk',
    );
  });

  test("a ledger entry's amount must be the transaction's amount", async () => {
    const wallet = await seedWallet('100.00');
    const move = movement(wallet, 'DEBIT', { amount: '10.00' });
    move.transaction.amount = '10.01';
    await expectConstraintViolation(
      sql.begin((tx) => applyMovement(tx, wallet, move)),
      'wallet_ledger_entries_transaction_fk',
    );
  });

  test('a reference must exist', async () => {
    const wallet = await seedWallet('100.00');
    await expectConstraintViolation(
      sql.begin((tx) =>
        applyMovement(
          tx,
          wallet,
          movement(wallet, 'CREDIT', {
            kind: 'REFUND',
            reference_external_transaction_id: 'missing',
            reference_transaction_id: newId(),
          }),
        ),
      ),
      'wager_transactions_reference_fk',
    );
  });

  test.each<[string, Row]>([
    ['round', { round_id: 'round-2' }],
    ['provider', { provider_id: 'provider-b' }],
  ])('a reference must belong to the same %s', async (_scope, overrides) => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT');
    await expectConstraintViolation(
      sql.begin((tx) =>
        applyMovement(
          tx,
          wallet,
          movement(wallet, 'CREDIT', {
            kind: 'REFUND',
            reference_external_transaction_id: bet.external_transaction_id,
            reference_transaction_id: bet.id,
            ...overrides,
          }),
        ),
      ),
      'wager_transactions_reference_fk',
    );
  });

  test('a reference must belong to the same wallet', async () => {
    const owner = await seedWallet('100.00');
    const other = await seedWallet('100.00');
    const bet = await commitMovement(owner, 'DEBIT');
    await expectConstraintViolation(
      sql.begin((tx) =>
        applyMovement(
          tx,
          other,
          movement(other, 'CREDIT', {
            kind: 'REFUND',
            reference_external_transaction_id: bet.external_transaction_id,
            reference_transaction_id: bet.id,
          }),
        ),
      ),
      'wager_transactions_reference_fk',
    );
  });
});

describe('wallet and ledger consistency (constraint triggers)', () => {
  test('the balance cannot change without a matching ledger entry', async () => {
    const wallet = await seedWallet('100.00');
    await expectConstraintViolation(
      sql`update wallets set balance_amount = 1000, version = 2 where id = ${wallet.id}`,
      'wallets_balance_matches_ledger',
    );
  });

  test('a wallet cannot be created with a balance and no OPENING entry', async () => {
    await expectRejectedInsert('wallets', walletRow({ balance_amount: '50.00' }), 'wallets_balance_matches_ledger');
  });

  test.each<[string, (wallet: WalletFixture) => Promise<unknown>]>([
    ['version moves without a balance change', (w) => sql`update wallets set version = version + 1 where id = ${w.id}`],
    [
      'balance moves by more than one version',
      (w) => sql`update wallets set balance_amount = 90, version = version + 2 where id = ${w.id}`,
    ],
    ['balance moves without a version', (w) => sql`update wallets set balance_amount = 90 where id = ${w.id}`],
  ])('wallet version tracks the balance: %s', async (_case, update) => {
    const wallet = await seedWallet('100.00');
    await expectConstraintViolation(update(wallet), 'wallets_version_tracks_balance');
  });

  test.each<[string, (wallet: WalletFixture) => Promise<unknown>]>([
    ['player_id', (w) => sql`update wallets set player_id = ${newId()} where id = ${w.id}`],
    ['currency', (w) => sql`update wallets set currency = 'USD' where id = ${w.id}`],
  ])('wallet identity is immutable: %s', async (_column, update) => {
    const wallet = await seedWallet('0.00');
    await expectConstraintViolation(update(wallet), 'wallets_identity_immutable');
  });

  test('a processed balance-moving transaction needs a ledger entry', async () => {
    const wallet = await seedWallet('100.00');
    await expectConstraintViolation(
      sql.begin((tx) => insert(tx, 'wager_transactions', transactionRow(wallet, { observed_balance_amount: '90.00' }))),
      'wager_transactions_processed_has_ledger',
    );
  });

  test.each<[string, Row]>([
    ['REJECTED', { status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS', processed_at: null }],
    ['FAILED', { status: 'FAILED', failure_code: 'PERMANENT_INTEGRITY_ERROR', processed_at: null, observed_balance_amount: null }],
    ['LOSS', { kind: 'LOSS' }],
  ])('a %s transaction cannot move the balance', async (_case, overrides) => {
    const wallet = await seedWallet('100.00');
    await expectConstraintViolation(
      sql.begin((tx) => applyMovement(tx, wallet, movement(wallet, 'DEBIT', overrides))),
      'wallet_ledger_entries_transaction_applied',
    );
  });

  test.each<[string, Direction, Row]>([
    ['BET as a credit', 'CREDIT', {}],
    ['WIN as a debit', 'DEBIT', { kind: 'WIN' }],
  ])('the ledger direction follows the kind: %s', async (_case, direction, overrides) => {
    const wallet = await seedWallet('100.00');
    await expectConstraintViolation(
      sql.begin((tx) => applyMovement(tx, wallet, movement(wallet, direction, overrides))),
      'wallet_ledger_entries_direction_matches_kind',
    );
  });

  test('a ROLLBACK debits what it reverses when the reference was a credit', async () => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT');
    const win = await commitMovement(wallet, 'CREDIT', {
      kind: 'WIN',
      amount: '25.00',
      reference_external_transaction_id: bet.external_transaction_id,
      reference_transaction_id: bet.id,
    });
    const rollback = {
      kind: 'ROLLBACK',
      amount: '25.00',
      reference_external_transaction_id: win.external_transaction_id,
      reference_transaction_id: win.id,
    };

    await expectConstraintViolation(
      sql.begin((tx) => applyMovement(tx, wallet, movement(wallet, 'CREDIT', rollback))),
      'wallet_ledger_entries_direction_matches_kind',
    );
    await commitMovement(wallet, 'DEBIT', rollback);
    expect(wallet.balance.toJSON().amount).toBe('90.00');
  });

  test('a ledger entry must match the wallet it moves', async () => {
    const wallet = await seedWallet('100.00');
    const move = movement(wallet, 'DEBIT');
    await expectConstraintViolation(
      sql.begin(async (tx) => {
        await insert(tx, 'wager_transactions', move.transaction);
        await insert(tx, 'wallet_ledger_entries', move.entry);
      }),
      'wallet_ledger_entries_match_wallet',
    );
  });

  test("a ledger entry must start where the previous one ended", async () => {
    const wallet = await seedWallet('100.00');
    const move = movement(wallet, 'DEBIT');
    move.entry.balance_before = '200.00';
    move.entry.balance_after = '190.00';
    await expectConstraintViolation(sql.begin((tx) => applyMovement(tx, wallet, move)), 'wallet_ledger_entries_chain');
  });

  test('ledger versions cannot be skipped', async () => {
    const wallet = await seedWallet('100.00');
    const move = movement(wallet, 'DEBIT');
    move.entry.wallet_version = 3;
    move.version = 3;
    await expectConstraintViolation(sql.begin((tx) => applyMovement(tx, wallet, move)), 'wallet_ledger_entries_chain');
  });
});

describe('reference rules (constraint triggers)', () => {
  test('REFUND cannot reference a WIN', async () => {
    const wallet = await seedWallet('100.00');
    const win = await commitMovement(wallet, 'CREDIT', { kind: 'WIN' });
    await expectConstraintViolation(
      sql.begin((tx) =>
        applyMovement(
          tx,
          wallet,
          movement(wallet, 'CREDIT', {
            kind: 'REFUND',
            reference_external_transaction_id: win.external_transaction_id,
            reference_transaction_id: win.id,
          }),
        ),
      ),
      'wager_transactions_reference_kind',
    );
  });

  test('the reference must be PROCESSED', async () => {
    const wallet = await seedWallet('100.00');
    const rejected = transactionRow(wallet, {
      status: 'REJECTED',
      failure_code: 'INSUFFICIENT_FUNDS',
      processed_at: null,
      observed_balance_amount: '100.00',
    });
    await sql.begin((tx) => insert(tx, 'wager_transactions', rejected));

    await expectConstraintViolation(
      sql.begin((tx) =>
        applyMovement(
          tx,
          wallet,
          movement(wallet, 'CREDIT', {
            kind: 'ROLLBACK',
            reference_external_transaction_id: rejected.external_transaction_id,
            reference_transaction_id: rejected.id,
          }),
        ),
      ),
      'wager_transactions_reference_processed',
    );
  });

  test('a reversal must reverse the exact amount', async () => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT', { amount: '10.00' });
    await expectConstraintViolation(
      sql.begin((tx) =>
        applyMovement(
          tx,
          wallet,
          movement(wallet, 'CREDIT', {
            kind: 'REFUND',
            amount: '9.99',
            reference_external_transaction_id: bet.external_transaction_id,
            reference_transaction_id: bet.id,
          }),
        ),
      ),
      'wager_transactions_reversal_amount',
    );
  });
});

describe('immutability', () => {
  test.each<[string, (id: string) => Promise<unknown>]>([
    ['UPDATE', (id) => sql`update wallet_ledger_entries set amount = amount where id = ${id}`],
    ['DELETE', (id) => sql`delete from wallet_ledger_entries where id = ${id}`],
    ['TRUNCATE', () => sql`truncate wallet_ledger_entries`],
  ])('the ledger rejects %s', async (_operation, statement) => {
    const wallet = await seedWallet('100.00');
    const [entry] = await sql`select id from wallet_ledger_entries where wallet_id = ${wallet.id}`;
    await expectConstraintViolation(statement(entry.id), 'wallet_ledger_entries_append_only');

    const [{ entries }] = await sql`select count(*)::int as entries from wallet_ledger_entries where id = ${entry.id}`;
    expect(entries).toBe(1);
  });

  test('wager transactions cannot be deleted', async () => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT');
    await expectConstraintViolation(
      sql`delete from wager_transactions where id = ${bet.id}`,
      'wager_transactions_no_delete',
    );
  });

  test.each(['PROCESSED', 'REJECTED', 'FAILED'])('a %s transaction is frozen', async (status) => {
    const wallet = await seedWallet('100.00');
    const id =
      status === 'PROCESSED'
        ? (await commitMovement(wallet, 'DEBIT')).id
        : await (async () => {
            const row = transactionRow(wallet, {
              status,
              failure_code: 'INSUFFICIENT_FUNDS',
              processed_at: null,
              observed_balance_amount: status === 'REJECTED' ? '100.00' : null,
            });
            await sql.begin((tx) => insert(tx, 'wager_transactions', row));
            return row.id;
          })();
    await expectConstraintViolation(
      sql`update wager_transactions set attempt_count = attempt_count + 1 where id = ${id}`,
      'wager_transactions_terminal_immutable',
    );
  });

  test('PENDING_REFERENCE can be resolved, but its payload is immutable', async () => {
    const wallet = await seedWallet('100.00');
    const bet = await commitMovement(wallet, 'DEBIT');
    const pending = transactionRow(wallet, {
      kind: 'REFUND',
      status: 'PENDING_REFERENCE',
      reference_external_transaction_id: bet.external_transaction_id,
      processed_at: null,
      observed_balance_amount: null,
      attempt_count: 1,
      next_attempt_at: later,
    });
    await sql.begin((tx) => insert(tx, 'wager_transactions', pending));

    await expectConstraintViolation(
      sql`update wager_transactions set amount = 11 where id = ${pending.id}`,
      'wager_transactions_payload_immutable',
    );
    await sql`update wager_transactions set attempt_count = 2, next_attempt_at = ${later} where id = ${pending.id}`;

    const after = wallet.balance.add(Money.from({ amount: '10.00', currency: 'BRL' })).toJSON().amount;
    await sql.begin(async (tx) => {
      await tx`update wager_transactions
               set status = 'PROCESSED', reference_transaction_id = ${bet.id}, observed_balance_amount = ${after},
                   processed_at = ${later}, next_attempt_at = null
               where id = ${pending.id}`;
      await insert(tx, 'wallet_ledger_entries', {
        id: newId(),
        wallet_id: wallet.id,
        transaction_id: pending.id,
        direction: 'CREDIT',
        amount: '10.00',
        currency: 'BRL',
        balance_before: wallet.balance.toJSON().amount,
        balance_after: after,
        wallet_version: wallet.version + 1,
        created_at: later,
      });
      await tx`update wallets set balance_amount = ${after}, version = ${wallet.version + 1} where id = ${wallet.id}`;
    });

    const [stored] = await sql`select status, attempt_count from wager_transactions where id = ${pending.id}`;
    expect(stored).toEqual({ status: 'PROCESSED', attempt_count: 2 });
  });
});

describe('inbox and outbox guards', () => {
  test('an inbox message is immutable and processed once', async () => {
    const row = inboxRow();
    await sql.begin((tx) => insert(tx, 'inbox_messages', row));
    const setProcessedAt = (at: Date | null) =>
      sql`update inbox_messages set processed_at = ${at}
          where consumer_name = ${row.consumer_name} and message_id = ${row.message_id}`;

    await expectConstraintViolation(
      sql`update inbox_messages set payload_hash = ${'b'.repeat(64)}
          where consumer_name = ${row.consumer_name} and message_id = ${row.message_id}`,
      'inbox_messages_immutable',
    );
    await setProcessedAt(later);
    await expectConstraintViolation(setProcessedAt(new Date(later.getTime() + 1)), 'inbox_messages_processed_once');
    await expectConstraintViolation(setProcessedAt(null), 'inbox_messages_processed_once');
  });

  test('an outbox message is immutable, published once, and its attempts only grow', async () => {
    const row = outboxRow();
    await sql.begin((tx) => insert(tx, 'outbox_messages', row));

    await expectConstraintViolation(
      sql`update outbox_messages set payload = ${{ eventId: 'changed' }} where id = ${row.id}`,
      'outbox_messages_immutable',
    );
    await sql`update outbox_messages set attempts = 2, next_attempt_at = ${later} where id = ${row.id}`;
    await expectConstraintViolation(
      sql`update outbox_messages set attempts = 1 where id = ${row.id}`,
      'outbox_messages_attempts_monotonic',
    );
    await sql`update outbox_messages set published_at = ${later} where id = ${row.id}`;
    await expectConstraintViolation(
      sql`update outbox_messages set published_at = null where id = ${row.id}`,
      'outbox_messages_published_once',
    );
  });
});
