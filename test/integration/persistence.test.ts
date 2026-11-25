import type { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { canonicalHash } from '../../src/common/hashing/canonical-hash';
import { newId } from '../../src/common/ids';
import { InboxMessage } from '../../src/common/messaging/inbox-message';
import { IntegrationEvent, type IntegrationEventProps } from '../../src/common/messaging/integration-event';
import { OutboxMessage } from '../../src/common/messaging/outbox-message';
import { Money } from '../../src/common/money/money';
import { DecimalType } from '../../src/database/decimal.type';
import {
  InboxMessageRecord,
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletLedgerEntryRecord,
  WalletRecord,
} from '../../src/database/entities';
import { WagerTransaction, WagerTransactionKind } from '../../src/wagering/wager-transaction';
import { Wallet } from '../../src/wallets/wallet';
import { WalletLedgerEntry } from '../../src/wallets/wallet-ledger-entry';
import { createTestOrm, resetSchema } from '../support/database';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const t0 = new Date('2026-01-01T00:00:00.123Z');
const t1 = new Date('2026-01-01T00:00:01.456Z');

class SampleEvent extends IntegrationEvent<{ amount: string }> {
  readonly eventType = 'sample.happened';
  readonly version = 1;

  constructor(props: IntegrationEventProps<{ amount: string }>) {
    super(props);
  }
}

let orm: MikroORM;

beforeAll(async () => {
  const migrator = await createTestOrm('migrator');
  try {
    await resetSchema(migrator);
  } finally {
    await migrator.close();
  }
  orm = await createTestOrm('app');
});

afterAll(async () => {
  await orm.close();
});

/** Persists an opened wallet the way services will: wallet, OPENING transaction and entry in one SQL transaction. */
async function persistOpenedWallet(initialBalance: string) {
  const walletId = newId();
  const playerId = newId();
  const opening = WagerTransaction.opening({ id: newId(), walletId, playerId, money: brl(initialBalance), now: t0 });
  const { wallet, openingEntry } = Wallet.open({
    id: walletId,
    playerId,
    initialBalance: brl(initialBalance),
    openingTransactionId: opening.id,
    now: t0,
  });
  await orm.em.fork().transactional((em) => {
    em.create(WalletRecord, wallet.toState());
    em.create(WagerTransactionRecord, opening.toState());
    em.create(WalletLedgerEntryRecord, openingEntry!.toState());
  });
  return { wallet, opening, openingEntry: openingEntry! };
}

describe('domain state round-trips through the records', () => {
  test('wallet, OPENING transaction and ledger entry are rehydrated unchanged', async () => {
    const { wallet, opening, openingEntry } = await persistOpenedWallet('100.00');

    const em = orm.em.fork();
    const walletRecord = await em.findOneOrFail(WalletRecord, wallet.id);
    const transactionRecord = await em.findOneOrFail(WagerTransactionRecord, opening.id);
    const entryRecord = await em.findOneOrFail(WalletLedgerEntryRecord, openingEntry.id);

    expect(walletRecord).toBeInstanceOf(WalletRecord);
    expect(Wallet.rehydrate(walletRecord).toState()).toEqual(wallet.toState());
    expect(WagerTransaction.rehydrate(transactionRecord).toState()).toEqual(opening.toState());
    expect(WalletLedgerEntry.rehydrate(entryRecord).toState()).toEqual(openingEntry.toState());
  });

  test('a domain movement applied to a rehydrated wallet satisfies the database rules', async () => {
    const { wallet: opened } = await persistOpenedWallet('50.00');

    const bet = WagerTransaction.create({
      id: newId(),
      providerId: 'provider-a',
      externalTransactionId: `bet-${opened.id}`,
      idempotencyKey: `idem-${opened.id}`,
      payloadHash: canonicalHash({ bet: opened.id }),
      walletId: opened.id,
      playerId: opened.playerId,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: WagerTransactionKind.Bet,
      money: brl('12.34'),
      now: t1,
    });

    await orm.em.fork().transactional(async (em) => {
      const record = await em.findOneOrFail(WalletRecord, opened.id);
      const wallet = Wallet.rehydrate(record);
      const entry = wallet.debit(bet.money, bet.id, t1);
      bet.markProcessed(undefined, wallet.balance, t1);

      em.assign(record, wallet.toState());
      em.create(WagerTransactionRecord, bet.toState());
      em.create(WalletLedgerEntryRecord, entry.toState());
    });

    const em = orm.em.fork();
    const stored = Wallet.rehydrate(await em.findOneOrFail(WalletRecord, opened.id));
    const storedBet = WagerTransaction.rehydrate(await em.findOneOrFail(WagerTransactionRecord, bet.id));
    expect(stored.toState()).toMatchObject({ balanceAmount: '37.66', version: 2, updatedAt: t1 });
    expect(storedBet.toState()).toEqual(bet.toState());
    expect(storedBet.observedBalance?.toJSON().amount).toBe('37.66');
  });

  test('inbox and outbox messages are rehydrated unchanged', async () => {
    const inbox = InboxMessage.receive({
      messageId: newId(),
      consumerName: 'wager-processor',
      payloadHash: canonicalHash({ m: 1 }),
      receivedAt: t0,
    });
    inbox.markProcessed(t1);
    const outbox = OutboxMessage.enqueue(
      new SampleEvent({ eventId: newId(), aggregateId: newId(), correlationId: newId(), occurredAt: t0, data: { amount: '1.00' } }),
    );
    outbox.scheduleRetry(t0);

    await orm.em.fork().transactional((em) => {
      em.create(InboxMessageRecord, inbox.toState());
      em.create(OutboxMessageRecord, outbox.toState());
    });

    const em = orm.em.fork();
    const inboxRecord = await em.findOneOrFail(InboxMessageRecord, {
      consumerName: inbox.consumerName,
      messageId: inbox.messageId,
    });
    const outboxRecord = await em.findOneOrFail(OutboxMessageRecord, outbox.id);
    expect(InboxMessage.rehydrate(inboxRecord).toState()).toEqual(inbox.toState());
    expect(OutboxMessage.rehydrate(outboxRecord).toState()).toEqual(outbox.toState());
  });
});

describe('DecimalType', () => {
  test('reads numeric as an exact string with scale 2, even at the top of the range', async () => {
    const { wallet } = await persistOpenedWallet('99999999999999999.99');
    const record = await orm.em.fork().findOneOrFail(WalletRecord, wallet.id);
    expect(record.balanceAmount).toBe('99999999999999999.99');

    const small = await persistOpenedWallet('7');
    expect((await orm.em.fork().findOneOrFail(WalletRecord, small.wallet.id)).balanceAmount).toBe('7.00');
  });

  test.each<[string, unknown]>([
    ['a number', 100],
    ['a float', 0.1],
    ['more than 2 decimals', '10.005'],
    ['an exponent', '1e3'],
  ])('refuses to write %s', async (_case, balanceAmount) => {
    const { wallet } = Wallet.open({ id: newId(), playerId: newId(), initialBalance: brl('0'), now: t0 });
    const work = orm.em.fork().transactional((em) => {
      em.create(WalletRecord, { ...wallet.toState(), balanceAmount: balanceAmount as string });
    });
    await expect(work).rejects.toThrow(TypeError);

    const count = await orm.em.fork().count(WalletRecord, { id: wallet.id });
    expect(count).toBe(0);
  });

  test('refuses to read a number', () => {
    expect(() => new DecimalType().convertToJSValue(1 as unknown as string)).toThrow(TypeError);
    expect(new DecimalType().convertToJSValue('1.00')).toBe('1.00');
    expect(new DecimalType().convertToJSValue(null)).toBeNull();
  });
});
