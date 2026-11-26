import { describe, expect, test } from 'bun:test';
import { InvalidStateError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { canonicalHash } from '../../src/common/hashing/canonical-hash';
import { isUuid, newId } from '../../src/common/ids';
import type { EventContext, IntegrationEvent } from '../../src/common/messaging/integration-event';
import { OutboxMessage } from '../../src/common/messaging/outbox-message';
import { Money } from '../../src/common/money/money';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from '../../src/wagering/wager-events';
import { WagerTransaction, WagerTransactionKind } from '../../src/wagering/wager-transaction';
import { Wallet } from '../../src/wallets/wallet';
import { WalletBalanceChanged } from '../../src/wallets/wallet-events';
import { LedgerDirection } from '../../src/wallets/wallet-ledger-entry';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const t0 = new Date('2026-01-01T00:00:00.000Z');
const t1 = new Date('2026-01-01T00:00:05.000Z');
const ctx: EventContext = { correlationId: 'req-42', causationId: newId(), occurredAt: t1 };

function openWallet(initialBalance = '100.00') {
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
  return { wallet, opening, openingEntry: openingEntry! };
}

function submit(wallet: Wallet, kind: WagerTransactionKind, amount: string, reference?: string) {
  const externalTransactionId = `ext-${newId()}`;
  return WagerTransaction.create({
    id: newId(),
    providerId: 'provider-a',
    externalTransactionId,
    idempotencyKey: `provider-a:${externalTransactionId}`,
    payloadHash: canonicalHash({ externalTransactionId }),
    walletId: wallet.id,
    playerId: wallet.playerId,
    roundId: 'round-1',
    gameId: 'game-1',
    kind,
    money: brl(amount),
    ...(reference !== undefined ? { referenceExternalTransactionId: reference } : {}),
    now: t1,
  });
}

function expectEnvelope(event: IntegrationEvent<object>, eventType: string, walletId: string) {
  const json = event.toJSON();
  expect(json).toMatchObject({
    eventType,
    version: 1,
    aggregateId: walletId,
    correlationId: ctx.correlationId,
    causationId: ctx.causationId,
    occurredAt: '2026-01-01T00:00:05.000Z',
  });
  expect(isUuid(json.eventId)).toBe(true);
  // The outbox stores the JSON round trip; nothing may be lost or coerced on the way.
  expect(OutboxMessage.enqueue(event).payload).toEqual(JSON.parse(JSON.stringify(json)));
  return json.data as Record<string, unknown>;
}

describe('WagerTransactionProcessed', () => {
  test('serializes a processed BET with the observed balance as decimal strings', () => {
    const { wallet } = openWallet();
    const bet = submit(wallet, WagerTransactionKind.Bet, '30.5');
    wallet.debit(bet.money, bet.id, t1);
    bet.markProcessed(undefined, wallet.balance, t1);

    const event = WagerTransactionProcessed.from(bet, ctx);

    expect(expectEnvelope(event, 'WagerTransactionProcessed', wallet.id)).toEqual({
      transactionId: bet.id,
      providerId: 'provider-a',
      externalTransactionId: bet.externalTransactionId,
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: 'BET',
      money: { amount: '30.50', currency: 'BRL' },
      referenceExternalTransactionId: null,
      referenceTransactionId: null,
      observedBalance: { amount: '69.50', currency: 'BRL' },
    });
    expect(Object.isFrozen(event.data)).toBe(true);
  });

  test('carries both reference ids of a processed REFUND, and covers the OPENING credit and a LOSS', () => {
    const { wallet, opening } = openWallet();
    const bet = submit(wallet, WagerTransactionKind.Bet, '10.00');
    const refund = submit(wallet, WagerTransactionKind.Refund, '10.00', bet.externalTransactionId);
    refund.markProcessed(bet.id, wallet.balance, t1);
    const loss = submit(wallet, WagerTransactionKind.Loss, '0');
    loss.markProcessed(undefined, wallet.balance, t1);

    expect(WagerTransactionProcessed.from(refund, ctx).data).toMatchObject({
      kind: 'REFUND',
      referenceExternalTransactionId: bet.externalTransactionId,
      referenceTransactionId: bet.id,
    });
    expect(WagerTransactionProcessed.from(opening, ctx).data).toMatchObject({
      kind: 'OPENING',
      providerId: 'internal',
      observedBalance: { amount: '100.00', currency: 'BRL' },
    });
    expect(WagerTransactionProcessed.from(loss, ctx).data).toMatchObject({ kind: 'LOSS', money: { amount: '0.00' } });
  });

  test('refuses a transaction that is not PROCESSED', () => {
    const { wallet } = openWallet();
    const bet = submit(wallet, WagerTransactionKind.Bet, '10.00');
    expect(() => WagerTransactionProcessed.from(bet, ctx)).toThrow(InvalidStateError);
    bet.reject(FailureCode.InsufficientFunds, wallet.balance);
    expect(() => WagerTransactionProcessed.from(bet, ctx)).toThrow(InvalidStateError);
  });
});

describe('WagerTransactionRejected', () => {
  test('serializes the failure code and the balance observed when it was decided', () => {
    const { wallet } = openWallet('5.00');
    const bet = submit(wallet, WagerTransactionKind.Bet, '10.00');
    bet.reject(FailureCode.InsufficientFunds, wallet.balance);

    const data = expectEnvelope(WagerTransactionRejected.from(bet, ctx), 'WagerTransactionRejected', wallet.id);

    expect(data).toMatchObject({
      transactionId: bet.id,
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
      failureCode: 'INSUFFICIENT_FUNDS',
      observedBalance: { amount: '5.00', currency: 'BRL' },
    });
    expect('referenceTransactionId' in data).toBe(false);
  });

  test('refuses a transaction that is not REJECTED', () => {
    const { wallet } = openWallet();
    const bet = submit(wallet, WagerTransactionKind.Bet, '10.00');
    bet.markProcessed(undefined, wallet.balance, t1);
    expect(() => WagerTransactionRejected.from(bet, ctx)).toThrow(InvalidStateError);
  });
});

describe('WagerTransactionPendingReference', () => {
  test('serializes the missing reference and when it will be looked up again', () => {
    const { wallet } = openWallet();
    const rollback = submit(wallet, WagerTransactionKind.Rollback, '10.00', 'bet-not-yet-arrived');
    rollback.markPendingReference(new Date('2026-01-01T00:01:00.000Z'));

    const data = expectEnvelope(
      WagerTransactionPendingReference.from(rollback, ctx),
      'WagerTransactionPendingReference',
      wallet.id,
    );

    expect(data).toMatchObject({
      transactionId: rollback.id,
      kind: 'ROLLBACK',
      referenceExternalTransactionId: 'bet-not-yet-arrived',
      attemptCount: 1,
      nextAttemptAt: '2026-01-01T00:01:00.000Z',
    });
  });

  test('refuses a transaction that is not PENDING_REFERENCE', () => {
    const { wallet } = openWallet();
    const refund = submit(wallet, WagerTransactionKind.Refund, '10.00', 'bet-1');
    expect(() => WagerTransactionPendingReference.from(refund, ctx)).toThrow(InvalidStateError);
  });
});

describe('WalletBalanceChanged', () => {
  test('serializes the opening credit and a later debit as MoneyProps', () => {
    const { wallet, opening, openingEntry } = openWallet('100.00');

    const openingData = expectEnvelope(WalletBalanceChanged.from(wallet, openingEntry, ctx), 'WalletBalanceChanged', wallet.id);
    expect(openingData).toEqual({
      walletId: wallet.id,
      transactionId: opening.id,
      direction: 'CREDIT',
      money: { amount: '100.00', currency: 'BRL' },
      balanceBefore: { amount: '0.00', currency: 'BRL' },
      balanceAfter: { amount: '100.00', currency: 'BRL' },
      walletVersion: 1,
    });

    const debitId = newId();
    const debit = wallet.debit(brl('0.01'), debitId, t1);
    expect(WalletBalanceChanged.from(wallet, debit, ctx).data).toEqual({
      walletId: wallet.id,
      transactionId: debitId,
      direction: LedgerDirection.Debit,
      money: { amount: '0.01', currency: 'BRL' },
      balanceBefore: { amount: '100.00', currency: 'BRL' },
      balanceAfter: { amount: '99.99', currency: 'BRL' },
      walletVersion: 2,
    });
  });

  test('refuses an entry that is not the head of the wallet', () => {
    const { wallet, openingEntry } = openWallet('100.00');
    wallet.debit(brl('1.00'), newId(), t1);
    const other = openWallet('100.00');

    expect(() => WalletBalanceChanged.from(wallet, openingEntry, ctx)).toThrow(InvalidStateError);
    expect(() => WalletBalanceChanged.from(other.wallet, openingEntry, ctx)).toThrow(InvalidStateError);
  });
});

describe('event identity and context', () => {
  test('each event gets its own id, and causationId is omitted when absent', () => {
    const { wallet, opening, openingEntry } = openWallet();
    const bare: EventContext = { correlationId: 'req-1', occurredAt: t0 };

    const first = WagerTransactionProcessed.from(opening, bare);
    const second = WagerTransactionProcessed.from(opening, bare);
    const balance = WalletBalanceChanged.from(wallet, openingEntry, bare);

    expect(new Set([first.eventId, second.eventId, balance.eventId]).size).toBe(3);
    expect('causationId' in first.toJSON()).toBe(false);
    expect(first.correlationId).toBe('req-1');
    expect(first.aggregateId).toBe(balance.aggregateId);
  });
});
