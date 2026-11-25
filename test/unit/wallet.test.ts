import { describe, expect, test } from 'bun:test';
import { FailureCode } from '../../src/common/errors/failure-code';
import { newId } from '../../src/common/ids';
import { Money } from '../../src/common/money/money';
import { Wallet } from '../../src/wallets/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../../src/wallets/wallet-ledger-entry';
import { expectDomainError } from '../support/domain';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const t0 = new Date('2026-01-01T00:00:00.000Z');
const t1 = new Date('2026-01-01T00:00:01.000Z');

function openWallet(initialBalance = '100.00') {
  return Wallet.open({
    id: newId(),
    playerId: newId(),
    initialBalance: brl(initialBalance),
    openingTransactionId: initialBalance === '0' ? undefined : newId(),
    now: t0,
  });
}

describe('Wallet.open', () => {
  test('a positive balance is credited by the OPENING entry at version 1', () => {
    const openingTransactionId = newId();
    const { wallet, openingEntry } = Wallet.open({
      id: newId(),
      playerId: newId(),
      initialBalance: brl('100'),
      openingTransactionId,
      now: t0,
    });

    expect(wallet.toState()).toMatchObject({ currency: 'BRL', balanceAmount: '100.00', version: 1 });
    expect(openingEntry?.toState()).toMatchObject({
      walletId: wallet.id,
      transactionId: openingTransactionId,
      direction: LedgerDirection.Credit,
      amount: '100.00',
      balanceBefore: '0.00',
      balanceAfter: '100.00',
      walletVersion: 1,
    });
  });

  test('a zero balance has no ledger entry', () => {
    const { wallet, openingEntry } = openWallet('0');
    expect(wallet.balance.isZero()).toBe(true);
    expect(wallet.version).toBe(1);
    expect(openingEntry).toBeUndefined();
  });

  test('the OPENING transaction id is required exactly when the balance is positive', () => {
    const base = { id: newId(), playerId: newId(), now: t0 };
    expect(() => Wallet.open({ ...base, initialBalance: brl('1') })).toThrow(RangeError);
    expect(() => Wallet.open({ ...base, initialBalance: brl('0'), openingTransactionId: newId() })).toThrow(
      RangeError,
    );
  });

  test('ids must be UUIDs', () => {
    expectDomainError(
      () => Wallet.open({ id: 'wallet-1', playerId: newId(), initialBalance: brl('0'), now: t0 }),
      FailureCode.InvalidPayload,
    );
    expectDomainError(
      () => Wallet.open({ id: newId(), playerId: 'player-1', initialBalance: brl('0'), now: t0 }),
      FailureCode.InvalidPayload,
    );
  });
});

describe('Wallet movements', () => {
  test('debit lowers the balance and bumps the version by one', () => {
    const { wallet } = openWallet('100.00');
    const transactionId = newId();

    const entry = wallet.debit(brl('30.25'), transactionId, t1);

    expect(wallet.toState()).toMatchObject({ balanceAmount: '69.75', version: 2, updatedAt: t1 });
    expect(entry.toState()).toMatchObject({
      transactionId,
      direction: LedgerDirection.Debit,
      amount: '30.25',
      balanceBefore: '100.00',
      balanceAfter: '69.75',
      walletVersion: 2,
      createdAt: t1,
    });
  });

  test('credit raises the balance and bumps the version by one', () => {
    const { wallet } = openWallet('0');
    const entry = wallet.credit(brl('50'), newId(), t1);

    expect(wallet.toState()).toMatchObject({ balanceAmount: '50.00', version: 2 });
    expect(entry.toState()).toMatchObject({ direction: LedgerDirection.Credit, walletVersion: 2 });
  });

  test('the whole balance can be debited', () => {
    const { wallet } = openWallet('10.00');
    wallet.debit(brl('10.00'), newId(), t1);
    expect(wallet.balance.toJSON().amount).toBe('0.00');
  });

  test('insufficient funds are rejected and leave the wallet untouched', () => {
    const { wallet } = openWallet('10.00');
    expect(wallet.canDebit(brl('10.01'))).toBe(false);

    expectDomainError(() => wallet.debit(brl('10.01'), newId(), t1), FailureCode.InsufficientFunds);
    expect(wallet.toState()).toMatchObject({ balanceAmount: '10.00', version: 1, updatedAt: t0 });
  });

  test('another currency is rejected and leaves the wallet untouched', () => {
    const { wallet } = openWallet('10.00');
    const usd = Money.from({ amount: '1.00', currency: 'USD' });

    expectDomainError(() => wallet.debit(usd, newId(), t1), FailureCode.CurrencyMismatch);
    expectDomainError(() => wallet.credit(usd, newId(), t1), FailureCode.CurrencyMismatch);
    expect(wallet.version).toBe(1);
  });

  test('zero movements are rejected', () => {
    const { wallet } = openWallet('10.00');
    expectDomainError(() => wallet.credit(brl('0'), newId(), t1), FailureCode.InvalidAmount);
    expectDomainError(() => wallet.debit(brl('0'), newId(), t1), FailureCode.InvalidAmount);
    expect(wallet.version).toBe(1);
  });

  test('replaying the ledger reproduces the balance and version', () => {
    const { wallet, openingEntry } = openWallet('100.00');
    const entries = [openingEntry!];
    entries.push(wallet.debit(brl('10.00'), newId(), t1));
    entries.push(wallet.credit(brl('25.50'), newId(), t1));
    entries.push(wallet.debit(brl('0.01'), newId(), t1));

    let balance = Money.zero('BRL');
    entries.forEach((entry, index) => {
      expect(entry.balanceBefore.equals(balance)).toBe(true);
      expect(entry.walletVersion).toBe(index + 1);
      balance = entry.balanceAfter;
    });
    expect(balance.equals(wallet.balance)).toBe(true);
    expect(wallet.version).toBe(entries.length);
  });
});

describe('Wallet.rehydrate', () => {
  test('restores exactly the persisted state', () => {
    const { wallet } = openWallet('42.00');
    wallet.debit(brl('2'), newId(), t1);

    const restored = Wallet.rehydrate(wallet.toState());

    expect(restored.toState()).toEqual(wallet.toState());
    expect(restored.balance.equals(brl('40'))).toBe(true);
  });

  test('keeps the stored version without replaying transitions', () => {
    const state = { ...openWallet('5').wallet.toState(), version: 17 };
    const restored = Wallet.rehydrate(state);

    restored.credit(brl('1'), newId(), t1);
    expect(restored.version).toBe(18);
  });

  test('rejects a corrupt stored balance', () => {
    const state = { ...openWallet('5').wallet.toState(), balanceAmount: '5.001' };
    expectDomainError(() => Wallet.rehydrate(state), FailureCode.InvalidAmount);
  });
});

describe('WalletLedgerEntry', () => {
  const base = {
    walletId: newId(),
    transactionId: newId(),
    direction: LedgerDirection.Debit,
    money: brl('10'),
    balanceBefore: brl('30'),
    balanceAfter: brl('20'),
    walletVersion: 3,
    createdAt: t0,
  };

  test('create accepts a balanced entry and freezes it', () => {
    const entry = WalletLedgerEntry.create(base);
    expect(entry.isBalanced()).toBe(true);
    expect(Object.isFrozen(entry)).toBe(true);
  });

  test('rejects entries whose balances do not add up', () => {
    expect(() => WalletLedgerEntry.create({ ...base, balanceAfter: brl('21') })).toThrow(RangeError);
    expect(() => WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Credit })).toThrow(RangeError);
  });

  test('rejects zero amounts and negative balances', () => {
    expectDomainError(() => WalletLedgerEntry.create({ ...base, money: brl('0') }), FailureCode.InvalidAmount);
    expectDomainError(
      () =>
        WalletLedgerEntry.create({
          ...base,
          balanceBefore: brl('5'),
          balanceAfter: brl('5').subtract(brl('10')),
        }),
      FailureCode.InsufficientFunds,
    );
  });

  test.each([0, -1, 1.5, Number.NaN])('rejects the wallet version %p', (walletVersion) => {
    expect(() => WalletLedgerEntry.create({ ...base, walletVersion })).toThrow(RangeError);
  });

  test('rejects mixed currencies', () => {
    expectDomainError(
      () => WalletLedgerEntry.create({ ...base, money: Money.from({ amount: '10', currency: 'USD' }) }),
      FailureCode.CurrencyMismatch,
    );
  });

  test('rehydrate restores the persisted state', () => {
    const entry = WalletLedgerEntry.create(base);
    const restored = WalletLedgerEntry.rehydrate(entry.toState());
    expect(restored.toState()).toEqual(entry.toState());
    expect(Object.isFrozen(restored)).toBe(true);
  });
});
