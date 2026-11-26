import { describe, expect, test } from 'bun:test';
import { InvalidStateError } from '../../src/common/errors/domain.error';
import { newId } from '../../src/common/ids';
import { Money } from '../../src/common/money/money';
import { Wallet } from '../../src/wallets/wallet';
import type { WalletLedgerEntry } from '../../src/wallets/wallet-ledger-entry';
import { reconcileWallet } from '../../src/wallets/wallet-reconciliation';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const t0 = new Date('2026-01-01T00:00:00.000Z');

/** A wallet and the entries of the movements applied to it, as the ledger would hold them. */
function walletWithMovements(initialBalance: string, movements: readonly [credit: boolean, amount: string][]) {
  const positive = initialBalance !== '0.00';
  const { wallet, openingEntry } = Wallet.open({
    id: newId(),
    playerId: newId(),
    initialBalance: brl(initialBalance),
    openingTransactionId: positive ? newId() : undefined,
    now: t0,
  });
  const entries: WalletLedgerEntry[] = openingEntry === undefined ? [] : [openingEntry];
  for (const [credit, amount] of movements) {
    entries.push(credit ? wallet.credit(brl(amount), newId(), t0) : wallet.debit(brl(amount), newId(), t0));
  }
  return { wallet, entries };
}

/** The stored wallet with another balance, as a divergence would leave it. */
function withStoredBalance(wallet: Wallet, balanceAmount: string): Wallet {
  return Wallet.rehydrate({ ...wallet.toState(), balanceAmount });
}

describe('reconcileWallet', () => {
  test('rebuilds the balance as 0 + credits - debits and matches the stored balance', () => {
    const { wallet, entries } = walletWithMovements('100.00', [
      [false, '25.10'],
      [true, '0.30'],
      [false, '74.20'],
      [true, '12.34'],
    ]);

    expect(reconcileWallet(wallet, entries)).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '13.34', currency: 'BRL' },
      calculatedBalance: { amount: '13.34', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 5,
    });
  });

  test('is exact where binary floating point is not (0.10 + 0.20)', () => {
    const { wallet, entries } = walletWithMovements('0.00', [
      [true, '0.10'],
      [true, '0.20'],
    ]);
    expect(reconcileWallet(wallet, entries)).toMatchObject({
      calculatedBalance: { amount: '0.30', currency: 'BRL' },
      consistent: true,
    });
  });

  test('a wallet opened with zero balance and no movement has nothing to check', () => {
    const { wallet, entries } = walletWithMovements('0.00', []);
    expect(reconcileWallet(wallet, entries)).toMatchObject({
      calculatedBalance: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 0,
    });
  });

  test.each([
    ['above', '60.00', '10.00'],
    ['below', '45.50', '-4.50'],
  ])('a stored balance %s the ledger is flagged with the signed difference', (_case, stored, difference) => {
    const { wallet, entries } = walletWithMovements('100.00', [[false, '50.00']]);

    expect(reconcileWallet(withStoredBalance(wallet, stored), entries)).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: stored, currency: 'BRL' },
      calculatedBalance: { amount: '50.00', currency: 'BRL' },
      difference: { amount: difference, currency: 'BRL' },
      consistent: false,
      checkedEntries: 2,
    });
  });

  test('a missing entry is a divergence, not an error', () => {
    const { wallet, entries } = walletWithMovements('100.00', [[true, '1.00']]);
    expect(reconcileWallet(wallet, entries.slice(0, 1))).toMatchObject({
      difference: { amount: '1.00', currency: 'BRL' },
      consistent: false,
      checkedEntries: 1,
    });
  });

  test('refuses entries of another wallet', () => {
    const { wallet } = walletWithMovements('10.00', []);
    const other = walletWithMovements('10.00', []);
    expect(() => reconcileWallet(wallet, other.entries)).toThrow(InvalidStateError);
  });
});
