import { InvalidStateError } from '../common/errors/domain.error';
import { Money, type MoneyProps } from '../common/money/money';
import type { Wallet } from './wallet';
import { LedgerDirection, type WalletLedgerEntry } from './wallet-ledger-entry';

export interface WalletReconciliation {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  /** storedBalance - calculatedBalance */
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

/** Rebuilds the balance from the ledger (0 + credits - debits) and compares it with the stored one. */
export function reconcileWallet(wallet: Wallet, entries: readonly WalletLedgerEntry[]): WalletReconciliation {
  let calculated = Money.zero(wallet.currency);
  for (const entry of entries) {
    if (entry.walletId !== wallet.id) {
      throw new InvalidStateError(`Ledger entry ${entry.id} does not belong to wallet ${wallet.id}`);
    }
    calculated =
      entry.direction === LedgerDirection.Credit ? calculated.add(entry.money) : calculated.subtract(entry.money);
  }
  const difference = wallet.balance.subtract(calculated);
  return {
    walletId: wallet.id,
    storedBalance: wallet.balance.toJSON(),
    calculatedBalance: calculated.toJSON(),
    difference: difference.toJSON(),
    consistent: difference.isZero(),
    checkedEntries: entries.length,
  };
}
