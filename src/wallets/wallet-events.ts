import { InvalidStateError } from '../common/errors/domain.error';
import { newId } from '../common/ids';
import { type EventContext, IntegrationEvent, type IntegrationEventProps } from '../common/messaging/integration-event';
import type { MoneyProps } from '../common/money/money';
import type { Wallet } from './wallet';
import type { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

/** Only when the balance moves: one event per ledger entry. */
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  private constructor(props: IntegrationEventProps<WalletBalanceChangedData>) {
    super(props);
  }

  /** `entry` must be the movement that brought `wallet` to its current version and balance. */
  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    if (
      entry.walletId !== wallet.id ||
      entry.walletVersion !== wallet.version ||
      !entry.balanceAfter.equals(wallet.balance)
    ) {
      throw new InvalidStateError(`Ledger entry ${entry.id} is not the head of wallet ${wallet.id}`);
    }
    return new WalletBalanceChanged({
      ...ctx,
      eventId: newId(),
      aggregateId: wallet.id,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: entry.walletVersion,
      },
    });
  }
}
