import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { isUuid } from '../common/ids';
import { Money } from '../common/money/money';
import { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balanceAmount: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  /** Required when the initial balance is positive: the internal OPENING transaction. */
  openingTransactionId?: string;
  now: Date;
}

export interface OpenedWallet {
  wallet: Wallet;
  openingEntry: WalletLedgerEntry | undefined;
}

export class Wallet {
  private constructor(
    readonly id: string,
    readonly playerId: string,
    readonly currency: string,
    private _balance: Money,
    private _version: number,
    readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  /**
   * Opens a wallet at version 1. A positive initial balance is credited by the
   * OPENING transaction, so the ledger entry is returned for the same SQL transaction.
   */
  static open(props: OpenWalletProps): OpenedWallet {
    if (!isUuid(props.id) || !isUuid(props.playerId)) {
      throw new DomainError(FailureCode.InvalidPayload, 'Wallet id and playerId must be UUIDs');
    }
    const { initialBalance } = props;
    if (initialBalance.isNegative()) {
      throw new DomainError(FailureCode.InvalidAmount, 'Initial balance cannot be negative');
    }
    const wallet = new Wallet(
      props.id,
      props.playerId,
      initialBalance.currency,
      initialBalance,
      1,
      props.now,
      props.now,
    );
    if (initialBalance.isZero()) {
      if (props.openingTransactionId !== undefined) {
        throw new RangeError('A zero initial balance has no OPENING transaction');
      }
      return { wallet, openingEntry: undefined };
    }
    if (props.openingTransactionId === undefined) {
      throw new RangeError('A positive initial balance requires an OPENING transaction id');
    }
    const openingEntry = WalletLedgerEntry.create({
      walletId: wallet.id,
      transactionId: props.openingTransactionId,
      direction: LedgerDirection.Credit,
      money: initialBalance,
      balanceBefore: Money.zero(initialBalance.currency),
      balanceAfter: initialBalance,
      walletVersion: 1,
      createdAt: props.now,
    });
    return { wallet, openingEntry };
  }

  /** Reconstruction from persistence — does not re-validate transitions. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      Money.from({ amount: state.balanceAmount, currency: state.currency }),
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  canDebit(money: Money): boolean {
    this.assertSameCurrency(money);
    return !this._balance.isLessThan(money);
  }

  /** Throws INSUFFICIENT_FUNDS; callers that need another failure code check canDebit first. */
  debit(money: Money, transactionId: string, now: Date): WalletLedgerEntry {
    if (!this.canDebit(money)) {
      throw new DomainError(
        FailureCode.InsufficientFunds,
        `Balance ${this._balance} cannot cover ${money}`,
      );
    }
    return this.apply(LedgerDirection.Debit, money, transactionId, now);
  }

  credit(money: Money, transactionId: string, now: Date): WalletLedgerEntry {
    this.assertSameCurrency(money);
    return this.apply(LedgerDirection.Credit, money, transactionId, now);
  }

  toState(): WalletState {
    return {
      id: this.id,
      playerId: this.playerId,
      currency: this.currency,
      balanceAmount: this._balance.toJSON().amount,
      version: this._version,
      createdAt: this.createdAt,
      updatedAt: this._updatedAt,
    };
  }

  private apply(
    direction: LedgerDirection,
    money: Money,
    transactionId: string,
    now: Date,
  ): WalletLedgerEntry {
    if (!money.isPositive()) {
      throw new DomainError(FailureCode.InvalidAmount, 'Balance movements must be positive');
    }
    const balanceBefore = this._balance;
    const balanceAfter =
      direction === LedgerDirection.Credit ? balanceBefore.add(money) : balanceBefore.subtract(money);
    // Build the entry first: if it is rejected, the wallet must stay untouched.
    const entry = WalletLedgerEntry.create({
      walletId: this.id,
      transactionId,
      direction,
      money,
      balanceBefore,
      balanceAfter,
      walletVersion: this._version + 1,
      createdAt: now,
    });
    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = now;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new DomainError(
        FailureCode.CurrencyMismatch,
        `Wallet ${this.id} holds ${this.currency}, not ${money.currency}`,
      );
    }
  }
}
