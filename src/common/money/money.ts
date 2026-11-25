import Decimal from 'decimal.js';
import { DomainError } from '../errors/domain.error';
import { FailureCode } from '../errors/failure-code';

export const MONEY_SCALE = 2;
/** numeric(19,2) holds 17 integer digits. */
const MAX_ABSOLUTE = new Decimal('99999999999999999.99');

// Only plain non-negative decimals: no sign, exponent, leading zeros or more than 2 decimals.
const AMOUNT_PATTERN = /^(0|[1-9]\d{0,16})(\.\d{1,2})?$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;

const MoneyDecimal = Decimal.clone({
  precision: 40,
  rounding: Decimal.ROUND_HALF_EVEN,
  toExpNeg: -40,
  toExpPos: 40,
});

export interface MoneyProps {
  amount: string;
  currency: string;
}

export class Money {
  private constructor(
    private readonly value: Decimal,
    readonly currency: string,
  ) {}

  /** Parses a contract value: non-negative decimal string with at most 2 decimals. */
  static from(props: MoneyProps): Money {
    const { amount, currency } = (props ?? {}) as { amount?: unknown; currency?: unknown };
    Money.assertCurrency(currency);
    if (typeof amount !== 'string' || !AMOUNT_PATTERN.test(amount)) {
      throw new DomainError(
        FailureCode.InvalidAmount,
        'Amount must be a non-negative decimal string with at most 2 decimal places',
      );
    }
    return new Money(new MoneyDecimal(amount), currency);
  }

  static zero(currency: string): Money {
    Money.assertCurrency(currency);
    return new Money(new MoneyDecimal(0), currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.bounded(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.bounded(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return this.isZero() ? this : new Money(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.value.equals(other.value);
  }

  toJSON(): MoneyProps {
    return { amount: this.value.toFixed(MONEY_SCALE), currency: this.currency };
  }

  toString(): string {
    return `${this.value.toFixed(MONEY_SCALE)} ${this.currency}`;
  }

  private assertSameCurrency(other: Money): void {
    if (other.currency !== this.currency) {
      throw new DomainError(
        FailureCode.CurrencyMismatch,
        `Cannot combine ${this.currency} with ${other.currency}`,
      );
    }
  }

  private static assertCurrency(currency: unknown): asserts currency is string {
    if (typeof currency !== 'string' || !CURRENCY_PATTERN.test(currency)) {
      throw new DomainError(FailureCode.InvalidCurrency, 'Currency must be an ISO-4217 code');
    }
  }

  private static bounded(value: Decimal, currency: string): Money {
    if (value.abs().greaterThan(MAX_ABSOLUTE)) {
      throw new DomainError(FailureCode.AmountOutOfRange, 'Amount exceeds the supported range');
    }
    return new Money(value, currency);
  }
}
