import { describe, expect, test } from 'bun:test';
import { FailureCode } from '../../src/common/errors/failure-code';
import { Money } from '../../src/common/money/money';
import { expectDomainError } from '../support/domain';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Money.from', () => {
  test.each([
    ['25.00', '25.00'],
    ['25', '25.00'],
    ['25.5', '25.50'],
    ['0', '0.00'],
    ['0.01', '0.01'],
    ['99999999999999999.99', '99999999999999999.99'],
  ])('accepts %p and serializes it with scale 2 as %p', (input, serialized) => {
    expect(brl(input).toJSON()).toEqual({ amount: serialized, currency: 'BRL' });
  });

  test.each([
    'NaN',
    'Infinity',
    '-1.00',
    '-0',
    '+1.00',
    '1e3',
    '1E3',
    '',
    ' 1.00',
    '1.00 ',
    '1.001',
    '01.00',
    '.50',
    '1.',
    '1,00',
    '0x10',
    '100000000000000000.00',
  ])('rejects the amount %p', (amount) => {
    expectDomainError(() => brl(amount), FailureCode.InvalidAmount);
  });

  test.each([[25], [25.5], [0.1], [null], [undefined], [{}], [['1.00']]])('rejects the non-string amount %p', (amount) => {
    expectDomainError(
      () => Money.from({ amount: amount as unknown as string, currency: 'BRL' }),
      FailureCode.InvalidAmount,
    );
  });

  test.each(['brl', 'BR', 'BRLX', '', 'B1L', 123, undefined])('rejects the currency %p', (currency) => {
    expectDomainError(
      () => Money.from({ amount: '1.00', currency: currency as unknown as string }),
      FailureCode.InvalidCurrency,
    );
  });

  test('Money.zero validates the currency too', () => {
    expect(Money.zero('USD').toJSON()).toEqual({ amount: '0.00', currency: 'USD' });
    expectDomainError(() => Money.zero('usd'), FailureCode.InvalidCurrency);
  });
});

describe('Money arithmetic', () => {
  test('is exact where binary floating point is not', () => {
    expect(0.1 + 0.2).not.toBe(0.3);
    expect(brl('0.10').add(brl('0.20')).equals(brl('0.30'))).toBe(true);
  });

  test('keeps cents exact across many operations', () => {
    let total = Money.zero('BRL');
    for (let i = 0; i < 10_000; i++) {
      total = total.add(brl('0.01'));
    }
    expect(total.toJSON().amount).toBe('100.00');
  });

  test('keeps full precision at the top of the numeric(19,2) range', () => {
    const max = brl('99999999999999999.99');
    expect(max.subtract(brl('0.01')).toJSON().amount).toBe('99999999999999999.98');
    expect(brl('99999999999999999.98').add(brl('0.01')).equals(max)).toBe(true);
  });

  test('rejects results outside the supported range', () => {
    expectDomainError(() => brl('99999999999999999.99').add(brl('0.01')), FailureCode.AmountOutOfRange);
  });

  test('subtraction can go negative, and negatives serialize with scale 2', () => {
    const result = brl('10.00').subtract(brl('15.50'));
    expect(result.isNegative()).toBe(true);
    expect(result.toJSON().amount).toBe('-5.50');
  });

  test('negate never produces negative zero', () => {
    expect(brl('0').negate().toJSON().amount).toBe('0.00');
    expect(brl('3.25').negate().toJSON().amount).toBe('-3.25');
  });

  test('is immutable', () => {
    const a = brl('10.00');
    const sum = a.add(brl('5.00'));
    expect(sum).not.toBe(a);
    expect(a.toJSON().amount).toBe('10.00');
  });
});

describe('Money comparisons and currency', () => {
  test('sign predicates', () => {
    expect(brl('0').isZero()).toBe(true);
    expect(brl('0.00').isPositive()).toBe(false);
    expect(brl('0.01').isPositive()).toBe(true);
    expect(brl('0.01').isNegative()).toBe(false);
  });

  test('isLessThan compares by value', () => {
    expect(brl('9.99').isLessThan(brl('10'))).toBe(true);
    expect(brl('10.00').isLessThan(brl('10'))).toBe(false);
  });

  test('equals ignores representation and respects currency', () => {
    expect(brl('10').equals(brl('10.00'))).toBe(true);
    expect(brl('10').equals(Money.from({ amount: '10', currency: 'USD' }))).toBe(false);
  });

  test('refuses to combine different currencies', () => {
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    expectDomainError(() => brl('1.00').add(usd), FailureCode.CurrencyMismatch);
    expectDomainError(() => brl('1.00').subtract(usd), FailureCode.CurrencyMismatch);
    expectDomainError(() => brl('1.00').isLessThan(usd), FailureCode.CurrencyMismatch);
  });

  test('toString and JSON.stringify', () => {
    expect(String(brl('25'))).toBe('25.00 BRL');
    expect(JSON.stringify({ money: brl('25') })).toBe('{"money":{"amount":"25.00","currency":"BRL"}}');
  });
});
