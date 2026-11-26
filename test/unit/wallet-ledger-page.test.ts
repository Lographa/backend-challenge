import { describe, expect, test } from 'bun:test';
import { FailureCode } from '../../src/common/errors/failure-code';
import {
  DEFAULT_LEDGER_PAGE_SIZE,
  decodeLedgerCursor,
  encodeLedgerCursor,
  MAX_LEDGER_PAGE_SIZE,
  parseLedgerLimit,
} from '../../src/wallets/ledger-page';
import { expectDomainError } from '../support/domain';

const base64url = (text: string) => Buffer.from(text).toString('base64url');

describe('ledger cursor', () => {
  test.each([1, 2, 50, 2_147_483_647])('round-trips wallet version %d', (version) => {
    const cursor = encodeLedgerCursor(version);
    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(decodeLedgerCursor(cursor)).toBe(version);
  });

  test('does not expose the version as a plain number', () => {
    expect(encodeLedgerCursor(42)).not.toContain('42');
  });

  test.each<[string, unknown]>([
    ['empty', ''],
    ['not base64url', 'abc+/='],
    ['not JSON', base64url('nope')],
    ['JSON without v', base64url('{"x":1}')],
    ['JSON null', base64url('null')],
    ['a bare number', base64url('5')],
    ['v as a string', base64url('{"v":"5"}')],
    ['v fractional', base64url('{"v":1.5}')],
    ['v zero', base64url('{"v":0}')],
    ['v negative', base64url('{"v":-1}')],
    ['v beyond the integer column', base64url('{"v":2147483648}')],
    ['extra keys', base64url('{"v":5,"walletId":"x"}')],
    ['other spacing', base64url('{ "v": 5 }')],
    ['too long', 'a'.repeat(65)],
    ['not a string', 5],
    ['repeated query parameter', ['a', 'b']],
  ])('rejects %s with INVALID_PAYLOAD', (_case, cursor) => {
    expectDomainError(() => decodeLedgerCursor(cursor), FailureCode.InvalidPayload);
  });
});

describe('ledger page limit', () => {
  test('defaults to 50 and accepts 1 to 200', () => {
    expect(parseLedgerLimit(undefined)).toBe(DEFAULT_LEDGER_PAGE_SIZE);
    expect(DEFAULT_LEDGER_PAGE_SIZE).toBe(50);
    expect(parseLedgerLimit('1')).toBe(1);
    expect(parseLedgerLimit('200')).toBe(MAX_LEDGER_PAGE_SIZE);
  });

  test.each<[string, unknown]>([
    ['zero', '0'],
    ['above the maximum', '201'],
    ['negative', '-1'],
    ['fractional', '1.5'],
    ['leading zero', '010'],
    ['exponent', '1e2'],
    ['blank', ''],
    ['text', 'all'],
    ['repeated query parameter', ['10', '20']],
  ])('rejects %s with INVALID_PAYLOAD', (_case, limit) => {
    expectDomainError(() => parseLedgerLimit(limit), FailureCode.InvalidPayload);
  });
});
