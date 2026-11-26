import { describe, expect, test } from 'bun:test';
import { FailureCode } from '../../src/common/errors/failure-code';
import { PAYLOAD_HASH_PATTERN } from '../../src/common/hashing/canonical-hash';
import { newId } from '../../src/common/ids';
import { Money } from '../../src/common/money/money';
import {
  nextPendingReferenceAttemptAt,
  parseIdempotencyKey,
  parseWagerRequest,
  wagerPayloadHash,
} from '../../src/wagering/wager-payload';
import { WagerTransactionKind } from '../../src/wagering/wager-transaction';
import { expectDomainError } from '../support/domain';

const walletId = newId();
const playerId = newId();

function validBody(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    providerId: 'provider-a',
    externalTransactionId: 'tx-1',
    playerId,
    walletId,
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: { amount: '25.00', currency: 'BRL' },
    ...overrides,
  };
}

describe('wagerPayloadHash & parseWagerRequest', () => {
  test('produces a 64-char SHA-256 hex digest independent of key order and transport fields', () => {
    const first = parseWagerRequest(validBody({ extraTransportField: 'ignored' }), 'idem-1');
    const reordered = parseWagerRequest(
      {
        money: { currency: 'BRL', amount: '25' },
        kind: 'BET',
        gameId: 'fortune-chimp',
        roundId: 'round-1',
        walletId,
        playerId,
        externalTransactionId: 'tx-1',
        providerId: 'provider-a',
        referenceExternalTransactionId: null,
      },
      'idem-2',
    );

    expect(first.payloadHash).toMatch(PAYLOAD_HASH_PATTERN);
    expect(reordered.payloadHash).toBe(first.payloadHash);
    expect(first.referenceExternalTransactionId).toBeUndefined();
    expect(reordered.referenceExternalTransactionId).toBeUndefined();
  });

  test.each<[string, Record<string, unknown>]>([
    ['providerId', { providerId: 'provider-b' }],
    ['externalTransactionId', { externalTransactionId: 'tx-2' }],
    ['playerId', { playerId: newId() }],
    ['walletId', { walletId: newId() }],
    ['roundId', { roundId: 'round-2' }],
    ['gameId', { gameId: 'other-game' }],
    ['kind', { kind: 'WIN' }],
    ['amount', { money: { amount: '25.01', currency: 'BRL' } }],
    ['currency', { money: { amount: '25.00', currency: 'USD' } }],
    ['referenceExternalTransactionId', { kind: 'WIN', referenceExternalTransactionId: 'bet-1' }],
  ])('changing business field %s changes payloadHash', (_field, diff) => {
    const base = parseWagerRequest(validBody(), 'idem-1');
    const changed = parseWagerRequest(validBody(diff), 'idem-1');
    expect(changed.payloadHash).not.toBe(base.payloadHash);
  });

  test('wagerPayloadHash matches parseWagerRequest hash', () => {
    const parsed = parseWagerRequest(validBody({ kind: 'REFUND', referenceExternalTransactionId: 'bet-0' }), 'idem-1');
    expect(
      wagerPayloadHash({
        providerId: 'provider-a',
        externalTransactionId: 'tx-1',
        playerId,
        walletId,
        roundId: 'round-1',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Refund,
        money: Money.from({ amount: '25.00', currency: 'BRL' }),
        referenceExternalTransactionId: 'bet-0',
      }),
    ).toBe(parsed.payloadHash);
  });

  test.each<[string, unknown]>([
    ['undefined', undefined],
    ['empty', ''],
    ['whitespace', '   '],
    ['too long', 'k'.repeat(256)],
    ['repeated header array', ['a', 'b']],
  ])('rejects invalid Idempotency-Key (%s)', (_label, key) => {
    expectDomainError(() => parseIdempotencyKey(key), FailureCode.InvalidPayload);
  });

  test.each<[string, unknown, FailureCode]>([
    ['null body', null, FailureCode.InvalidPayload],
    ['array body', [], FailureCode.InvalidPayload],
    ['missing providerId', validBody({ providerId: undefined }), FailureCode.InvalidPayload],
    ['numeric externalTransactionId', validBody({ externalTransactionId: 123 }), FailureCode.InvalidPayload],
    ['missing money', validBody({ money: undefined }), FailureCode.InvalidPayload],
    ['array money', validBody({ money: [] }), FailureCode.InvalidPayload],
    ['number amount', validBody({ money: { amount: 25, currency: 'BRL' } }), FailureCode.InvalidAmount],
    ['lowercase currency', validBody({ money: { amount: '25.00', currency: 'brl' } }), FailureCode.InvalidCurrency],
    ['non-string reference', validBody({ referenceExternalTransactionId: 42 }), FailureCode.InvalidPayload],
  ])('rejects %s with %s', (_label, body, code) => {
    expectDomainError(() => parseWagerRequest(body, 'idem-1'), code);
  });
});

describe('nextPendingReferenceAttemptAt', () => {
  test('uses capped exponential backoff starting at 1s up to 15s', () => {
    const t0 = new Date('2026-01-01T00:00:00.000Z');
    const delays = [1, 2, 3, 4, 5, 6].map((n) => nextPendingReferenceAttemptAt(t0, n).getTime() - t0.getTime());
    expect(delays).toEqual([1_000, 2_000, 4_000, 8_000, 15_000, 15_000]);
  });

  test('rejects non-positive attempt numbers', () => {
    const t0 = new Date('2026-01-01T00:00:00.000Z');
    expect(() => nextPendingReferenceAttemptAt(t0, 0)).toThrow(RangeError);
    expect(() => nextPendingReferenceAttemptAt(t0, -1)).toThrow(RangeError);
  });
});
