import { describe, expect, test } from 'bun:test';
import { InvalidTransactionStateError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { canonicalHash } from '../../src/common/hashing/canonical-hash';
import { newId } from '../../src/common/ids';
import { Money } from '../../src/common/money/money';
import {
  type CreateWagerTransactionProps,
  INTERNAL_PROVIDER_ID,
  WagerTransaction,
  WagerTransactionKind as Kind,
  WagerTransactionStatus as Status,
} from '../../src/wagering/wager-transaction';
import { LedgerDirection } from '../../src/wallets/wallet-ledger-entry';
import { expectDomainError } from '../support/domain';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const t0 = new Date('2026-01-01T00:00:00.000Z');
const t1 = new Date('2026-01-01T00:00:05.000Z');
const scope = { walletId: newId(), playerId: newId() };

let sequence = 0;
function props(overrides: Partial<CreateWagerTransactionProps> = {}): CreateWagerTransactionProps {
  sequence += 1;
  return {
    id: newId(),
    providerId: 'provider-a',
    externalTransactionId: `ext-${sequence}`,
    idempotencyKey: `idem-${sequence}`,
    payloadHash: canonicalHash({ sequence }),
    ...scope,
    roundId: 'round-1',
    gameId: 'game-1',
    kind: Kind.Bet,
    money: brl('10.00'),
    now: t0,
    ...overrides,
  };
}

function processed(overrides: Partial<CreateWagerTransactionProps> = {}): WagerTransaction {
  const transaction = WagerTransaction.create(props(overrides));
  transaction.markProcessed(undefined, brl('90'), t0);
  return transaction;
}

function referencing(reference: WagerTransaction, kind: Kind, overrides: Partial<CreateWagerTransactionProps> = {}) {
  return WagerTransaction.create(
    props({ kind, money: reference.money, referenceExternalTransactionId: reference.externalTransactionId, ...overrides }),
  );
}

describe('WagerTransaction.create', () => {
  test('starts PENDING without an outcome', () => {
    const transaction = WagerTransaction.create(props());
    expect(transaction.status).toBe(Status.Pending);
    expect(transaction.isTerminal()).toBe(false);
    expect(transaction.attemptCount).toBe(0);
    expect(transaction.observedBalance).toBeUndefined();
    expect(transaction.failureCode).toBeUndefined();
  });

  test('cannot be persisted while PENDING', () => {
    expect(() => WagerTransaction.create(props()).toState()).toThrow(InvalidTransactionStateError);
  });

  test('OPENING and unknown kinds are not accepted from providers', () => {
    expectDomainError(() => WagerTransaction.create(props({ kind: Kind.Opening })), FailureCode.UnsupportedKind);
    expectDomainError(
      () => WagerTransaction.create(props({ kind: 'CASHOUT' as Kind })),
      FailureCode.UnsupportedKind,
    );
  });

  test('the internal provider id is reserved', () => {
    expectDomainError(
      () => WagerTransaction.create(props({ providerId: INTERNAL_PROVIDER_ID })),
      FailureCode.InvalidPayload,
    );
  });

  test.each([
    ['providerId', ''],
    ['providerId', 'p'.repeat(101)],
    ['externalTransactionId', '   '],
    ['externalTransactionId', 'e'.repeat(201)],
    ['idempotencyKey', 'k'.repeat(256)],
    ['roundId', ''],
    ['gameId', 'g'.repeat(201)],
    ['id', 'not-a-uuid'],
    ['walletId', 'wallet-1'],
    ['playerId', 'player-1'],
    ['payloadHash', 'A'.repeat(64)],
    ['payloadHash', 'abc'],
  ])('rejects an invalid %s (%p)', (field, value) => {
    expectDomainError(() => WagerTransaction.create(props({ [field]: value })), FailureCode.InvalidPayload);
  });

  test.each([Kind.Bet, Kind.Win, Kind.Refund, Kind.Rollback])('a zero %s is rejected', (kind) => {
    const reference = [Kind.Refund, Kind.Rollback].includes(kind) ? { referenceExternalTransactionId: 'ext-x' } : {};
    expectDomainError(
      () => WagerTransaction.create(props({ kind, money: brl('0'), ...reference })),
      FailureCode.InvalidAmount,
    );
  });

  test('a zero LOSS is accepted', () => {
    expect(WagerTransaction.create(props({ kind: Kind.Loss, money: brl('0') })).status).toBe(Status.Pending);
  });

  test.each([Kind.Refund, Kind.Rollback])('%s requires a reference', (kind) => {
    expectDomainError(() => WagerTransaction.create(props({ kind })), FailureCode.ReferenceRequired);
  });

  test('WIN and LOSS may reference a BET, BET may not reference anything', () => {
    expect(WagerTransaction.create(props({ kind: Kind.Win, referenceExternalTransactionId: 'b' })).kind).toBe(
      Kind.Win,
    );
    expect(WagerTransaction.create(props({ kind: Kind.Loss })).kind).toBe(Kind.Loss);
    expectDomainError(
      () => WagerTransaction.create(props({ kind: Kind.Bet, referenceExternalTransactionId: 'b' })),
      FailureCode.ReferenceNotAllowed,
    );
  });

  test('a transaction cannot reference itself', () => {
    expectDomainError(
      () =>
        WagerTransaction.create(
          props({ kind: Kind.Refund, externalTransactionId: 'same', referenceExternalTransactionId: 'same' }),
        ),
      FailureCode.ReferenceNotAllowed,
    );
  });
});

describe('WagerTransaction.opening', () => {
  test('is internal, already PROCESSED and keyed by wallet', () => {
    const opening = WagerTransaction.opening({ id: newId(), ...scope, money: brl('100'), now: t0 });
    expect(opening.toState()).toMatchObject({
      providerId: INTERNAL_PROVIDER_ID,
      externalTransactionId: `opening:${scope.walletId}`,
      idempotencyKey: `opening:${scope.walletId}`,
      kind: Kind.Opening,
      status: Status.Processed,
      amount: '100.00',
      observedBalanceAmount: '100.00',
      processedAt: t0,
    });
    expect(opening.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
  });

  test('has a deterministic payload hash', () => {
    const a = WagerTransaction.opening({ id: newId(), ...scope, money: brl('100'), now: t0 });
    const b = WagerTransaction.opening({ id: newId(), ...scope, money: brl('100.00'), now: t1 });
    expect(a.payloadHash).toBe(b.payloadHash);
  });

  test('requires a positive amount', () => {
    expectDomainError(
      () => WagerTransaction.opening({ id: newId(), ...scope, money: brl('0'), now: t0 }),
      FailureCode.InvalidAmount,
    );
  });
});

describe('WagerTransaction transitions', () => {
  test('PENDING -> PROCESSED records the outcome', () => {
    const transaction = WagerTransaction.create(props());
    transaction.markProcessed(undefined, brl('90'), t1);
    expect(transaction.toState()).toMatchObject({
      status: Status.Processed,
      observedBalanceAmount: '90.00',
      processedAt: t1,
      nextAttemptAt: null,
      failureCode: null,
    });
  });

  test('PENDING -> PENDING_REFERENCE -> PENDING_REFERENCE -> PROCESSED', () => {
    const referenceId = newId();
    const refund = WagerTransaction.create(props({ kind: Kind.Refund, referenceExternalTransactionId: 'bet-1' }));

    refund.markPendingReference(t1);
    expect(refund.toState()).toMatchObject({ status: Status.PendingReference, attemptCount: 1, nextAttemptAt: t1 });
    refund.markPendingReference(t1);
    expect(refund.attemptCount).toBe(2);

    refund.markProcessed(referenceId, brl('110'), t1);
    expect(refund.toState()).toMatchObject({
      status: Status.Processed,
      referenceTransactionId: referenceId,
      nextAttemptAt: null,
      attemptCount: 2,
    });
  });

  test('PENDING_REFERENCE -> REJECTED', () => {
    const rollback = WagerTransaction.create(props({ kind: Kind.Rollback, referenceExternalTransactionId: 'x' }));
    rollback.markPendingReference(t1);
    rollback.reject(FailureCode.ReferenceNotFound, brl('90'));
    expect(rollback.toState()).toMatchObject({
      status: Status.Rejected,
      failureCode: FailureCode.ReferenceNotFound,
      observedBalanceAmount: '90.00',
      nextAttemptAt: null,
      processedAt: null,
    });
  });

  test('PENDING -> FAILED keeps no observed balance', () => {
    const transaction = WagerTransaction.create(props());
    transaction.fail(FailureCode.PermanentIntegrityError);
    expect(transaction.toState()).toMatchObject({
      status: Status.Failed,
      failureCode: FailureCode.PermanentIntegrityError,
      observedBalanceAmount: null,
    });
  });

  test('a reference must be resolved before processing, and only when there is one', () => {
    const refund = WagerTransaction.create(props({ kind: Kind.Refund, referenceExternalTransactionId: 'bet-1' }));
    expect(() => refund.markProcessed(undefined, brl('1'), t1)).toThrow(InvalidTransactionStateError);

    const bet = WagerTransaction.create(props());
    expect(() => bet.markProcessed(newId(), brl('1'), t1)).toThrow(InvalidTransactionStateError);
    expect(() => bet.markPendingReference(t1)).toThrow(InvalidTransactionStateError);
  });

  test('the observed balance must be in the transaction currency', () => {
    const usd = Money.from({ amount: '1', currency: 'USD' });
    expectDomainError(
      () => WagerTransaction.create(props()).markProcessed(undefined, usd, t1),
      FailureCode.CurrencyMismatch,
    );
    expectDomainError(
      () => WagerTransaction.create(props()).reject(FailureCode.InsufficientFunds, usd),
      FailureCode.CurrencyMismatch,
    );
  });

  const terminal: Array<[Status, () => WagerTransaction]> = [
    [Status.Processed, () => processed()],
    [
      Status.Rejected,
      () => {
        const transaction = WagerTransaction.create(props());
        transaction.reject(FailureCode.InsufficientFunds, brl('5'));
        return transaction;
      },
    ],
    [
      Status.Failed,
      () => {
        const transaction = WagerTransaction.create(props());
        transaction.fail(FailureCode.PermanentIntegrityError);
        return transaction;
      },
    ],
  ];
  const transitions: Array<[string, (transaction: WagerTransaction) => void]> = [
    ['markProcessed', (transaction) => transaction.markProcessed(undefined, brl('1'), t1)],
    ['markPendingReference', (transaction) => transaction.markPendingReference(t1)],
    ['reject', (transaction) => transaction.reject(FailureCode.InsufficientFunds, brl('1'))],
    ['fail', (transaction) => transaction.fail(FailureCode.PermanentIntegrityError)],
  ];

  for (const [status, build] of terminal) {
    for (const [name, transition] of transitions) {
      test(`${status} is terminal: ${name} throws and changes nothing`, () => {
        const transaction = build();
        const before = transaction.toState();
        expect(() => transition(transaction)).toThrow(InvalidTransactionStateError);
        expect(transaction.toState()).toEqual(before);
      });
    }
  }
});

describe('WagerTransaction domain queries', () => {
  test('only LOSS leaves the balance alone', () => {
    expect(WagerTransaction.create(props({ kind: Kind.Loss })).affectsBalance()).toBe(false);
    expect(WagerTransaction.create(props()).affectsBalance()).toBe(true);
  });

  test('matchesPayload compares the payload hash', () => {
    const transaction = WagerTransaction.create(props({ payloadHash: canonicalHash({ a: 1 }) }));
    expect(transaction.matchesPayload(canonicalHash({ a: 1 }))).toBe(true);
    expect(transaction.matchesPayload(canonicalHash({ a: 2 }))).toBe(false);
  });

  test('ledger direction per kind', () => {
    const bet = processed();
    const win = processed({ kind: Kind.Win });
    const refund = referencing(bet, Kind.Refund);

    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(win.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(refund.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(referencing(bet, Kind.Rollback).ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
    expect(referencing(win, Kind.Rollback).ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    expect(referencing(refund, Kind.Rollback).ledgerDirectionFor(refund)).toBe(LedgerDirection.Debit);
  });

  test('LOSS and an unresolved ROLLBACK have no direction', () => {
    expect(() => WagerTransaction.create(props({ kind: Kind.Loss })).ledgerDirectionFor()).toThrow(
      InvalidTransactionStateError,
    );
    expect(() => referencing(processed(), Kind.Rollback).ledgerDirectionFor()).toThrow(
      InvalidTransactionStateError,
    );
  });
});

describe('WagerTransaction.referenceViolation', () => {
  test('a processed BET backs a REFUND, ROLLBACK, WIN and LOSS', () => {
    const bet = processed();
    expect(referencing(bet, Kind.Refund).referenceViolation(bet)).toBeUndefined();
    expect(referencing(bet, Kind.Rollback).referenceViolation(bet)).toBeUndefined();
    expect(referencing(bet, Kind.Win, { money: brl('35') }).referenceViolation(bet)).toBeUndefined();
    expect(referencing(bet, Kind.Loss, { money: brl('0') }).referenceViolation(bet)).toBeUndefined();
  });

  test('ROLLBACK may reverse a WIN or a REFUND; REFUND may not reverse a WIN', () => {
    const win = processed({ kind: Kind.Win });
    expect(referencing(win, Kind.Rollback).referenceViolation(win)).toBeUndefined();
    expect(referencing(win, Kind.Refund).referenceViolation(win)).toBe(FailureCode.ReferenceKindNotAllowed);
    expect(referencing(win, Kind.Win).referenceViolation(win)).toBe(FailureCode.ReferenceKindNotAllowed);
  });

  test('the reference must be PROCESSED', () => {
    const rejected = WagerTransaction.create(props());
    rejected.reject(FailureCode.InsufficientFunds, brl('0'));
    expect(referencing(rejected, Kind.Refund).referenceViolation(rejected)).toBe(FailureCode.ReferenceNotProcessed);
  });

  test.each([
    ['walletId', { walletId: newId() }],
    ['playerId', { playerId: newId() }],
    ['roundId', { roundId: 'round-2' }],
    ['currency', { money: Money.from({ amount: '10.00', currency: 'USD' }) }],
  ])('a different %s is a scope mismatch', (_field, overrides) => {
    const bet = processed();
    expect(referencing(bet, Kind.Refund, overrides).referenceViolation(bet)).toBe(FailureCode.ReferenceScopeMismatch);
  });

  test('REFUND and ROLLBACK must reverse the exact amount', () => {
    const bet = processed();
    expect(referencing(bet, Kind.Refund, { money: brl('9.99') }).referenceViolation(bet)).toBe(
      FailureCode.AmountMismatch,
    );
    expect(referencing(bet, Kind.Rollback, { money: brl('10.01') }).referenceViolation(bet)).toBe(
      FailureCode.AmountMismatch,
    );
  });

  test('passing a transaction that is not the reference is a bug', () => {
    const bet = processed();
    const other = processed();
    expect(() => referencing(bet, Kind.Refund).referenceViolation(other)).toThrow(InvalidTransactionStateError);
    expect(() =>
      referencing(bet, Kind.Refund).referenceViolation(processed({ providerId: 'provider-b', externalTransactionId: bet.externalTransactionId })),
    ).toThrow(InvalidTransactionStateError);
  });
});

describe('WagerTransaction.rehydrate', () => {
  test('restores every persisted field', () => {
    const bet = processed();
    const refund = referencing(bet, Kind.Refund);
    refund.markPendingReference(t1);
    refund.markProcessed(bet.id, brl('100'), t1);

    for (const transaction of [bet, refund]) {
      expect(WagerTransaction.rehydrate(transaction.toState()).toState()).toEqual(transaction.toState());
    }
  });

  test('a rehydrated terminal transaction stays terminal', () => {
    const restored = WagerTransaction.rehydrate(processed().toState());
    expect(restored.isTerminal()).toBe(true);
    expect(() => restored.fail(FailureCode.PermanentIntegrityError)).toThrow(InvalidTransactionStateError);
  });

  test('a rehydrated PENDING_REFERENCE transaction continues from its attempt count', () => {
    const rollback = WagerTransaction.create(props({ kind: Kind.Rollback, referenceExternalTransactionId: 'x' }));
    rollback.markPendingReference(t1);
    const restored = WagerTransaction.rehydrate({ ...rollback.toState(), attemptCount: 4 });

    restored.markPendingReference(t1);
    expect(restored.attemptCount).toBe(5);
  });
});
