import { describe, expect, test } from 'bun:test';
import { AppConfig, InvalidEnvironmentError } from '../../src/common/config/env';
import { DomainError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { newId } from '../../src/common/ids';
import {
  deriveDlqRouting,
  parseWagerMessage,
  toWagerTransactionSendInput,
  WAGER_REQUESTED_MESSAGE_TYPE,
  type WagerTransactionRequestedEnvelope,
} from '../../src/messaging/wager-message';
import { parseWagerRequest } from '../../src/wagering/wager-payload';
import { WagerTransactionKind } from '../../src/wagering/wager-transaction';

const walletId = newId();
const playerId = newId();

function buildEnvelope(
  overrides: Partial<Record<string, unknown>> = {},
  dataOverrides: Partial<Record<string, unknown>> = {},
): Record<string, unknown> {
  return {
    messageId: 'msg-123',
    type: WAGER_REQUESTED_MESSAGE_TYPE,
    occurredAt: '2026-07-29T15:00:00.000Z',
    data: {
      providerId: 'provider-a',
      externalTransactionId: 'transaction-123',
      idempotencyKey: 'provider-a:transaction-123',
      playerId,
      walletId,
      roundId: 'round-987',
      gameId: 'fortune-chimp',
      kind: WagerTransactionKind.Bet,
      money: { amount: '25.00', currency: 'BRL' },
      ...dataOverrides,
    },
    ...overrides,
  };
}

function expectDomainFailure(fn: () => unknown, expected: FailureCode): void {
  let caught: unknown;
  try {
    fn();
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(DomainError);
  expect((caught as DomainError).code).toBe(expected);
}

describe('parseWagerMessage', () => {
  test('parses a valid WagerTransactionRequested envelope and matches HTTP payloadHash', () => {
    const raw = JSON.stringify(buildEnvelope({}, { money: { amount: '25', currency: 'BRL' } }));
    const parsed = parseWagerMessage(raw, walletId);

    const httpEquivalent = parseWagerRequest(
      {
        providerId: 'provider-a',
        externalTransactionId: 'transaction-123',
        playerId,
        walletId,
        roundId: 'round-987',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '25.00', currency: 'BRL' },
      },
      'provider-a:transaction-123',
    );

    expect(parsed.messageId).toBe('msg-123');
    expect(parsed.command.payloadHash).toBe(httpEquivalent.payloadHash);
    expect(parsed.command.money.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(parsed.command.correlationId).toBe('msg-123');
    expect(parsed.command.causationId).toBe('msg-123');
    expect(parsed.inboxPayloadHash).toMatch(/^[0-9a-f]{64}$/);
  });

  test('inboxPayloadHash ignores transport timestamps and correlationId but changes with business fields or idempotencyKey', () => {
    const first = parseWagerMessage(
      JSON.stringify(buildEnvelope({ occurredAt: '2026-07-29T15:00:00.000Z', correlationId: 'corr-a' })),
    );
    const redelivered = parseWagerMessage(
      JSON.stringify(
        buildEnvelope(
          { occurredAt: '2026-07-29T15:05:00.000Z', correlationId: 'corr-b' },
          { money: { amount: '25', currency: 'BRL' } },
        ),
      ),
    );
    const mutatedAmount = parseWagerMessage(
      JSON.stringify(buildEnvelope({}, { money: { amount: '30.00', currency: 'BRL' } })),
    );
    const mutatedKey = parseWagerMessage(
      JSON.stringify(buildEnvelope({}, { idempotencyKey: 'provider-a:other-key' })),
    );

    expect(first.inboxPayloadHash).toBe(redelivered.inboxPayloadHash);
    expect(first.inboxPayloadHash).not.toBe(mutatedAmount.inboxPayloadHash);
    expect(first.inboxPayloadHash).not.toBe(mutatedKey.inboxPayloadHash);
  });

  test.each<[string, string | undefined, string | undefined, FailureCode]>([
    ['empty body', '', undefined, FailureCode.InvalidPayload],
    ['undefined body', undefined, undefined, FailureCode.InvalidPayload],
    ['malformed JSON', '{bad-json', undefined, FailureCode.InvalidPayload],
    ['array JSON', '[]', undefined, FailureCode.InvalidPayload],
    ['blank messageId', JSON.stringify(buildEnvelope({ messageId: '   ' })), undefined, FailureCode.InvalidPayload],
    [
      'overlong messageId',
      JSON.stringify(buildEnvelope({ messageId: 'm'.repeat(256) })),
      undefined,
      FailureCode.InvalidPayload,
    ],
    ['wrong message type', JSON.stringify(buildEnvelope({ type: 'OtherEvent' })), undefined, FailureCode.InvalidPayload],
    [
      'non-ISO occurredAt',
      JSON.stringify(buildEnvelope({ occurredAt: 'July 29, 2026' })),
      undefined,
      FailureCode.InvalidPayload,
    ],
    [
      'impossible calendar date',
      JSON.stringify(buildEnvelope({ occurredAt: '2026-02-30T10:00:00.000Z' })),
      undefined,
      FailureCode.InvalidPayload,
    ],
    ['missing data object', JSON.stringify(buildEnvelope({ data: null })), undefined, FailureCode.InvalidPayload],
    [
      'missing idempotencyKey',
      JSON.stringify(buildEnvelope({}, { idempotencyKey: '' })),
      undefined,
      FailureCode.InvalidPayload,
    ],
    [
      'malformed correlationId',
      JSON.stringify(buildEnvelope({ correlationId: 'bad space!' })),
      undefined,
      FailureCode.InvalidPayload,
    ],
    [
      'OPENING kind from external producer',
      JSON.stringify(buildEnvelope({}, { kind: 'OPENING' })),
      undefined,
      FailureCode.UnsupportedKind,
    ],
    [
      'lowercase currency',
      JSON.stringify(buildEnvelope({}, { money: { amount: '25.00', currency: 'brl' } })),
      undefined,
      FailureCode.InvalidCurrency,
    ],
    [
      'zero BET amount',
      JSON.stringify(buildEnvelope({}, { kind: 'BET', money: { amount: '0.00', currency: 'BRL' } })),
      undefined,
      FailureCode.InvalidAmount,
    ],
    [
      'REFUND without referenceExternalTransactionId',
      JSON.stringify(buildEnvelope({}, { kind: 'REFUND' })),
      undefined,
      FailureCode.ReferenceRequired,
    ],
    [
      'BET with referenceExternalTransactionId',
      JSON.stringify(buildEnvelope({}, { kind: 'BET', referenceExternalTransactionId: 'ref-1' })),
      undefined,
      FailureCode.ReferenceNotAllowed,
    ],
    [
      'mismatched SQS MessageGroupId',
      JSON.stringify(buildEnvelope()),
      newId(),
      FailureCode.InvalidPayload,
    ],
  ])('rejects %s with %s', (_label, rawBody, groupId, expectedCode) => {
    expectDomainFailure(() => parseWagerMessage(rawBody, groupId), expectedCode);
  });
});

describe('SQS FIFO helpers', () => {
  test('toWagerTransactionSendInput sets MessageGroupId = walletId and deduplicates by messageId', () => {
    const envelope = buildEnvelope() as unknown as WagerTransactionRequestedEnvelope;
    const input = toWagerTransactionSendInput('http://localhost:4566/000000000000/wager-transactions.fifo', envelope);

    expect(input.MessageGroupId).toBe(walletId);
    expect(input.MessageDeduplicationId).toBe('msg-123');
    expect(JSON.parse(input.MessageBody ?? '{}')).toEqual(envelope);
  });

  test('toWagerTransactionSendInput hashes messageIds that are not valid SQS FIFO tokens', () => {
    const envelope = buildEnvelope({ messageId: 'msg with spaces 123' }) as unknown as WagerTransactionRequestedEnvelope;
    const input = toWagerTransactionSendInput('http://localhost:4566/000000000000/wager-transactions.fifo', envelope);

    expect(input.MessageGroupId).toBe(walletId);
    expect(input.MessageDeduplicationId).toMatch(/^[0-9a-f]{64}$/);
  });

  test('deriveDlqRouting prefers data.walletId, falls back to SQS MessageGroupId, then default group', () => {
    const fromBody = deriveDlqRouting({
      MessageId: 'sqs-1',
      Body: JSON.stringify(buildEnvelope()),
      Attributes: { MessageGroupId: 'other-group' },
    });
    expect(fromBody.messageGroupId).toBe(walletId);
    expect(fromBody.messageDeduplicationId).toMatch(/^[0-9a-f]{64}$/);

    const fromAttr = deriveDlqRouting({
      MessageId: 'sqs-2',
      Body: '{not-json',
      Attributes: { MessageGroupId: 'group-abc' },
    });
    expect(fromAttr.messageGroupId).toBe('group-abc');

    const fallback = deriveDlqRouting({
      MessageId: 'sqs-3',
      Body: '{not-json',
    });
    expect(fallback.messageGroupId).toBe('invalid-wager-message');
  });
});

describe('AppConfig Stage 6 worker settings', () => {
  const baseEnv = {
    DATABASE_URL: 'postgresql://user:pass@localhost:5433/db',
    AWS_REGION: 'us-east-1',
  };

  test('enables background workers by default outside NODE_ENV=test and disables by default in NODE_ENV=test', () => {
    expect(AppConfig.load(baseEnv).workersEnabled).toBe(true);
    expect(AppConfig.load({ ...baseEnv, NODE_ENV: 'production' }).workersEnabled).toBe(true);
    expect(AppConfig.load({ ...baseEnv, NODE_ENV: 'test' }).workersEnabled).toBe(false);
    expect(AppConfig.load({ ...baseEnv, NODE_ENV: 'test', WORKERS_ENABLED: 'true' }).workersEnabled).toBe(true);
    expect(AppConfig.load({ ...baseEnv, NODE_ENV: 'production', WORKERS_ENABLED: 'false' }).workersEnabled).toBe(false);
  });

  test('rejects invalid WORKERS_ENABLED values', () => {
    expect(() => AppConfig.load({ ...baseEnv, WORKERS_ENABLED: 'maybe' })).toThrow(InvalidEnvironmentError);
  });
});
