import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { canonicalHash } from '../common/hashing/canonical-hash';
import { Money, type MoneyProps } from '../common/money/money';
import type { WagerTransactionKind } from './wager-transaction';

const MAX_IDEMPOTENCY_KEY_LENGTH = 255;
const PENDING_REFERENCE_BASE_DELAY_MS = 1_000;
const PENDING_REFERENCE_MAX_DELAY_MS = 15_000;

export interface WagerPayloadFields {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
}

export interface ParsedWagerInput extends WagerPayloadFields {
  idempotencyKey: string;
  payloadHash: string;
}

/**
 * SHA-256 of the canonical JSON of business fields only. Transport metadata (Idempotency-Key,
 * X-Correlation-Id, SQS messageId/occurredAt) never enters the hash, and Money is normalized to
 * fixed 2-decimal MoneyProps so "25" and "25.00" produce the same digest.
 */
export function wagerPayloadHash(fields: WagerPayloadFields): string {
  return canonicalHash({
    providerId: fields.providerId,
    externalTransactionId: fields.externalTransactionId,
    playerId: fields.playerId,
    walletId: fields.walletId,
    roundId: fields.roundId,
    gameId: fields.gameId,
    kind: fields.kind,
    money: fields.money.toJSON(),
    referenceExternalTransactionId: fields.referenceExternalTransactionId,
  });
}

export function parseIdempotencyKey(value: unknown): string {
  if (typeof value !== 'string' || value.trim().length === 0 || value.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
    throw new DomainError(
      FailureCode.InvalidPayload,
      `Idempotency-Key must be a non-blank string up to ${MAX_IDEMPOTENCY_KEY_LENGTH} characters`,
    );
  }
  return value;
}

export function parseWagerRequest(rawBody: unknown, rawIdempotencyKey: unknown): ParsedWagerInput {
  const idempotencyKey = parseIdempotencyKey(rawIdempotencyKey);
  if (typeof rawBody !== 'object' || rawBody === null || Array.isArray(rawBody)) {
    throw new DomainError(FailureCode.InvalidPayload, 'Request body must be a JSON object');
  }
  const body = rawBody as Record<string, unknown>;

  const providerId = requireString(body, 'providerId');
  const externalTransactionId = requireString(body, 'externalTransactionId');
  const playerId = requireString(body, 'playerId');
  const walletId = requireString(body, 'walletId');
  const roundId = requireString(body, 'roundId');
  const gameId = requireString(body, 'gameId');
  const kind = requireString(body, 'kind') as WagerTransactionKind;

  const rawMoney = body.money;
  if (typeof rawMoney !== 'object' || rawMoney === null || Array.isArray(rawMoney)) {
    throw new DomainError(FailureCode.InvalidPayload, 'money must be { amount, currency }');
  }
  const money = Money.from(rawMoney as MoneyProps);

  const rawRef = body.referenceExternalTransactionId;
  let referenceExternalTransactionId: string | undefined;
  if (rawRef !== undefined && rawRef !== null) {
    if (typeof rawRef !== 'string') {
      throw new DomainError(FailureCode.InvalidPayload, 'referenceExternalTransactionId must be a string');
    }
    referenceExternalTransactionId = rawRef;
  }

  const fields: WagerPayloadFields = {
    providerId,
    externalTransactionId,
    playerId,
    walletId,
    roundId,
    gameId,
    kind,
    money,
    ...(referenceExternalTransactionId !== undefined ? { referenceExternalTransactionId } : {}),
  };

  return {
    ...fields,
    idempotencyKey,
    payloadHash: wagerPayloadHash(fields),
  };
}

/** Exponential backoff for out-of-order references (1s, 2s, 4s, 8s, capped at 15s). */
export function nextPendingReferenceAttemptAt(now: Date, attemptNumber: number): Date {
  if (!Number.isInteger(attemptNumber) || attemptNumber < 1) {
    throw new RangeError('attemptNumber must be a positive integer');
  }
  const delay = Math.min(PENDING_REFERENCE_BASE_DELAY_MS * 2 ** (attemptNumber - 1), PENDING_REFERENCE_MAX_DELAY_MS);
  return new Date(now.getTime() + delay);
}

function requireString(source: Record<string, unknown>, field: string): string {
  const value = source[field];
  if (typeof value !== 'string') {
    throw new DomainError(FailureCode.InvalidPayload, `${field} must be a string`);
  }
  return value;
}
