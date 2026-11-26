import type { Message, SendMessageCommandInput } from '@aws-sdk/client-sqs';
import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { canonicalHash } from '../common/hashing/canonical-hash';
import { isUuid, newId } from '../common/ids';
import type { MoneyProps } from '../common/money/money';
import { parseWagerRequest } from '../wagering/wager-payload';
import type { SubmitWagerCommand } from '../wagering/wager-processor.service';
import { WagerTransaction, type WagerTransactionKind } from '../wagering/wager-transaction';

/** Fixed consumer identity shared by every instance consuming wager-transactions.fifo. */
export const WAGER_CONSUMER_NAME = 'wager-transactions';
export const WAGER_REQUESTED_MESSAGE_TYPE = 'WagerTransactionRequested';

const MAX_MESSAGE_ID_LENGTH = 255;
const CORRELATION_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const FIFO_TOKEN_PATTERN = /^[!-~]{1,128}$/;
const ISO_8601_DATETIME_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(?:Z|([+-])(\d{2}):(\d{2}))$/;
const VALIDATION_PROBE_ID = '0192f298-345e-7e38-af88-e43f851a819d';
const FALLBACK_DLQ_GROUP_ID = 'invalid-wager-message';

export interface WagerTransactionRequestedData {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface WagerTransactionRequestedEnvelope {
  messageId: string;
  type: typeof WAGER_REQUESTED_MESSAGE_TYPE;
  occurredAt: string;
  correlationId?: string;
  data: WagerTransactionRequestedData;
}

export interface ParsedWagerMessage {
  readonly messageId: string;
  readonly occurredAt: Date;
  readonly inboxPayloadHash: string;
  readonly command: SubmitWagerCommand;
}

/**
 * Validates the SQS envelope and business payload without duplicating wagering rules:
 * delegates field normalization to `parseWagerRequest` and domain invariant checks to
 * `WagerTransaction.create`. Throws `DomainError` when the message cannot be accepted.
 */
export function parseWagerMessage(
  rawBody: string | undefined,
  sqsMessageGroupId?: string,
): ParsedWagerMessage {
  if (typeof rawBody !== 'string' || rawBody.trim().length === 0) {
    throw new DomainError(FailureCode.InvalidPayload, 'SQS message body must be a non-empty JSON string');
  }

  let decoded: unknown;
  try {
    decoded = JSON.parse(rawBody);
  } catch {
    throw new DomainError(FailureCode.InvalidPayload, 'SQS message body is not valid JSON');
  }

  if (!isPlainObject(decoded)) {
    throw new DomainError(FailureCode.InvalidPayload, 'SQS message envelope must be a JSON object');
  }

  const rawMessageId = decoded.messageId;
  if (
    typeof rawMessageId !== 'string' ||
    rawMessageId.trim().length === 0 ||
    rawMessageId.length > MAX_MESSAGE_ID_LENGTH
  ) {
    throw new DomainError(
      FailureCode.InvalidPayload,
      `messageId must be a non-blank string up to ${MAX_MESSAGE_ID_LENGTH} characters`,
    );
  }

  if (decoded.type !== WAGER_REQUESTED_MESSAGE_TYPE) {
    throw new DomainError(
      FailureCode.InvalidPayload,
      `Unsupported message type "${String(decoded.type)}"; expected "${WAGER_REQUESTED_MESSAGE_TYPE}"`,
    );
  }

  const rawOccurredAt = decoded.occurredAt;
  if (typeof rawOccurredAt !== 'string') {
    throw new DomainError(FailureCode.InvalidPayload, 'occurredAt must be an ISO-8601 datetime string');
  }
  const occurredAt = parseStrictIsoDateTime(rawOccurredAt);

  const rawData = decoded.data;
  if (!isPlainObject(rawData)) {
    throw new DomainError(FailureCode.InvalidPayload, 'envelope.data must be a JSON object');
  }

  const parsedInput = parseWagerRequest(rawData, rawData.idempotencyKey);

  // Enforce domain construction invariants before touching PostgreSQL.
  WagerTransaction.create({
    id: VALIDATION_PROBE_ID,
    providerId: parsedInput.providerId,
    externalTransactionId: parsedInput.externalTransactionId,
    idempotencyKey: parsedInput.idempotencyKey,
    payloadHash: parsedInput.payloadHash,
    walletId: parsedInput.walletId,
    playerId: parsedInput.playerId,
    roundId: parsedInput.roundId,
    gameId: parsedInput.gameId,
    kind: parsedInput.kind,
    money: parsedInput.money,
    referenceExternalTransactionId: parsedInput.referenceExternalTransactionId,
    now: occurredAt,
  });

  if (sqsMessageGroupId !== undefined && sqsMessageGroupId !== parsedInput.walletId) {
    throw new DomainError(
      FailureCode.InvalidPayload,
      `SQS MessageGroupId "${sqsMessageGroupId}" must match data.walletId "${parsedInput.walletId}"`,
    );
  }

  const rawCorrelationId = decoded.correlationId;
  if (rawCorrelationId !== undefined && (typeof rawCorrelationId !== 'string' || !CORRELATION_ID_PATTERN.test(rawCorrelationId))) {
    throw new DomainError(FailureCode.InvalidPayload, 'correlationId is malformed');
  }
  const correlationId =
    typeof rawCorrelationId === 'string'
      ? rawCorrelationId
      : CORRELATION_ID_PATTERN.test(rawMessageId)
        ? rawMessageId
        : newId();

  const inboxPayloadHash = canonicalHash({
    type: WAGER_REQUESTED_MESSAGE_TYPE,
    idempotencyKey: parsedInput.idempotencyKey,
    payloadHash: parsedInput.payloadHash,
  });

  return {
    messageId: rawMessageId,
    occurredAt,
    inboxPayloadHash,
    command: {
      ...parsedInput,
      correlationId,
      causationId: rawMessageId,
    },
  };
}

/**
 * Builds a FIFO-compliant `SendMessageCommandInput` for `wager-transactions.fifo`
 * using `MessageGroupId = walletId` as mandated by the architecture.
 */
export function toWagerTransactionSendInput(
  queueUrl: string,
  envelope: WagerTransactionRequestedEnvelope,
): SendMessageCommandInput {
  if (!isUuid(envelope.data.walletId)) {
    throw new DomainError(FailureCode.InvalidPayload, 'envelope.data.walletId must be a UUID');
  }
  return {
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(envelope),
    MessageGroupId: envelope.data.walletId,
    MessageDeduplicationId: toFifoToken(envelope.messageId),
  };
}

/**
 * Derives deterministic FIFO routing keys when forwarding a poison message to
 * `wager-transactions-dlq.fifo` (which is a FIFO queue without ContentBasedDeduplication).
 */
export function deriveDlqRouting(message: Pick<Message, 'MessageId' | 'Body' | 'Attributes'>): {
  readonly messageGroupId: string;
  readonly messageDeduplicationId: string;
} {
  const walletGroupId = tryExtractWalletGroupId(message.Body);
  const sqsGroupId = message.Attributes?.MessageGroupId;
  const messageGroupId =
    walletGroupId ??
    (typeof sqsGroupId === 'string' && FIFO_TOKEN_PATTERN.test(sqsGroupId) ? sqsGroupId : FALLBACK_DLQ_GROUP_ID);

  const messageDeduplicationId = canonicalHash({
    sqsMessageId: message.MessageId ?? null,
    body: message.Body ?? '',
  });

  return { messageGroupId, messageDeduplicationId };
}

function toFifoToken(value: string): string {
  return FIFO_TOKEN_PATTERN.test(value) ? value : canonicalHash({ value });
}

function tryExtractWalletGroupId(rawBody: string | undefined): string | undefined {
  if (typeof rawBody !== 'string') {
    return undefined;
  }
  try {
    const parsed = JSON.parse(rawBody) as unknown;
    if (!isPlainObject(parsed) || !isPlainObject(parsed.data)) {
      return undefined;
    }
    const walletId = parsed.data.walletId;
    return typeof walletId === 'string' && isUuid(walletId) ? walletId : undefined;
  } catch {
    return undefined;
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseStrictIsoDateTime(value: string): Date {
  const match = ISO_8601_DATETIME_PATTERN.exec(value);
  if (match === null) {
    throw new DomainError(FailureCode.InvalidPayload, 'occurredAt must be an ISO-8601 datetime string');
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[8] !== undefined ? Number(match[8]) : 0;
  const offsetMinute = match[9] !== undefined ? Number(match[9]) : 0;

  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 23 ||
    offsetMinute > 59
  ) {
    throw new DomainError(FailureCode.InvalidPayload, 'occurredAt is not a valid calendar date');
  }

  const utcProbe = new Date(Date.UTC(year, month - 1, day));
  if (
    utcProbe.getUTCFullYear() !== year ||
    utcProbe.getUTCMonth() !== month - 1 ||
    utcProbe.getUTCDate() !== day
  ) {
    throw new DomainError(FailureCode.InvalidPayload, 'occurredAt is not a valid calendar date');
  }

  const epochMs = Date.parse(value);
  if (Number.isNaN(epochMs)) {
    throw new DomainError(FailureCode.InvalidPayload, 'occurredAt is not a valid calendar date');
  }
  return new Date(epochMs);
}
