import { InvalidStateError } from '../common/errors/domain.error';
import type { FailureCode } from '../common/errors/failure-code';
import { newId } from '../common/ids';
import { type EventContext, IntegrationEvent, type IntegrationEventProps } from '../common/messaging/integration-event';
import type { MoneyProps } from '../common/money/money';
import { type WagerTransaction, type WagerTransactionKind, WagerTransactionStatus } from './wager-transaction';

interface WagerTransactionEventData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId: string | null;
}

export interface WagerTransactionProcessedData extends WagerTransactionEventData {
  referenceTransactionId: string | null;
  observedBalance: MoneyProps;
}

export interface WagerTransactionRejectedData extends WagerTransactionEventData {
  failureCode: FailureCode;
  observedBalance: MoneyProps;
}

export interface WagerTransactionPendingReferenceData extends WagerTransactionEventData {
  referenceExternalTransactionId: string;
  attemptCount: number;
  nextAttemptAt: string;
}

/** Any applied transaction, LOSS included. Published on the wallet's stream. */
export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  private constructor(props: IntegrationEventProps<WagerTransactionProcessedData>) {
    super(props);
  }

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    const observedBalance = transaction.observedBalance;
    if (transaction.status !== WagerTransactionStatus.Processed || observedBalance === undefined) {
      throw unexpectedStatus(transaction, WagerTransactionStatus.Processed);
    }
    return new WagerTransactionProcessed(
      props(transaction, ctx, {
        ...baseData(transaction),
        referenceTransactionId: transaction.referenceTransactionId ?? null,
        observedBalance: observedBalance.toJSON(),
      }),
    );
  }
}

/** A transaction rejected by a business rule; the balance did not move. */
export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  private constructor(props: IntegrationEventProps<WagerTransactionRejectedData>) {
    super(props);
  }

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    const { failureCode, observedBalance } = transaction;
    if (
      transaction.status !== WagerTransactionStatus.Rejected ||
      failureCode === undefined ||
      observedBalance === undefined
    ) {
      throw unexpectedStatus(transaction, WagerTransactionStatus.Rejected);
    }
    return new WagerTransactionRejected(
      props(transaction, ctx, { ...baseData(transaction), failureCode, observedBalance: observedBalance.toJSON() }),
    );
  }
}

/** A REFUND or ROLLBACK whose reference has not arrived yet; it will be retried. */
export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  private constructor(props: IntegrationEventProps<WagerTransactionPendingReferenceData>) {
    super(props);
  }

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    const { referenceExternalTransactionId, nextAttemptAt } = transaction;
    if (
      transaction.status !== WagerTransactionStatus.PendingReference ||
      referenceExternalTransactionId === undefined ||
      nextAttemptAt === undefined
    ) {
      throw unexpectedStatus(transaction, WagerTransactionStatus.PendingReference);
    }
    return new WagerTransactionPendingReference(
      props(transaction, ctx, {
        ...baseData(transaction),
        referenceExternalTransactionId,
        attemptCount: transaction.attemptCount,
        nextAttemptAt: nextAttemptAt.toISOString(),
      }),
    );
  }
}

/** Every event of a transaction belongs to its wallet's stream, ordered with the balance changes. */
function props<T>(transaction: WagerTransaction, ctx: EventContext, data: T): IntegrationEventProps<T> {
  return { ...ctx, eventId: newId(), aggregateId: transaction.walletId, data };
}

function baseData(transaction: WagerTransaction): WagerTransactionEventData {
  return {
    transactionId: transaction.id,
    providerId: transaction.providerId,
    externalTransactionId: transaction.externalTransactionId,
    walletId: transaction.walletId,
    playerId: transaction.playerId,
    roundId: transaction.roundId,
    gameId: transaction.gameId,
    kind: transaction.kind,
    money: transaction.money.toJSON(),
    referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
  };
}

function unexpectedStatus(transaction: WagerTransaction, expected: WagerTransactionStatus): InvalidStateError {
  return new InvalidStateError(`Transaction ${transaction.id} is ${transaction.status}, not a complete ${expected}`);
}
