import { LockMode, UniqueConstraintViolationException } from '@mikro-orm/core';
import { type EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Injectable } from '@nestjs/common';
import { DomainError, InvalidStateError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { isUuid, newId } from '../common/ids';
import type { EventContext, IntegrationEvent } from '../common/messaging/integration-event';
import { WagerTransactionRecord, WalletLedgerEntryRecord, WalletRecord } from '../database/entities';
import { enqueueEvents } from '../database/outbox';
import { Wallet } from '../wallets/wallet';
import { WalletBalanceChanged } from '../wallets/wallet-events';
import { LedgerDirection, type WalletLedgerEntry } from '../wallets/wallet-ledger-entry';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from './wager-events';
import { nextPendingReferenceAttemptAt, type ParsedWagerInput } from './wager-payload';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from './wager-transaction';

const IDEMPOTENCY_KEY_CONSTRAINT = 'wager_transactions_idempotency_key_uq';
const PROVIDER_EXTERNAL_CONSTRAINT = 'wager_transactions_provider_external_uq';
const SINGLE_ACTIVE_REVERSAL_CONSTRAINT = 'wager_transactions_single_active_reversal_uq';
const MAX_PROVIDER_ID_LENGTH = 100;
const MAX_EXTERNAL_ID_LENGTH = 200;

export interface SubmitWagerCommand extends ParsedWagerInput {
  correlationId: string;
  causationId?: string;
}

export interface ProcessWagerResult {
  transaction: WagerTransaction;
  idempotentReplay: boolean;
}

interface DecidedWrite {
  transaction: WagerTransaction;
  walletMovement?: {
    previousVersion: number;
    wallet: Wallet;
    entry: WalletLedgerEntry;
  };
  events: readonly IntegrationEvent<unknown>[];
}

@Injectable()
export class WagerProcessorService {
  constructor(private readonly orm: MikroORM) {}

  /** Entry point when the caller does not already own a SQL transaction (HTTP). */
  async process(command: SubmitWagerCommand): Promise<ProcessWagerResult> {
    // Validate domain invariants before checking out a pooled connection.
    buildCandidate(command, new Date());
    return this.orm.em.fork().transactional((txEm) => this.processInTransaction(txEm, command));
  }

  /**
   * Executes inside the caller's active SQL transaction (e.g. alongside inbox_messages in SQS)
   * without opening an independent root transaction.
   *
   * Lock order: (caller inbox) -> wallets FOR UPDATE -> wager_transactions -> ledger / outbox.
   */
  async processInTransaction(txEm: EntityManager, command: SubmitWagerCommand): Promise<ProcessWagerResult> {
    if (!txEm.isInTransaction()) {
      throw new InvalidStateError('WagerProcessorService.processInTransaction requires an active transaction');
    }

    const now = new Date();
    const candidate = buildCandidate(command, now);
    const ctx: EventContext = {
      correlationId: command.correlationId,
      ...(command.causationId !== undefined ? { causationId: command.causationId } : {}),
      occurredAt: now,
    };

    const walletRecord = await txEm.findOne(WalletRecord, candidate.walletId, {
      lockMode: LockMode.PESSIMISTIC_WRITE,
      refresh: true,
    });

    const collisions = await findCollisions(txEm, candidate);
    if (collisions.length > 0) {
      return resolveCollision(collisions, candidate);
    }

    if (walletRecord === null) {
      throw new DomainError(FailureCode.WalletNotFound, `Wallet ${candidate.walletId} does not exist`);
    }

    const wallet = Wallet.rehydrate(walletRecord);
    if (wallet.playerId !== candidate.playerId) {
      throw new DomainError(
        FailureCode.WalletPlayerMismatch,
        `Wallet ${wallet.id} does not belong to player ${candidate.playerId}`,
      );
    }
    if (wallet.currency !== candidate.money.currency) {
      throw new DomainError(
        FailureCode.CurrencyMismatch,
        `Wallet ${wallet.id} holds ${wallet.currency}, not ${candidate.money.currency}`,
      );
    }

    const decided = await this.decide(txEm, wallet, candidate, ctx, now);
    return this.persistWithSavepoint(txEm, command, walletRecord, decided, ctx, now);
  }

  async getById(transactionId: string): Promise<WagerTransaction> {
    if (!isUuid(transactionId)) {
      throw new DomainError(FailureCode.InvalidPayload, 'transactionId must be a UUID');
    }
    const record = await this.orm.em.fork().findOne(WagerTransactionRecord, transactionId);
    if (record === null) {
      throw new DomainError(FailureCode.TransactionNotFound, `Transaction ${transactionId} does not exist`);
    }
    return WagerTransaction.rehydrate(record);
  }

  async getByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction> {
    assertLookupIdentifier('providerId', providerId, MAX_PROVIDER_ID_LENGTH);
    assertLookupIdentifier('externalTransactionId', externalTransactionId, MAX_EXTERNAL_ID_LENGTH);
    const record = await this.orm.em.fork().findOne(WagerTransactionRecord, {
      providerId,
      externalTransactionId,
    });
    if (record === null) {
      throw new DomainError(
        FailureCode.TransactionNotFound,
        `Transaction ${providerId}:${externalTransactionId} does not exist`,
      );
    }
    return WagerTransaction.rehydrate(record);
  }

  private async decide(
    txEm: EntityManager,
    wallet: Wallet,
    candidate: WagerTransaction,
    ctx: EventContext,
    now: Date,
  ): Promise<DecidedWrite> {
    let resolvedReference: WagerTransaction | undefined;

    if (candidate.referenceExternalTransactionId !== undefined) {
      const refRecord = await txEm.findOne(
        WagerTransactionRecord,
        {
          providerId: candidate.providerId,
          externalTransactionId: candidate.referenceExternalTransactionId,
        },
        { refresh: true },
      );

      if (refRecord === null) {
        if (candidate.requiresReference() && (await hasActiveReversal(txEm, candidate))) {
          return rejectCandidate(candidate, FailureCode.ReferenceAlreadyReversed, wallet, ctx);
        }
        candidate.markPendingReference(nextPendingReferenceAttemptAt(now, candidate.attemptCount + 1));
        return {
          transaction: candidate,
          events: [WagerTransactionPendingReference.from(candidate, ctx)],
        };
      }

      const reference = WagerTransaction.rehydrate(refRecord);
      const violation = candidate.referenceViolation(reference);
      if (violation !== undefined) {
        return rejectCandidate(candidate, violation, wallet, ctx);
      }
      if (candidate.requiresReference() && (await hasActiveReversal(txEm, candidate))) {
        return rejectCandidate(candidate, FailureCode.ReferenceAlreadyReversed, wallet, ctx);
      }
      resolvedReference = reference;
    }

    if (!candidate.affectsBalance()) {
      candidate.markProcessed(resolvedReference?.id, wallet.balance, now);
      return {
        transaction: candidate,
        events: [WagerTransactionProcessed.from(candidate, ctx)],
      };
    }

    const direction = candidate.ledgerDirectionFor(resolvedReference);
    const previousVersion = wallet.version;

    if (direction === LedgerDirection.Debit && !wallet.canDebit(candidate.money)) {
      const code =
        candidate.kind === WagerTransactionKind.Rollback
          ? FailureCode.NegativeBalanceReversal
          : FailureCode.InsufficientFunds;
      return rejectCandidate(candidate, code, wallet, ctx);
    }

    let entry: WalletLedgerEntry;
    try {
      entry =
        direction === LedgerDirection.Credit
          ? wallet.credit(candidate.money, candidate.id, now)
          : wallet.debit(candidate.money, candidate.id, now);
    } catch (error) {
      if (error instanceof DomainError && error.code === FailureCode.AmountOutOfRange) {
        return rejectCandidate(candidate, FailureCode.AmountOutOfRange, wallet, ctx);
      }
      throw error;
    }
    candidate.markProcessed(resolvedReference?.id, wallet.balance, now);

    return {
      transaction: candidate,
      walletMovement: { previousVersion, wallet, entry },
      events: [
        WagerTransactionProcessed.from(candidate, ctx),
        WalletBalanceChanged.from(wallet, entry, ctx),
      ],
    };
  }

  /**
   * Isolates writes in a SAVEPOINT with `{ clear: true }` so a cross-wallet unique constraint race
   * (23505) rolls back cleanly without aborting the caller's outer SQL transaction or leaking dirty
   * entities into `txEm`.
   */
  private async persistWithSavepoint(
    txEm: EntityManager,
    command: SubmitWagerCommand,
    lockedWalletRecord: WalletRecord,
    decided: DecidedWrite,
    ctx: EventContext,
    now: Date,
  ): Promise<ProcessWagerResult> {
    try {
      await executeDecidedWrite(txEm, decided);
      return { transaction: decided.transaction, idempotentReplay: false };
    } catch (error) {
      if (!(error instanceof UniqueConstraintViolationException)) {
        throw error;
      }
      const constraint = (error as { constraint?: unknown }).constraint;
      if (constraint === IDEMPOTENCY_KEY_CONSTRAINT || constraint === PROVIDER_EXTERNAL_CONSTRAINT) {
        const collisions = await findCollisions(txEm, decided.transaction);
        if (collisions.length > 0) {
          return resolveCollision(collisions, decided.transaction);
        }
      }
      if (constraint === SINGLE_ACTIVE_REVERSAL_CONSTRAINT) {
        const freshWallet = Wallet.rehydrate(lockedWalletRecord);
        const rejected = rejectCandidate(
          buildCandidate(command, now),
          FailureCode.ReferenceAlreadyReversed,
          freshWallet,
          ctx,
        );
        try {
          await executeDecidedWrite(txEm, rejected);
          return { transaction: rejected.transaction, idempotentReplay: false };
        } catch (retryError) {
          if (retryError instanceof UniqueConstraintViolationException) {
            const retryConstraint = (retryError as { constraint?: unknown }).constraint;
            if (retryConstraint === IDEMPOTENCY_KEY_CONSTRAINT || retryConstraint === PROVIDER_EXTERNAL_CONSTRAINT) {
              const collisions = await findCollisions(txEm, rejected.transaction);
              if (collisions.length > 0) {
                return resolveCollision(collisions, rejected.transaction);
              }
            }
          }
          throw retryError;
        }
      }
      throw error;
    }
  }
}

async function executeDecidedWrite(txEm: EntityManager, decided: DecidedWrite): Promise<void> {
  await txEm.transactional(
    async (innerEm) => {
      await innerEm.insert(WagerTransactionRecord, decided.transaction.toState());
      if (decided.walletMovement !== undefined) {
        const { previousVersion, wallet, entry } = decided.walletMovement;
        const state = wallet.toState();
        const updated = await innerEm.nativeUpdate(
          WalletRecord,
          { id: wallet.id, version: previousVersion },
          { balanceAmount: state.balanceAmount, version: state.version, updatedAt: state.updatedAt },
        );
        if (updated !== 1) {
          throw new DomainError(
            FailureCode.PermanentIntegrityError,
            `Failed to update locked wallet ${wallet.id} at version ${previousVersion}`,
          );
        }
        await innerEm.insert(WalletLedgerEntryRecord, entry.toState());
      }
      await enqueueEvents(innerEm, decided.events);
    },
    { clear: true },
  );
}

function buildCandidate(command: SubmitWagerCommand, now: Date): WagerTransaction {
  return WagerTransaction.create({
    id: newId(),
    providerId: command.providerId,
    externalTransactionId: command.externalTransactionId,
    idempotencyKey: command.idempotencyKey,
    payloadHash: command.payloadHash,
    walletId: command.walletId,
    playerId: command.playerId,
    roundId: command.roundId,
    gameId: command.gameId,
    kind: command.kind,
    money: command.money,
    referenceExternalTransactionId: command.referenceExternalTransactionId,
    now,
  });
}

function rejectCandidate(
  candidate: WagerTransaction,
  code: FailureCode,
  wallet: Wallet,
  ctx: EventContext,
): DecidedWrite {
  candidate.reject(code, wallet.balance);
  return {
    transaction: candidate,
    events: [WagerTransactionRejected.from(candidate, ctx)],
  };
}

async function findCollisions(txEm: EntityManager, candidate: WagerTransaction): Promise<WagerTransactionRecord[]> {
  return txEm.find(
    WagerTransactionRecord,
    {
      $or: [
        { idempotencyKey: candidate.idempotencyKey },
        { providerId: candidate.providerId, externalTransactionId: candidate.externalTransactionId },
      ],
    },
    { refresh: true },
  );
}

function resolveCollision(
  collisions: readonly WagerTransactionRecord[],
  candidate: WagerTransaction,
): ProcessWagerResult {
  const byKey = collisions.find((record) => record.idempotencyKey === candidate.idempotencyKey);
  if (byKey !== undefined) {
    const existing = WagerTransaction.rehydrate(byKey);
    if (existing.matchesPayload(candidate.payloadHash)) {
      return { transaction: existing, idempotentReplay: true };
    }
    throw new DomainError(
      FailureCode.IdempotencyPayloadConflict,
      `Idempotency-Key "${candidate.idempotencyKey}" was already used with a different payload`,
    );
  }
  throw new DomainError(
    FailureCode.IdempotencyPayloadConflict,
    `External transaction "${candidate.providerId}:${candidate.externalTransactionId}" was already recorded with another Idempotency-Key`,
  );
}

async function hasActiveReversal(txEm: EntityManager, candidate: WagerTransaction): Promise<boolean> {
  if (candidate.referenceExternalTransactionId === undefined) {
    return false;
  }
  const existing = await txEm.findOne(
    WagerTransactionRecord,
    {
      providerId: candidate.providerId,
      referenceExternalTransactionId: candidate.referenceExternalTransactionId,
      kind: { $in: [WagerTransactionKind.Refund, WagerTransactionKind.Rollback] },
      status: { $in: [WagerTransactionStatus.PendingReference, WagerTransactionStatus.Processed] },
    },
    { refresh: true },
  );
  return existing !== null;
}

function assertLookupIdentifier(field: string, value: string, maxLength: number): void {
  if (typeof value !== 'string' || value.trim().length === 0 || value.length > maxLength) {
    throw new DomainError(
      FailureCode.InvalidPayload,
      `${field} must be a non-blank string up to ${maxLength} characters`,
    );
  }
}
