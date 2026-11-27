import { IsolationLevel, LockMode, UniqueConstraintViolationException } from '@mikro-orm/core';
import { type EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Injectable, Optional, ServiceUnavailableException } from '@nestjs/common';
import { AppConfig } from '../common/config/env';
import { DomainError, InvalidStateError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { SERVICE_UNAVAILABLE_CODE } from '../common/http/domain-exception.filter';
import { ShutdownService } from '../common/http/shutdown.service';
import { guardMetrics, MetricsRegistry, recordStatusTransition } from '../common/observability/metrics';
import { isUuid, newId } from '../common/ids';
import type { EventContext, IntegrationEvent } from '../common/messaging/integration-event';
import { WagerTransactionRecord, WalletLedgerEntryRecord, WalletRecord } from '../database/entities';
import { readDatabaseClock } from '../database/clock';
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
  type WagerTransactionState,
  WagerTransactionStatus,
} from './wager-transaction';

const IDEMPOTENCY_KEY_CONSTRAINT = 'wager_transactions_idempotency_key_uq';
const PROVIDER_EXTERNAL_CONSTRAINT = 'wager_transactions_provider_external_uq';
const SINGLE_ACTIVE_REVERSAL_CONSTRAINT = 'wager_transactions_single_active_reversal_uq';
const MAX_PROVIDER_ID_LENGTH = 100;
const MAX_EXTERNAL_ID_LENGTH = 200;
const DEFAULT_PENDING_REFERENCE_TTL_MS = 60_000;
const DEFAULT_PENDING_REFERENCE_MAX_ATTEMPTS = 10;

export interface SubmitWagerCommand extends ParsedWagerInput {
  correlationId: string;
  causationId?: string;
}

export interface ProcessWagerResult {
  transaction: WagerTransaction;
  idempotentReplay: boolean;
}

export interface PendingReferencePolicy {
  readonly ttlMs: number;
  readonly maxAttempts: number;
}

export type ResolvePendingReferenceResult =
  | { readonly outcome: 'skipped' }
  | { readonly outcome: 'rescheduled'; readonly transaction: WagerTransaction }
  | { readonly outcome: 'resolved'; readonly transaction: WagerTransaction };

type EventContextProvider = () => EventContext | Promise<EventContext>;

/**
 * Private internal sentinel (never exported, never an instance of `DomainError`) thrown inside the
 * resolution SAVEPOINT when a concurrent transaction committed between the lock-free prefetch probe
 * and `wallets FOR UPDATE`, turning a predicted `rescheduled` pass into a terminal resolution while
 * `cachedCtx` is still empty. Catching this rolls back the SAVEPOINT (releasing `wallets FOR UPDATE`
 * and `wager_transactions FOR UPDATE` in PostgreSQL) so `findOriginatingEventContext` executes
 * completely outside pessimistic row locks before a single bounded re-run.
 */
class OriginatingContextPrefetchRequiredSignal extends Error {
  constructor() {
    super('Originating event context prefetch required outside wallet lock');
    this.name = 'OriginatingContextPrefetchRequiredSignal';
  }
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
  constructor(
    private readonly orm: MikroORM,
    @Optional() private readonly config?: AppConfig,
    @Optional() private readonly shutdown?: ShutdownService,
    @Optional() private readonly metrics?: MetricsRegistry,
  ) {}

  /** Entry point when the caller does not already own a SQL transaction (HTTP). */
  async process(command: SubmitWagerCommand): Promise<ProcessWagerResult> {
    // Validate domain invariants before checking out a pooled connection.
    buildCandidate(command, new Date());
    return this.runPostgresOperation(
      'WagerProcessorService.process',
      {
        walletId: command.walletId,
        correlationId: command.correlationId,
      },
      async () => {
        const started = performance.now();
        try {
          const result = await this.orm.em.fork().transactional(
            (txEm) => this.processInTransaction(txEm, command),
            { isolationLevel: IsolationLevel.READ_COMMITTED },
          );
          this.recordHttpOutcome(result, started, 'completed');
          return result;
        } catch (error) {
          this.recordHttpFailure(error, started);
          throw error;
        }
      },
    );
  }

  private recordHttpOutcome(result: ProcessWagerResult, started: number, outcome: 'completed' | 'error'): void {
    guardMetrics(() => {
      this.metrics?.observe('wager_processing_duration_ms', performance.now() - started, { channel: 'http', outcome });
      if (result.idempotentReplay) {
        this.metrics?.increment('wager_duplicates_total', { cause: 'idempotency_replay' });
        return;
      }
      recordStatusTransition(this.metrics, result.transaction.status);
    });
  }

  private recordHttpFailure(error: unknown, started: number): void {
    guardMetrics(() => {
      this.metrics?.observe('wager_processing_duration_ms', performance.now() - started, {
        channel: 'http',
        outcome: 'error',
      });
      if (error instanceof DomainError && error.code === FailureCode.IdempotencyPayloadConflict) {
        this.metrics?.increment('wager_hash_conflicts_total', { cause: 'idempotency' });
      }
    });
  }

  /**
   * Executes inside the caller's active SQL transaction (e.g. alongside inbox_messages in SQS)
   * without opening an independent root transaction.
   *
   * Lock order: (caller inbox) -> wallets FOR UPDATE -> wager_transactions -> ledger / outbox.
   */
  async processInTransaction(
    txEm: EntityManager,
    command: SubmitWagerCommand,
    options?: { readonly now?: Date },
  ): Promise<ProcessWagerResult> {
    if (!txEm.isInTransaction()) {
      throw new InvalidStateError('WagerProcessorService.processInTransaction requires an active transaction');
    }

    const now = options?.now ?? (await readDatabaseClock(txEm));
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

    const decided = await this.decide(txEm, wallet, candidate, () => ctx, now, this.defaultPendingPolicy());
    return this.persistWithSavepoint(txEm, command, walletRecord, decided, ctx, now);
  }

  /**
   * Re-evaluates a persisted PENDING_REFERENCE transaction inside the caller's READ COMMITTED
   * SQL transaction while strictly preserving the global lock order:
   * wallets FOR UPDATE -> wager_transactions FOR UPDATE -> ledger -> outbox.
   */
  async resolvePendingReferenceInTransaction(
    txEm: EntityManager,
    transactionId: string,
    walletId: string,
    options?: { now?: Date; policy?: PendingReferencePolicy },
  ): Promise<ResolvePendingReferenceResult> {
    if (!txEm.isInTransaction()) {
      throw new InvalidStateError(
        'WagerProcessorService.resolvePendingReferenceInTransaction requires an active transaction',
      );
    }
    if (!isUuid(transactionId) || !isUuid(walletId)) {
      throw new DomainError(FailureCode.InvalidPayload, 'transactionId and walletId must be UUIDs');
    }

    const now = options?.now ?? (await readDatabaseClock(txEm));
    const policy = options?.policy ?? this.defaultPendingPolicy();
    let cachedCtx: EventContext | undefined;

    // Lock-free PK + unique-index MVCC check inside txEm BEFORE acquiring `wallets FOR UPDATE`:
    // - Takes zero row locks, preserving `wallets FOR UPDATE -> wager_transactions FOR UPDATE -> ledger -> outbox`.
    // - Rehydrates domain entities to evaluate `referenceViolation` without duplicating domain rules in SQL.
    // - Skips `outbox_messages` completely on `skipped` and `rescheduled` paths.
    if (await shouldPrefetchOriginatingContext(txEm, transactionId, walletId, now, policy)) {
      cachedCtx = await findOriginatingEventContext(txEm, walletId, transactionId, now);
    }

    try {
      return await this.executePendingResolutionSavepoint(
        txEm,
        transactionId,
        walletId,
        now,
        policy,
        cachedCtx,
      );
    } catch (error) {
      if (!(error instanceof OriginatingContextPrefetchRequiredSignal)) {
        throw error;
      }
    }

    // SAVEPOINT rolled back, releasing `wallets FOR UPDATE` and `wager_transactions FOR UPDATE`.
    // Load the originating context with zero pessimistic row locks held, then re-run at most once.
    cachedCtx = await findOriginatingEventContext(txEm, walletId, transactionId, now);
    return this.executePendingResolutionSavepoint(
      txEm,
      transactionId,
      walletId,
      now,
      policy,
      cachedCtx,
    );
  }

  async getById(transactionId: string): Promise<WagerTransaction> {
    if (!isUuid(transactionId)) {
      throw new DomainError(FailureCode.InvalidPayload, 'transactionId must be a UUID');
    }
    return this.runPostgresOperation('WagerProcessorService.getById', { transactionId }, async () => {
      const record = await this.orm.em.fork().findOne(WagerTransactionRecord, transactionId);
      if (record === null) {
        throw new DomainError(FailureCode.TransactionNotFound, `Transaction ${transactionId} does not exist`);
      }
      return WagerTransaction.rehydrate(record);
    });
  }

  async getByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction> {
    assertLookupIdentifier('providerId', providerId, MAX_PROVIDER_ID_LENGTH);
    assertLookupIdentifier('externalTransactionId', externalTransactionId, MAX_EXTERNAL_ID_LENGTH);
    return this.runPostgresOperation('WagerProcessorService.getByExternalId', { providerId }, async () => {
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
    });
  }

  private runPostgresOperation<T>(
    name: string,
    context: Readonly<Record<string, string | undefined>>,
    work: () => Promise<T>,
  ): Promise<T> {
    if (this.shutdown !== undefined) {
      if (
        this.shutdown.isShuttingDown() &&
        (this.shutdown.isHttpDrainCompleted() || this.shutdown.remainingCooperativeMs() <= 0)
      ) {
        throw new ServiceUnavailableException({
          code: SERVICE_UNAVAILABLE_CODE,
          message: 'Service is shutting down; retry later',
          ...(context.correlationId !== undefined ? { correlationId: context.correlationId } : {}),
        });
      }
      return this.shutdown.trackCriticalOperation(
        {
          kind: 'http_financial',
          name,
          resources: ['postgres'],
          context,
        },
        work,
      );
    }
    return work();
  }

  private defaultPendingPolicy(): PendingReferencePolicy {
    return {
      ttlMs: this.config?.pendingReferenceTtlMs ?? DEFAULT_PENDING_REFERENCE_TTL_MS,
      maxAttempts: this.config?.pendingReferenceMaxAttempts ?? DEFAULT_PENDING_REFERENCE_MAX_ATTEMPTS,
    };
  }

  /**
   * Runs one locked resolution attempt inside a SAVEPOINT (`txEm.transactional` with `clear: true`).
   * `findOriginatingEventContext` is structurally unreachable here: if a concurrent commit made a
   * terminal event necessary while `prefetchedCtx` is `undefined`, `resolveCtx` throws
   * `OriginatingContextPrefetchRequiredSignal` so PostgreSQL rolls back to the SAVEPOINT and
   * releases all row locks before `findOriginatingEventContext` is queried.
   */
  private async executePendingResolutionSavepoint(
    txEm: EntityManager,
    transactionId: string,
    walletId: string,
    now: Date,
    policy: PendingReferencePolicy,
    prefetchedCtx: EventContext | undefined,
  ): Promise<ResolvePendingReferenceResult> {
    return txEm.transactional(
      async (attemptEm) => {
        const walletRecord = await attemptEm.findOne(WalletRecord, walletId, {
          lockMode: LockMode.PESSIMISTIC_WRITE,
          refresh: true,
        });
        if (walletRecord === null) {
          throw new DomainError(FailureCode.WalletNotFound, `Wallet ${walletId} does not exist`);
        }

        const txRecord = await attemptEm.findOne(WagerTransactionRecord, transactionId, {
          lockMode: LockMode.PESSIMISTIC_WRITE,
          refresh: true,
        });
        if (txRecord === null || txRecord.status !== WagerTransactionStatus.PendingReference) {
          attemptEm.clear();
          return { outcome: 'skipped' };
        }
        if (txRecord.walletId !== walletId) {
          throw new InvalidStateError(
            `Pending transaction ${transactionId} belongs to wallet ${txRecord.walletId}, not ${walletId}`,
          );
        }
        if (txRecord.nextAttemptAt === null || txRecord.nextAttemptAt > now) {
          attemptEm.clear();
          return { outcome: 'skipped' };
        }

        const wallet = Wallet.rehydrate(walletRecord);
        const candidate = WagerTransaction.rehydrate(txRecord);
        const resolveCtx: EventContextProvider = () => {
          if (prefetchedCtx === undefined) {
            throw new OriginatingContextPrefetchRequiredSignal();
          }
          return prefetchedCtx;
        };

        const decided = await this.decide(attemptEm, wallet, candidate, resolveCtx, now, policy);
        await applyPendingResolutionWrite(attemptEm, decided);
        // Clear pre-nativeUpdate entities so `mergeEntitiesToParent` cannot copy stale snapshots into `txEm`.
        attemptEm.clear();

        return decided.transaction.status === WagerTransactionStatus.PendingReference
          ? { outcome: 'rescheduled', transaction: decided.transaction }
          : { outcome: 'resolved', transaction: decided.transaction };
      },
      { clear: true },
    );
  }

  private async decide(
    txEm: EntityManager,
    wallet: Wallet,
    candidate: WagerTransaction,
    resolveCtx: EventContextProvider,
    now: Date,
    policy: PendingReferencePolicy,
  ): Promise<DecidedWrite> {
    const wasAlreadyPendingReference = candidate.status === WagerTransactionStatus.PendingReference;
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
          return rejectCandidate(candidate, FailureCode.ReferenceAlreadyReversed, wallet, await resolveCtx());
        }
        return parkOrExpirePendingReference(
          candidate,
          wallet,
          resolveCtx,
          now,
          wasAlreadyPendingReference,
          FailureCode.ReferenceNotFound,
          policy,
        );
      }

      const reference = WagerTransaction.rehydrate(refRecord);
      const violation = candidate.referenceViolation(reference);
      if (violation !== undefined) {
        if (violation === FailureCode.ReferenceNotProcessed && !reference.isTerminal()) {
          if (candidate.requiresReference() && (await hasActiveReversal(txEm, candidate))) {
            return rejectCandidate(candidate, FailureCode.ReferenceAlreadyReversed, wallet, await resolveCtx());
          }
          return parkOrExpirePendingReference(
            candidate,
            wallet,
            resolveCtx,
            now,
            wasAlreadyPendingReference,
            FailureCode.ReferenceNotProcessed,
            policy,
          );
        }
        return rejectCandidate(candidate, violation, wallet, await resolveCtx());
      }
      if (candidate.requiresReference() && (await hasActiveReversal(txEm, candidate))) {
        return rejectCandidate(candidate, FailureCode.ReferenceAlreadyReversed, wallet, await resolveCtx());
      }
      resolvedReference = reference;
    }

    if (!candidate.affectsBalance()) {
      candidate.markProcessed(resolvedReference?.id, wallet.balance, now);
      return {
        transaction: candidate,
        events: [WagerTransactionProcessed.from(candidate, await resolveCtx())],
      };
    }

    const direction = candidate.ledgerDirectionFor(resolvedReference);
    const previousVersion = wallet.version;

    if (direction === LedgerDirection.Debit && !wallet.canDebit(candidate.money)) {
      const code =
        candidate.kind === WagerTransactionKind.Rollback
          ? FailureCode.NegativeBalanceReversal
          : FailureCode.InsufficientFunds;
      return rejectCandidate(candidate, code, wallet, await resolveCtx());
    }

    let entry: WalletLedgerEntry;
    try {
      entry =
        direction === LedgerDirection.Credit
          ? wallet.credit(candidate.money, candidate.id, now)
          : wallet.debit(candidate.money, candidate.id, now);
    } catch (error) {
      if (error instanceof DomainError && error.code === FailureCode.AmountOutOfRange) {
        return rejectCandidate(candidate, FailureCode.AmountOutOfRange, wallet, await resolveCtx());
      }
      throw error;
    }
    candidate.markProcessed(resolvedReference?.id, wallet.balance, now);
    const ctx = await resolveCtx();

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
      await applyWalletAndEvents(innerEm, decided);
    },
    { clear: true },
  );
}

async function applyPendingResolutionWrite(attemptEm: EntityManager, decided: DecidedWrite): Promise<void> {
  const txState = decided.transaction.toState();
  const updatedTx = await attemptEm.nativeUpdate(
    WagerTransactionRecord,
    { id: txState.id, status: WagerTransactionStatus.PendingReference },
    {
      referenceTransactionId: txState.referenceTransactionId,
      status: txState.status,
      failureCode: txState.failureCode,
      observedBalanceAmount: txState.observedBalanceAmount,
      attemptCount: txState.attemptCount,
      nextAttemptAt: txState.nextAttemptAt,
      processedAt: txState.processedAt,
    },
  );
  if (updatedTx !== 1) {
    throw new DomainError(
      FailureCode.PermanentIntegrityError,
      `Failed to update pending transaction ${txState.id}`,
    );
  }
  await applyWalletAndEvents(attemptEm, decided);
}

async function applyWalletAndEvents(innerEm: EntityManager, decided: DecidedWrite): Promise<void> {
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
  if (decided.events.length > 0) {
    await enqueueEvents(innerEm, decided.events);
  }
}

async function parkOrExpirePendingReference(
  candidate: WagerTransaction,
  wallet: Wallet,
  resolveCtx: EventContextProvider,
  now: Date,
  wasAlreadyPendingReference: boolean,
  expirationCode: FailureCode,
  policy: PendingReferencePolicy,
): Promise<DecidedWrite> {
  if (!wasAlreadyPendingReference) {
    candidate.markPendingReference(nextPendingReferenceAttemptAt(now, candidate.attemptCount + 1));
    return {
      transaction: candidate,
      events: [WagerTransactionPendingReference.from(candidate, await resolveCtx())],
    };
  }

  const ageMs = now.getTime() - candidate.createdAt.getTime();
  if (candidate.attemptCount >= policy.maxAttempts || ageMs >= policy.ttlMs) {
    return rejectCandidate(candidate, expirationCode, wallet, await resolveCtx());
  }

  candidate.markPendingReference(nextPendingReferenceAttemptAt(now, candidate.attemptCount + 1));
  return {
    transaction: candidate,
    events: [],
  };
}

interface PendingPrefetchJoinRow {
  t_id: string;
  t_provider_id: string;
  t_external_transaction_id: string;
  t_idempotency_key: string;
  t_payload_hash: string;
  t_wallet_id: string;
  t_player_id: string;
  t_round_id: string;
  t_game_id: string;
  t_kind: WagerTransactionKind;
  t_amount: string;
  t_currency: string;
  t_reference_external_transaction_id: string | null;
  t_reference_transaction_id: string | null;
  t_status: WagerTransactionStatus;
  t_failure_code: FailureCode | null;
  t_observed_balance_amount: string | null;
  t_attempt_count: number | string;
  t_next_attempt_at: Date | string | null;
  t_processed_at: Date | string | null;
  t_created_at: Date | string;
  r_id: string | null;
  r_provider_id: string | null;
  r_external_transaction_id: string | null;
  r_idempotency_key: string | null;
  r_payload_hash: string | null;
  r_wallet_id: string | null;
  r_player_id: string | null;
  r_round_id: string | null;
  r_game_id: string | null;
  r_kind: WagerTransactionKind | null;
  r_amount: string | null;
  r_currency: string | null;
  r_reference_external_transaction_id: string | null;
  r_reference_transaction_id: string | null;
  r_status: WagerTransactionStatus | null;
  r_failure_code: FailureCode | null;
  r_observed_balance_amount: string | null;
  r_attempt_count: number | string | null;
  r_next_attempt_at: Date | string | null;
  r_processed_at: Date | string | null;
  r_created_at: Date | string | null;
  has_active_reversal: boolean;
}

/**
 * Single O(1) PK + unique-index MVCC probe inside `txEm` (zero row locks) that rehydrates
 * `WagerTransaction` domain entities and delegates to `candidate.referenceViolation(reference)`
 * so zero domain rules are duplicated in SQL. Returns `true` whenever the visible snapshot will
 * produce a terminal event (`PROCESSED` or `REJECTED`), and `false` on `skipped` / `rescheduled`.
 */
async function shouldPrefetchOriginatingContext(
  txEm: EntityManager,
  transactionId: string,
  walletId: string,
  now: Date,
  policy: PendingReferencePolicy,
): Promise<boolean> {
  const rows = await txEm.execute<PendingPrefetchJoinRow[]>(
    `select
        t.id as t_id,
        t.provider_id as t_provider_id,
        t.external_transaction_id as t_external_transaction_id,
        t.idempotency_key as t_idempotency_key,
        t.payload_hash as t_payload_hash,
        t.wallet_id as t_wallet_id,
        t.player_id as t_player_id,
        t.round_id as t_round_id,
        t.game_id as t_game_id,
        t.kind as t_kind,
        t.amount as t_amount,
        t.currency as t_currency,
        t.reference_external_transaction_id as t_reference_external_transaction_id,
        t.reference_transaction_id as t_reference_transaction_id,
        t.status as t_status,
        t.failure_code as t_failure_code,
        t.observed_balance_amount as t_observed_balance_amount,
        t.attempt_count as t_attempt_count,
        t.next_attempt_at as t_next_attempt_at,
        t.processed_at as t_processed_at,
        t.created_at as t_created_at,
        r.id as r_id,
        r.provider_id as r_provider_id,
        r.external_transaction_id as r_external_transaction_id,
        r.idempotency_key as r_idempotency_key,
        r.payload_hash as r_payload_hash,
        r.wallet_id as r_wallet_id,
        r.player_id as r_player_id,
        r.round_id as r_round_id,
        r.game_id as r_game_id,
        r.kind as r_kind,
        r.amount as r_amount,
        r.currency as r_currency,
        r.reference_external_transaction_id as r_reference_external_transaction_id,
        r.reference_transaction_id as r_reference_transaction_id,
        r.status as r_status,
        r.failure_code as r_failure_code,
        r.observed_balance_amount as r_observed_balance_amount,
        r.attempt_count as r_attempt_count,
        r.next_attempt_at as r_next_attempt_at,
        r.processed_at as r_processed_at,
        r.created_at as r_created_at,
        case
          when t.status = 'PENDING_REFERENCE' and t.kind in ('REFUND', 'ROLLBACK') then exists (
            select 1
            from wager_transactions rev
            where rev.provider_id = t.provider_id
              and rev.reference_external_transaction_id = t.reference_external_transaction_id
              and rev.id <> t.id
              and rev.kind in ('REFUND', 'ROLLBACK')
              and rev.status in ('PENDING_REFERENCE', 'PROCESSED')
          )
          else false
        end as has_active_reversal
     from wager_transactions t
     left join wager_transactions r
       on t.status = 'PENDING_REFERENCE'
      and r.provider_id = t.provider_id
      and r.external_transaction_id = t.reference_external_transaction_id
     where t.id = ?
       and t.wallet_id = ?`,
    [transactionId, walletId],
  );

  const probe = rows[0];
  if (
    probe === undefined ||
    probe.t_status !== WagerTransactionStatus.PendingReference ||
    probe.t_next_attempt_at === null
  ) {
    return false;
  }

  const candidate = WagerTransaction.rehydrate(extractPrefetchTransactionState(probe, 't'));
  if (candidate.nextAttemptAt === undefined || candidate.nextAttemptAt > now) {
    return false;
  }

  const ageMs = now.getTime() - candidate.createdAt.getTime();
  if (candidate.attemptCount >= policy.maxAttempts || ageMs >= policy.ttlMs) {
    return true;
  }

  if (candidate.requiresReference() && Boolean(probe.has_active_reversal)) {
    return true;
  }

  if (probe.r_id === null) {
    return false;
  }

  const reference = WagerTransaction.rehydrate(extractPrefetchTransactionState(probe, 'r'));
  const violation = candidate.referenceViolation(reference);
  return !(violation === FailureCode.ReferenceNotProcessed && !reference.isTerminal());
}

function extractPrefetchTransactionState(
  row: PendingPrefetchJoinRow,
  prefix: 't' | 'r',
): WagerTransactionState {
  if (prefix === 't') {
    return {
      id: row.t_id,
      providerId: row.t_provider_id,
      externalTransactionId: row.t_external_transaction_id,
      idempotencyKey: row.t_idempotency_key,
      payloadHash: row.t_payload_hash,
      walletId: row.t_wallet_id,
      playerId: row.t_player_id,
      roundId: row.t_round_id,
      gameId: row.t_game_id,
      kind: row.t_kind,
      amount: assertDecimalColumnString('t_amount', row.t_amount),
      currency: row.t_currency,
      referenceExternalTransactionId: row.t_reference_external_transaction_id,
      referenceTransactionId: row.t_reference_transaction_id,
      status: row.t_status,
      failureCode: row.t_failure_code,
      observedBalanceAmount:
        row.t_observed_balance_amount === null
          ? null
          : assertDecimalColumnString('t_observed_balance_amount', row.t_observed_balance_amount),
      attemptCount: Number(row.t_attempt_count),
      nextAttemptAt: toOptionalDate(row.t_next_attempt_at),
      processedAt: toOptionalDate(row.t_processed_at),
      createdAt: toRequiredDate(row.t_created_at),
    };
  }

  return {
    id: row.r_id!,
    providerId: row.r_provider_id!,
    externalTransactionId: row.r_external_transaction_id!,
    idempotencyKey: row.r_idempotency_key!,
    payloadHash: row.r_payload_hash!,
    walletId: row.r_wallet_id!,
    playerId: row.r_player_id!,
    roundId: row.r_round_id!,
    gameId: row.r_game_id!,
    kind: row.r_kind!,
    amount: assertDecimalColumnString('r_amount', row.r_amount),
    currency: row.r_currency!,
    referenceExternalTransactionId: row.r_reference_external_transaction_id,
    referenceTransactionId: row.r_reference_transaction_id,
    status: row.r_status!,
    failureCode: row.r_failure_code,
    observedBalanceAmount:
      row.r_observed_balance_amount === null
        ? null
        : assertDecimalColumnString('r_observed_balance_amount', row.r_observed_balance_amount),
    attemptCount: Number(row.r_attempt_count),
    nextAttemptAt: toOptionalDate(row.r_next_attempt_at),
    processedAt: toOptionalDate(row.r_processed_at),
    createdAt: toRequiredDate(row.r_created_at!),
  };
}

function assertDecimalColumnString(column: string, value: unknown): string {
  if (typeof value !== 'string') {
    throw new InvalidStateError(`Expected decimal string for ${column}, received ${typeof value}`);
  }
  return value;
}

function toRequiredDate(value: Date | string): Date {
  return value instanceof Date ? value : new Date(value);
}

function toOptionalDate(value: Date | string | null): Date | null {
  if (value === null) {
    return null;
  }
  return value instanceof Date ? value : new Date(value);
}

async function findOriginatingEventContext(
  txEm: EntityManager,
  walletId: string,
  transactionId: string,
  occurredAt: Date,
): Promise<EventContext> {
  // At most one WagerTransactionPendingReference event is ever emitted per transactionId, so
  // `limit 1` without `order by occurred_at` short-circuits on the first match without sorting.
  const rows = await txEm.execute<{ id: string; correlation_id: string | null }[]>(
    `select id, payload->>'correlationId' as correlation_id
     from outbox_messages
     where aggregate_id = ?
       and event_type = 'WagerTransactionPendingReference'
       and payload->'data'->>'transactionId' = ?
     limit 1`,
    [walletId, transactionId],
  );
  const origin = rows[0];
  if (origin !== undefined && typeof origin.correlation_id === 'string' && origin.correlation_id.length > 0) {
    return {
      correlationId: origin.correlation_id,
      causationId: origin.id,
      occurredAt,
    };
  }
  return { correlationId: newId(), occurredAt };
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
      id: { $ne: candidate.id },
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
