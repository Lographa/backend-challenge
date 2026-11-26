import { DomainError, InvalidTransactionStateError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { canonicalHash, PAYLOAD_HASH_PATTERN } from '../common/hashing/canonical-hash';
import { isUuid } from '../common/ids';
import { Money } from '../common/money/money';
import { LedgerDirection } from '../wallets/wallet-ledger-entry';

export enum WagerTransactionKind {
  /** Internal: credit of the wallet's initial balance. */
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  /** Accepted, not applied yet. Only ever exists inside the SQL transaction deciding it. */
  Pending = 'PENDING',
  /** Waiting for the referenced transaction to arrive. */
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

/** Provider id reserved for internal transactions; external providers cannot use it. */
export const INTERNAL_PROVIDER_ID = 'internal';

const TERMINAL_STATUSES: ReadonlySet<WagerTransactionStatus> = new Set([
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
]);

/** Kinds each kind may reference. An empty list means a reference is not accepted. */
const ALLOWED_REFERENCE_KINDS: Readonly<Record<WagerTransactionKind, readonly WagerTransactionKind[]>> = {
  [WagerTransactionKind.Opening]: [],
  [WagerTransactionKind.Bet]: [],
  [WagerTransactionKind.Win]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Loss]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Refund]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Rollback]: [
    WagerTransactionKind.Bet,
    WagerTransactionKind.Win,
    WagerTransactionKind.Refund,
  ],
};

const MAX_PROVIDER_ID_LENGTH = 100;
const MAX_IDENTIFIER_LENGTH = 200;
const MAX_IDEMPOTENCY_KEY_LENGTH = 255;

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  amount: string;
  currency: string;
  referenceExternalTransactionId: string | null;
  referenceTransactionId: string | null;
  status: WagerTransactionStatus;
  failureCode: FailureCode | null;
  observedBalanceAmount: string | null;
  attemptCount: number;
  nextAttemptAt: Date | null;
  processedAt: Date | null;
  createdAt: Date;
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  now: Date;
}

export interface CreateOpeningProps {
  id: string;
  walletId: string;
  playerId: string;
  money: Money;
  now: Date;
}

export class WagerTransaction {
  private constructor(
    readonly id: string,
    readonly providerId: string,
    readonly externalTransactionId: string,
    readonly idempotencyKey: string,
    readonly payloadHash: string,
    readonly walletId: string,
    readonly playerId: string,
    readonly roundId: string,
    readonly gameId: string,
    readonly kind: WagerTransactionKind,
    readonly money: Money,
    /** Id at the provider — not the internal id. */
    readonly referenceExternalTransactionId: string | undefined,
    readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _observedBalance?: Money,
    private _attemptCount = 0,
    private _nextAttemptAt?: Date,
    private _processedAt?: Date,
  ) {}

  /** Starts in PENDING. Validates the reference requirement per kind. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (!Object.values(WagerTransactionKind).includes(props.kind)) {
      throw new DomainError(FailureCode.UnsupportedKind, `Unsupported kind ${String(props.kind)}`);
    }
    if (props.kind === WagerTransactionKind.Opening) {
      throw new DomainError(FailureCode.UnsupportedKind, 'OPENING is internal and cannot be submitted');
    }
    if (props.providerId === INTERNAL_PROVIDER_ID) {
      throw new DomainError(FailureCode.InvalidPayload, `providerId "${INTERNAL_PROVIDER_ID}" is reserved`);
    }
    assertText('providerId', props.providerId, MAX_PROVIDER_ID_LENGTH);
    assertText('externalTransactionId', props.externalTransactionId, MAX_IDENTIFIER_LENGTH);
    assertText('idempotencyKey', props.idempotencyKey, MAX_IDEMPOTENCY_KEY_LENGTH);
    assertText('roundId', props.roundId, MAX_IDENTIFIER_LENGTH);
    assertText('gameId', props.gameId, MAX_IDENTIFIER_LENGTH);
    if (!isUuid(props.id) || !isUuid(props.walletId) || !isUuid(props.playerId)) {
      throw new DomainError(FailureCode.InvalidPayload, 'id, walletId and playerId must be UUIDs');
    }
    if (!PAYLOAD_HASH_PATTERN.test(props.payloadHash)) {
      throw new DomainError(FailureCode.InvalidPayload, 'payloadHash must be a SHA-256 hex digest');
    }
    const lossMayBeZero = props.kind === WagerTransactionKind.Loss && props.money.isZero();
    if (!props.money.isPositive() && !lossMayBeZero) {
      throw new DomainError(FailureCode.InvalidAmount, `${props.kind} amount must be positive`);
    }

    const reference = props.referenceExternalTransactionId;
    if (reference !== undefined) {
      assertText('referenceExternalTransactionId', reference, MAX_IDENTIFIER_LENGTH);
      if (ALLOWED_REFERENCE_KINDS[props.kind].length === 0) {
        throw new DomainError(FailureCode.ReferenceNotAllowed, `${props.kind} cannot reference a transaction`);
      }
      if (reference === props.externalTransactionId) {
        throw new DomainError(FailureCode.ReferenceNotAllowed, 'A transaction cannot reference itself');
      }
    } else if (requiresReference(props.kind)) {
      throw new DomainError(
        FailureCode.ReferenceRequired,
        `${props.kind} requires referenceExternalTransactionId`,
      );
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      reference,
      props.now,
      WagerTransactionStatus.Pending,
    );
  }

  /** The internal OPENING credit, already PROCESSED, for a positive initial balance. */
  static opening(props: CreateOpeningProps): WagerTransaction {
    if (!props.money.isPositive()) {
      throw new DomainError(FailureCode.InvalidAmount, 'OPENING amount must be positive');
    }
    const key = `opening:${props.walletId}`;
    return new WagerTransaction(
      props.id,
      INTERNAL_PROVIDER_ID,
      key,
      key,
      canonicalHash({ walletId: props.walletId, playerId: props.playerId, money: props.money.toJSON() }),
      props.walletId,
      props.playerId,
      'opening',
      'wallet-opening',
      WagerTransactionKind.Opening,
      props.money,
      undefined,
      props.now,
      WagerTransactionStatus.Processed,
      undefined,
      undefined,
      props.money,
      0,
      undefined,
      props.now,
    );
  }

  /** Reconstruction from persistence — does not re-validate transitions. */
  static rehydrate(state: WagerTransactionState): WagerTransaction {
    const money = (amount: string) => Money.from({ amount, currency: state.currency });
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      money(state.amount),
      state.referenceExternalTransactionId ?? undefined,
      state.createdAt,
      state.status,
      state.referenceTransactionId ?? undefined,
      state.failureCode ?? undefined,
      state.observedBalanceAmount === null ? undefined : money(state.observedBalanceAmount),
      state.attemptCount,
      state.nextAttemptAt ?? undefined,
      state.processedAt ?? undefined,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  /** Wallet balance observed when the outcome was decided; replays return it unchanged. */
  get observedBalance(): Money | undefined {
    return this._observedBalance;
  }

  get attemptCount(): number {
    return this._attemptCount;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  // ---- transitions (throw InvalidTransactionStateError when the current status is terminal)

  /** PENDING | PENDING_REFERENCE -> PROCESSED */
  markProcessed(referenceTransactionId: string | undefined, observedBalance: Money, at: Date): void {
    this.assertNotTerminal('PROCESSED');
    if (referenceTransactionId === undefined && this.referenceExternalTransactionId !== undefined) {
      throw new InvalidTransactionStateError(
        `Transaction ${this.id} references ${this.referenceExternalTransactionId}; resolve it before processing`,
      );
    }
    if (referenceTransactionId !== undefined && this.referenceExternalTransactionId === undefined) {
      throw new InvalidTransactionStateError(`Transaction ${this.id} has no reference to resolve`);
    }
    this.assertObservedCurrency(observedBalance);
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._observedBalance = observedBalance;
    this._nextAttemptAt = undefined;
    this._processedAt = at;
  }

  /** PENDING | PENDING_REFERENCE -> PENDING_REFERENCE, counting one more resolution attempt. */
  markPendingReference(nextAttemptAt: Date): void {
    this.assertNotTerminal('PENDING_REFERENCE');
    if (this.referenceExternalTransactionId === undefined) {
      throw new InvalidTransactionStateError(`Transaction ${this.id} has no reference to wait for`);
    }
    this._status = WagerTransactionStatus.PendingReference;
    this._attemptCount += 1;
    this._nextAttemptAt = nextAttemptAt;
  }

  /** PENDING | PENDING_REFERENCE -> REJECTED (business rule violation). */
  reject(code: FailureCode, observedBalance: Money): void {
    this.assertNotTerminal('REJECTED');
    this.assertObservedCurrency(observedBalance);
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._observedBalance = observedBalance;
    this._nextAttemptAt = undefined;
  }

  /** PENDING | PENDING_REFERENCE -> FAILED (permanent infrastructure error, kept for audit). */
  fail(code: FailureCode): void {
    this.assertNotTerminal('FAILED');
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._nextAttemptAt = undefined;
  }

  // ---- domain queries

  isTerminal(): boolean {
    return TERMINAL_STATUSES.has(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return requiresReference(this.kind);
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback:
        if (reference === undefined) {
          throw new InvalidTransactionStateError('ROLLBACK direction depends on its reference');
        }
        return reference.ledgerDirectionFor() === LedgerDirection.Debit
          ? LedgerDirection.Credit
          : LedgerDirection.Debit;
      case WagerTransactionKind.Loss:
        throw new InvalidTransactionStateError('LOSS does not move the balance');
    }
  }

  /**
   * Why `reference` cannot back this transaction, or undefined when it can.
   * Does not check "reversed only once": that needs other rows and is enforced
   * by a unique index.
   */
  referenceViolation(reference: WagerTransaction): FailureCode | undefined {
    if (
      reference.providerId !== this.providerId ||
      reference.externalTransactionId !== this.referenceExternalTransactionId
    ) {
      throw new InvalidTransactionStateError(
        `Transaction ${reference.id} is not the reference of ${this.id}`,
      );
    }
    if (
      reference.playerId !== this.playerId ||
      reference.walletId !== this.walletId ||
      reference.money.currency !== this.money.currency ||
      reference.roundId !== this.roundId
    ) {
      return FailureCode.ReferenceScopeMismatch;
    }
    if (!ALLOWED_REFERENCE_KINDS[this.kind].includes(reference.kind)) {
      return FailureCode.ReferenceKindNotAllowed;
    }
    if (this.requiresReference() && !reference.money.equals(this.money)) {
      return FailureCode.AmountMismatch;
    }
    if (reference.status !== WagerTransactionStatus.Processed) {
      return FailureCode.ReferenceNotProcessed;
    }
    return undefined;
  }

  toState(): WagerTransactionState {
    if (this._status === WagerTransactionStatus.Pending) {
      throw new InvalidTransactionStateError(
        `Transaction ${this.id} must be decided before it is persisted`,
      );
    }
    return {
      id: this.id,
      providerId: this.providerId,
      externalTransactionId: this.externalTransactionId,
      idempotencyKey: this.idempotencyKey,
      payloadHash: this.payloadHash,
      walletId: this.walletId,
      playerId: this.playerId,
      roundId: this.roundId,
      gameId: this.gameId,
      kind: this.kind,
      amount: this.money.toJSON().amount,
      currency: this.money.currency,
      referenceExternalTransactionId: this.referenceExternalTransactionId ?? null,
      referenceTransactionId: this._referenceTransactionId ?? null,
      status: this._status,
      failureCode: this._failureCode ?? null,
      observedBalanceAmount: this._observedBalance?.toJSON().amount ?? null,
      attemptCount: this._attemptCount,
      nextAttemptAt: this._nextAttemptAt ?? null,
      processedAt: this._processedAt ?? null,
      createdAt: this.createdAt,
    };
  }

  private assertNotTerminal(target: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(
        `Transaction ${this.id} is ${this._status} (terminal) and cannot become ${target}`,
      );
    }
  }

  private assertObservedCurrency(balance: Money): void {
    if (balance.currency !== this.money.currency) {
      throw new DomainError(
        FailureCode.CurrencyMismatch,
        `Observed balance is ${balance.currency}, transaction is ${this.money.currency}`,
      );
    }
  }
}

function requiresReference(kind: WagerTransactionKind): boolean {
  return kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback;
}

function assertText(field: string, value: string, maxLength: number): void {
  if (typeof value !== 'string' || value.trim().length === 0 || value.length > maxLength) {
    throw new DomainError(
      FailureCode.InvalidPayload,
      `${field} must be a non-blank string up to ${maxLength} characters`,
    );
  }
}
