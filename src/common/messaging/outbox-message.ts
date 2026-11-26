import { InvalidStateError } from '../errors/domain.error';
import type { IntegrationEvent } from './integration-event';

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Record<string, unknown>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt: Date | null;
  publishedAt: Date | null;
}

const RETRY_BASE_DELAY_MS = 1_000;
const RETRY_MAX_DELAY_MS = 5 * 60_000;

/** Failed attempts after which a message stops being retried automatically. */
export const OUTBOX_MAX_AUTO_ATTEMPTS = 10;
/**
 * next_attempt_at of a quarantined message. It stays pending, so it keeps blocking the later
 * events of its aggregate, and only an operator requeue (moving this date) makes it due again.
 */
export const OUTBOX_QUARANTINED_UNTIL_MS = Date.UTC(9999, 11, 31, 23, 59, 59, 999);

export class OutboxMessage {
  private constructor(
    readonly id: string,
    readonly aggregateId: string,
    readonly eventType: string,
    readonly payload: Readonly<Record<string, unknown>>,
    readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  /** The outbox id is the event id, so one event can only ever be enqueued once. */
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    // Round-trip through JSON so the stored payload is exactly what a consumer will read.
    const payload = JSON.parse(JSON.stringify(event.toJSON())) as Record<string, unknown>;
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      Object.freeze(payload),
      event.occurredAt,
      0,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      Object.freeze({ ...state.payload }),
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt ?? undefined,
      state.publishedAt ?? undefined,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return this.isPending() && (this._nextAttemptAt === undefined || this._nextAttemptAt <= now);
  }

  markPublished(at: Date): void {
    this.assertPending();
    this._publishedAt = at;
  }

  isQuarantined(): boolean {
    return this.isPending() && this._nextAttemptAt?.getTime() === OUTBOX_QUARANTINED_UNTIL_MS;
  }

  /**
   * Increments attempts and schedules the next try with capped exponential backoff, or quarantines
   * the message once attempts reach OUTBOX_MAX_AUTO_ATTEMPTS. Attempts never decrease, so a
   * requeued message that fails again goes straight back to quarantine.
   */
  scheduleRetry(now: Date): void {
    this.assertPending();
    this._attempts += 1;
    if (this._attempts >= OUTBOX_MAX_AUTO_ATTEMPTS) {
      this._nextAttemptAt = new Date(OUTBOX_QUARANTINED_UNTIL_MS);
      return;
    }
    const delay = Math.min(RETRY_BASE_DELAY_MS * 2 ** (this._attempts - 1), RETRY_MAX_DELAY_MS);
    this._nextAttemptAt = new Date(now.getTime() + delay);
  }

  toState(): OutboxMessageState {
    return {
      id: this.id,
      aggregateId: this.aggregateId,
      eventType: this.eventType,
      payload: { ...this.payload },
      occurredAt: this.occurredAt,
      attempts: this._attempts,
      nextAttemptAt: this._nextAttemptAt ?? null,
      publishedAt: this._publishedAt ?? null,
    };
  }

  private assertPending(): void {
    if (!this.isPending()) {
      throw new InvalidStateError(`Outbox message ${this.id} is already published`);
    }
  }
}
