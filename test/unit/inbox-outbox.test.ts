import { describe, expect, test } from 'bun:test';
import { InvalidStateError } from '../../src/common/errors/domain.error';
import { canonicalHash } from '../../src/common/hashing/canonical-hash';
import { newId } from '../../src/common/ids';
import { InboxMessage } from '../../src/common/messaging/inbox-message';
import { IntegrationEvent, type IntegrationEventProps } from '../../src/common/messaging/integration-event';
import {
  OUTBOX_MAX_AUTO_ATTEMPTS,
  OUTBOX_QUARANTINED_UNTIL_MS,
  OutboxMessage,
} from '../../src/common/messaging/outbox-message';

const t0 = new Date('2026-01-01T00:00:00.000Z');
const t1 = new Date('2026-01-01T00:00:10.000Z');

interface SampleData {
  amount: string;
  nested: { tags: string[] };
}

class SampleEvent extends IntegrationEvent<SampleData> {
  readonly eventType = 'sample.happened';
  readonly version = 1;

  constructor(props: IntegrationEventProps<SampleData>) {
    super(props);
  }
}

function sampleEvent(overrides: Partial<IntegrationEventProps<SampleData>> = {}) {
  return new SampleEvent({
    eventId: newId(),
    aggregateId: newId(),
    correlationId: newId(),
    occurredAt: t0,
    data: { amount: '10.00', nested: { tags: ['a'] } },
    ...overrides,
  });
}

describe('InboxMessage', () => {
  const props = { messageId: 'msg-1', consumerName: 'wager-processor', payloadHash: canonicalHash({ a: 1 }), receivedAt: t0 };

  test('receive starts unprocessed and can be processed once', () => {
    const message = InboxMessage.receive(props);
    expect(message.isProcessed()).toBe(false);

    message.markProcessed(t1);
    expect(message.toState()).toEqual({ ...props, processedAt: t1 });
    expect(() => message.markProcessed(t1)).toThrow(InvalidStateError);
    expect(message.processedAt).toBe(t1);
  });

  test.each([
    ['messageId', ''],
    ['messageId', 'm'.repeat(256)],
    ['consumerName', ''],
    ['payloadHash', 'F'.repeat(64)],
    ['payloadHash', 'abc'],
  ])('rejects an invalid %s', (field, value) => {
    expect(() => InboxMessage.receive({ ...props, [field]: value })).toThrow(RangeError);
  });

  test('rehydrate restores both processed and unprocessed messages', () => {
    const pending = InboxMessage.receive(props).toState();
    const done = { ...pending, processedAt: t1 };

    expect(InboxMessage.rehydrate(pending).toState()).toEqual(pending);
    expect(InboxMessage.rehydrate(done).isProcessed()).toBe(true);
    expect(() => InboxMessage.rehydrate(done).markProcessed(t1)).toThrow(InvalidStateError);
  });
});

describe('IntegrationEvent', () => {
  test('serializes an envelope with an ISO timestamp and the schema version', () => {
    const causationId = newId();
    const event = sampleEvent({ causationId });
    expect(event.toJSON()).toEqual({
      eventId: event.eventId,
      eventType: 'sample.happened',
      aggregateId: event.aggregateId,
      correlationId: event.correlationId,
      causationId,
      occurredAt: '2026-01-01T00:00:00.000Z',
      version: 1,
      data: { amount: '10.00', nested: { tags: ['a'] } },
    });
  });

  test('omits causationId when absent and freezes its data', () => {
    const event = sampleEvent();
    expect('causationId' in event.toJSON()).toBe(false);
    expect(Object.isFrozen(event.data)).toBe(true);
  });
});

describe('OutboxMessage', () => {
  test('enqueue uses the event id and stores the serialized envelope', () => {
    const event = sampleEvent();
    const message = OutboxMessage.enqueue(event);

    expect(message.toState()).toEqual({
      id: event.eventId,
      aggregateId: event.aggregateId,
      eventType: 'sample.happened',
      payload: JSON.parse(JSON.stringify(event)),
      occurredAt: t0,
      attempts: 0,
      nextAttemptAt: null,
      publishedAt: null,
    });
    expect(Object.isFrozen(message.payload)).toBe(true);
    expect(message.isPending()).toBe(true);
    expect(message.isDue(t0)).toBe(true);
  });

  test('retries back off exponentially until the automatic attempts run out', () => {
    const message = OutboxMessage.enqueue(sampleEvent());
    const delays: number[] = [];
    for (let i = 1; i < OUTBOX_MAX_AUTO_ATTEMPTS; i++) {
      message.scheduleRetry(t0);
      delays.push(message.nextAttemptAt!.getTime() - t0.getTime());
    }

    expect(delays).toEqual([1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000]);
    expect(message.attempts).toBe(OUTBOX_MAX_AUTO_ATTEMPTS - 1);
    expect(message.isQuarantined()).toBe(false);
  });

  test('is quarantined at the attempt limit: still pending, never due, attempts kept', () => {
    const message = OutboxMessage.enqueue(sampleEvent());
    for (let i = 0; i < OUTBOX_MAX_AUTO_ATTEMPTS; i++) {
      message.scheduleRetry(t0);
    }

    expect(message.attempts).toBe(OUTBOX_MAX_AUTO_ATTEMPTS);
    expect(message.isQuarantined()).toBe(true);
    expect(message.isPending()).toBe(true);
    expect(message.nextAttemptAt!.toISOString()).toBe('9999-12-31T23:59:59.999Z');
    expect(message.isDue(new Date('9999-12-31T23:59:59.998Z'))).toBe(false);

    message.scheduleRetry(t1);
    expect(message.attempts).toBe(OUTBOX_MAX_AUTO_ATTEMPTS + 1);
    expect(message.isQuarantined()).toBe(true);
  });

  test('a requeued message is due again and returns to quarantine on its next failure', () => {
    const quarantined = OutboxMessage.enqueue(sampleEvent());
    for (let i = 0; i < OUTBOX_MAX_AUTO_ATTEMPTS; i++) {
      quarantined.scheduleRetry(t0);
    }
    const requeued = OutboxMessage.rehydrate({ ...quarantined.toState(), nextAttemptAt: t1 });

    expect(requeued.isQuarantined()).toBe(false);
    expect(requeued.isDue(t1)).toBe(true);
    requeued.scheduleRetry(t1);
    expect(requeued.isQuarantined()).toBe(true);
    expect(requeued.attempts).toBe(OUTBOX_MAX_AUTO_ATTEMPTS + 1);
  });

  test('a published message is not quarantined', () => {
    const message = OutboxMessage.rehydrate({
      ...OutboxMessage.enqueue(sampleEvent()).toState(),
      attempts: OUTBOX_MAX_AUTO_ATTEMPTS,
      nextAttemptAt: new Date(OUTBOX_QUARANTINED_UNTIL_MS),
      publishedAt: t1,
    });
    expect(message.isQuarantined()).toBe(false);
  });

  test('is not due before the scheduled retry', () => {
    const message = OutboxMessage.enqueue(sampleEvent());
    message.scheduleRetry(t0);
    expect(message.isDue(new Date(t0.getTime() + 999))).toBe(false);
    expect(message.isDue(new Date(t0.getTime() + 1_000))).toBe(true);
  });

  test('a published message is final', () => {
    const message = OutboxMessage.enqueue(sampleEvent());
    message.markPublished(t1);

    expect(message.isPending()).toBe(false);
    expect(message.isDue(t1)).toBe(false);
    expect(() => message.markPublished(t1)).toThrow(InvalidStateError);
    expect(() => message.scheduleRetry(t1)).toThrow(InvalidStateError);
  });

  test('rehydrate restores the persisted state', () => {
    const message = OutboxMessage.enqueue(sampleEvent());
    message.scheduleRetry(t0);
    message.scheduleRetry(t0);

    const restored = OutboxMessage.rehydrate(message.toState());
    expect(restored.toState()).toEqual(message.toState());
    restored.scheduleRetry(t0);
    expect(restored.attempts).toBe(3);
  });
});
