import type { EntityManager } from '@mikro-orm/core';
import { InvalidStateError } from '../common/errors/domain.error';
import type { IntegrationEvent } from '../common/messaging/integration-event';
import { OutboxMessage } from '../common/messaging/outbox-message';
import { OutboxMessageRecord } from './entities';

/**
 * Writes the events to the outbox inside the caller's transaction, one INSERT per event and in
 * array order, so sequence_number follows the order the events were produced. A flushed batch
 * of em.create() calls gives no such guarantee.
 */
export async function enqueueEvents(em: EntityManager, events: readonly IntegrationEvent<unknown>[]): Promise<void> {
  if (!em.isInTransaction()) {
    throw new InvalidStateError('Outbox events must be enqueued in the transaction that produced them');
  }
  for (const event of events) {
    await em.insert(OutboxMessageRecord, OutboxMessage.enqueue(event).toState());
  }
}
