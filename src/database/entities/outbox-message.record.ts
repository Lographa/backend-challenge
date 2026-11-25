import { EntitySchema } from '@mikro-orm/core';
import type { OutboxMessageState } from '../../common/messaging/outbox-message';

export class OutboxMessageRecord implements OutboxMessageState {
  declare id: string;
  declare aggregateId: string;
  declare eventType: string;
  declare payload: Record<string, unknown>;
  declare occurredAt: Date;
  declare attempts: number;
  declare nextAttemptAt: Date | null;
  declare publishedAt: Date | null;
}

export const OutboxMessageSchema = new EntitySchema<OutboxMessageRecord>({
  class: OutboxMessageRecord,
  tableName: 'outbox_messages',
  properties: {
    id: { type: 'uuid', primary: true },
    aggregateId: { type: 'uuid' },
    eventType: { type: 'string', columnType: 'text' },
    payload: { type: 'json', columnType: 'jsonb' },
    occurredAt: { type: 'datetime', columnType: 'timestamptz' },
    attempts: { type: 'integer' },
    nextAttemptAt: { type: 'datetime', columnType: 'timestamptz', nullable: true },
    publishedAt: { type: 'datetime', columnType: 'timestamptz', nullable: true },
  },
});
