import { EntitySchema, PrimaryKeyProp } from '@mikro-orm/core';
import type { InboxMessageState } from '../../common/messaging/inbox-message';

export class InboxMessageRecord implements InboxMessageState {
  declare [PrimaryKeyProp]?: ['consumerName', 'messageId'];
  declare consumerName: string;
  declare messageId: string;
  declare payloadHash: string;
  declare receivedAt: Date;
  declare processedAt: Date | null;
}

export const InboxMessageSchema = new EntitySchema<InboxMessageRecord>({
  class: InboxMessageRecord,
  tableName: 'inbox_messages',
  properties: {
    consumerName: { type: 'string', columnType: 'text', primary: true },
    messageId: { type: 'string', columnType: 'text', primary: true },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    receivedAt: { type: 'datetime', columnType: 'timestamptz' },
    processedAt: { type: 'datetime', columnType: 'timestamptz', nullable: true },
  },
});
