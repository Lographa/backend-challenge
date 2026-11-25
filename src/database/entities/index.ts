import { InboxMessageSchema } from './inbox-message.record';
import { OutboxMessageSchema } from './outbox-message.record';
import { WagerTransactionSchema } from './wager-transaction.record';
import { WalletLedgerEntrySchema } from './wallet-ledger-entry.record';
import { WalletSchema } from './wallet.record';

export { InboxMessageRecord } from './inbox-message.record';
export { OutboxMessageRecord } from './outbox-message.record';
export { WagerTransactionRecord } from './wager-transaction.record';
export { WalletLedgerEntryRecord } from './wallet-ledger-entry.record';
export { WalletRecord } from './wallet.record';

export const entitySchemas = [
  WalletSchema,
  WagerTransactionSchema,
  WalletLedgerEntrySchema,
  InboxMessageSchema,
  OutboxMessageSchema,
];
