import { EntitySchema } from '@mikro-orm/core';
import type { LedgerDirection, LedgerEntryState } from '../../wallets/wallet-ledger-entry';
import { DecimalType } from '../decimal.type';

export class WalletLedgerEntryRecord implements LedgerEntryState {
  declare id: string;
  declare walletId: string;
  declare transactionId: string;
  declare direction: LedgerDirection;
  declare amount: string;
  declare currency: string;
  declare balanceBefore: string;
  declare balanceAfter: string;
  declare walletVersion: number;
  declare createdAt: Date;
}

export const WalletLedgerEntrySchema = new EntitySchema<WalletLedgerEntryRecord>({
  class: WalletLedgerEntryRecord,
  tableName: 'wallet_ledger_entries',
  properties: {
    id: { type: 'uuid', primary: true },
    walletId: { type: 'uuid' },
    transactionId: { type: 'uuid' },
    direction: { type: 'string', columnType: 'text' },
    amount: { type: DecimalType },
    currency: { type: 'string', columnType: 'char(3)' },
    balanceBefore: { type: DecimalType },
    balanceAfter: { type: DecimalType },
    walletVersion: { type: 'integer' },
    createdAt: { type: 'datetime', columnType: 'timestamptz' },
  },
});
