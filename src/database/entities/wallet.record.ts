import { EntitySchema } from '@mikro-orm/core';
import type { WalletState } from '../../wallets/wallet';
import { DecimalType } from '../decimal.type';

export class WalletRecord implements WalletState {
  declare id: string;
  declare playerId: string;
  declare currency: string;
  declare balanceAmount: string;
  declare version: number;
  declare createdAt: Date;
  declare updatedAt: Date;
}

export const WalletSchema = new EntitySchema<WalletRecord>({
  class: WalletRecord,
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid' },
    currency: { type: 'string', columnType: 'char(3)' },
    balanceAmount: { type: DecimalType },
    version: { type: 'integer' },
    createdAt: { type: 'datetime', columnType: 'timestamptz' },
    updatedAt: { type: 'datetime', columnType: 'timestamptz' },
  },
});
