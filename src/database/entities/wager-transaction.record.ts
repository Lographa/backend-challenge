import { EntitySchema } from '@mikro-orm/core';
import type { FailureCode } from '../../common/errors/failure-code';
import type {
  WagerTransactionKind,
  WagerTransactionState,
  WagerTransactionStatus,
} from '../../wagering/wager-transaction';
import { DecimalType } from '../decimal.type';

export class WagerTransactionRecord implements WagerTransactionState {
  declare id: string;
  declare providerId: string;
  declare externalTransactionId: string;
  declare idempotencyKey: string;
  declare payloadHash: string;
  declare walletId: string;
  declare playerId: string;
  declare roundId: string;
  declare gameId: string;
  declare kind: WagerTransactionKind;
  declare amount: string;
  declare currency: string;
  declare referenceExternalTransactionId: string | null;
  declare referenceTransactionId: string | null;
  declare status: WagerTransactionStatus;
  declare failureCode: FailureCode | null;
  declare observedBalanceAmount: string | null;
  declare attemptCount: number;
  declare nextAttemptAt: Date | null;
  declare processedAt: Date | null;
  declare createdAt: Date;
}

export const WagerTransactionSchema = new EntitySchema<WagerTransactionRecord>({
  class: WagerTransactionRecord,
  tableName: 'wager_transactions',
  properties: {
    id: { type: 'uuid', primary: true },
    providerId: { type: 'string', columnType: 'text' },
    externalTransactionId: { type: 'string', columnType: 'text' },
    idempotencyKey: { type: 'string', columnType: 'text' },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    walletId: { type: 'uuid' },
    playerId: { type: 'uuid' },
    roundId: { type: 'string', columnType: 'text' },
    gameId: { type: 'string', columnType: 'text' },
    kind: { type: 'string', columnType: 'text' },
    amount: { type: DecimalType },
    currency: { type: 'string', columnType: 'char(3)' },
    referenceExternalTransactionId: { type: 'string', columnType: 'text', nullable: true },
    referenceTransactionId: { type: 'uuid', nullable: true },
    status: { type: 'string', columnType: 'text' },
    failureCode: { type: 'string', columnType: 'text', nullable: true },
    observedBalanceAmount: { type: DecimalType, nullable: true },
    attemptCount: { type: 'integer' },
    nextAttemptAt: { type: 'datetime', columnType: 'timestamptz', nullable: true },
    processedAt: { type: 'datetime', columnType: 'timestamptz', nullable: true },
    createdAt: { type: 'datetime', columnType: 'timestamptz' },
  },
});
