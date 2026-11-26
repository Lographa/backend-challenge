import { IsolationLevel, QueryOrder, UniqueConstraintViolationException } from '@mikro-orm/core';
import { type EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Injectable, Logger } from '@nestjs/common';
import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { isUuid, newId } from '../common/ids';
import type { EventContext } from '../common/messaging/integration-event';
import type { Money } from '../common/money/money';
import { WagerTransactionRecord, WalletLedgerEntryRecord, WalletRecord } from '../database/entities';
import { enqueueEvents } from '../database/outbox';
import { WagerTransactionProcessed } from '../wagering/wager-events';
import { WagerTransaction } from '../wagering/wager-transaction';
import { encodeLedgerCursor } from './ledger-page';
import { Wallet } from './wallet';
import { WalletBalanceChanged } from './wallet-events';
import { WalletLedgerEntry } from './wallet-ledger-entry';
import { reconcileWallet, type WalletReconciliation } from './wallet-reconciliation';

const PLAYER_CURRENCY_CONSTRAINT = 'wallets_player_currency_uq';

export interface LedgerPage {
  entries: WalletLedgerEntry[];
  nextCursor: string | null;
}

@Injectable()
export class WalletsService {
  private readonly logger = new Logger(WalletsService.name);

  constructor(private readonly orm: MikroORM) {}

  /**
   * Wallet, OPENING transaction, its ledger entry and events commit together. The statements run
   * in lock order (wallet, transaction, ledger, outbox); a concurrent opening for the same player
   * and currency waits on the unique index and fails once this one commits.
   */
  async open(playerId: string, initialBalance: Money, correlationId: string): Promise<Wallet> {
    const now = new Date();
    const ctx: EventContext = { correlationId, occurredAt: now };
    const walletId = newId();
    const openingId = initialBalance.isPositive() ? newId() : undefined;
    const { wallet, openingEntry } = Wallet.open({
      id: walletId,
      playerId,
      initialBalance,
      openingTransactionId: openingId,
      now,
    });

    try {
      await this.orm.em.fork().transactional(async (em) => {
        await em.insert(WalletRecord, wallet.toState());
        if (openingId === undefined || openingEntry === undefined) {
          return;
        }
        const opening = WagerTransaction.opening({ id: openingId, walletId, playerId, money: initialBalance, now });
        await em.insert(WagerTransactionRecord, opening.toState());
        await em.insert(WalletLedgerEntryRecord, openingEntry.toState());
        await enqueueEvents(em, [
          WagerTransactionProcessed.from(opening, ctx),
          WalletBalanceChanged.from(wallet, openingEntry, ctx),
        ]);
      });
    } catch (error) {
      if (
        error instanceof UniqueConstraintViolationException &&
        (error as { constraint?: unknown }).constraint === PLAYER_CURRENCY_CONSTRAINT
      ) {
        throw new DomainError(
          FailureCode.WalletAlreadyExists,
          `Player ${playerId} already has a ${wallet.currency} wallet`,
        );
      }
      throw error;
    }
    return wallet;
  }

  async get(walletId: string): Promise<Wallet> {
    assertWalletId(walletId);
    return this.findWallet(this.orm.em.fork(), walletId);
  }

  /** Entries in wallet version order, starting after `afterVersion`. */
  async ledger(walletId: string, afterVersion: number, limit: number): Promise<LedgerPage> {
    assertWalletId(walletId);
    const em = this.orm.em.fork();
    await this.findWallet(em, walletId);
    const records = await em.find(
      WalletLedgerEntryRecord,
      { walletId, walletVersion: { $gt: afterVersion } },
      { orderBy: { walletVersion: QueryOrder.ASC }, limit: limit + 1 },
    );
    const entries = records.slice(0, limit).map((record) => WalletLedgerEntry.rehydrate(record));
    const last = entries.at(-1);
    return {
      entries,
      nextCursor: records.length > limit && last !== undefined ? encodeLedgerCursor(last.walletVersion) : null,
    };
  }

  /**
   * Reads the wallet and its ledger from one snapshot, so a movement committed in between cannot
   * look like a divergence. A divergence is reported and logged, never corrected here.
   */
  async reconcile(walletId: string, correlationId: string): Promise<WalletReconciliation> {
    assertWalletId(walletId);
    const report = await this.orm.em.fork().transactional(
      async (em) => {
        const wallet = await this.findWallet(em, walletId);
        const records = await em.find(
          WalletLedgerEntryRecord,
          { walletId },
          { orderBy: { walletVersion: QueryOrder.ASC } },
        );
        return reconcileWallet(
          wallet,
          records.map((record) => WalletLedgerEntry.rehydrate(record)),
        );
      },
      { isolationLevel: IsolationLevel.REPEATABLE_READ, readOnly: true },
    );
    if (!report.consistent) {
      this.logger.warn({
        message: 'Wallet balance diverges from its ledger',
        correlationId,
        walletId,
        checkedEntries: report.checkedEntries,
        difference: report.difference.amount,
      });
    }
    return report;
  }

  private async findWallet(em: EntityManager, walletId: string): Promise<Wallet> {
    const record = await em.findOne(WalletRecord, walletId);
    if (record === null) {
      throw new DomainError(FailureCode.WalletNotFound, `Wallet ${walletId} does not exist`);
    }
    return Wallet.rehydrate(record);
  }
}

function assertWalletId(walletId: string): void {
  if (!isUuid(walletId)) {
    throw new DomainError(FailureCode.InvalidPayload, 'walletId must be a UUID');
  }
}
