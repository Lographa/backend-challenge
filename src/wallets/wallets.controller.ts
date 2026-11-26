import { Body, Controller, Get, HttpCode, HttpStatus, Param, Post, Query } from '@nestjs/common';
import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { CorrelationId } from '../common/http/correlation';
import { isUuid } from '../common/ids';
import { Money, type MoneyProps } from '../common/money/money';
import { decodeLedgerCursor, parseLedgerLimit } from './ledger-page';
import type { Wallet } from './wallet';
import type { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';
import type { WalletReconciliation } from './wallet-reconciliation';
import { WalletsService } from './wallets.service';

export interface WalletView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
  createdAt: string;
}

export interface LedgerPageView {
  items: LedgerEntryView[];
  /** Absent once the last entry has been returned. */
  nextCursor: string | null;
}

@Controller('wallets')
export class WalletsController {
  constructor(private readonly wallets: WalletsService) {}

  @Post()
  async open(@Body() body: unknown, @CorrelationId() correlationId: string): Promise<WalletView> {
    const { playerId, initialBalance } = parseOpenWallet(body);
    return walletView(await this.wallets.open(playerId, initialBalance, correlationId));
  }

  @Get(':walletId')
  async get(@Param('walletId') walletId: string): Promise<WalletView> {
    return walletView(await this.wallets.get(walletId));
  }

  @Get(':walletId/ledger')
  async ledger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor: unknown,
    @Query('limit') limit: unknown,
  ): Promise<LedgerPageView> {
    const afterVersion = cursor === undefined ? 0 : decodeLedgerCursor(cursor);
    const page = await this.wallets.ledger(walletId, afterVersion, parseLedgerLimit(limit));
    return { items: page.entries.map(ledgerEntryView), nextCursor: page.nextCursor };
  }

  @Post(':walletId/reconciliation')
  @HttpCode(HttpStatus.OK)
  reconcile(
    @Param('walletId') walletId: string,
    @CorrelationId() correlationId: string,
  ): Promise<WalletReconciliation> {
    return this.wallets.reconcile(walletId, correlationId);
  }
}

function parseOpenWallet(body: unknown): { playerId: string; initialBalance: Money } {
  const { playerId, initialBalance } = (typeof body === 'object' && body !== null ? body : {}) as Record<
    string,
    unknown
  >;
  if (!isUuid(playerId)) {
    throw new DomainError(FailureCode.InvalidPayload, 'playerId must be a UUID');
  }
  if (typeof initialBalance !== 'object' || initialBalance === null) {
    throw new DomainError(FailureCode.InvalidPayload, 'initialBalance must be { amount, currency }');
  }
  return { playerId, initialBalance: Money.from(initialBalance as MoneyProps) };
}

function walletView(wallet: Wallet): WalletView {
  return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
}

function ledgerEntryView(entry: WalletLedgerEntry): LedgerEntryView {
  return {
    id: entry.id,
    transactionId: entry.transactionId,
    direction: entry.direction,
    money: entry.money.toJSON(),
    balanceBefore: entry.balanceBefore.toJSON(),
    balanceAfter: entry.balanceAfter.toJSON(),
    walletVersion: entry.walletVersion,
    createdAt: entry.createdAt.toISOString(),
  };
}
