import type { ServerResponse } from 'node:http';
import { Body, Controller, Get, Headers, HttpStatus, Param, Post, Res } from '@nestjs/common';
import type { FailureCode } from '../common/errors/failure-code';
import { CorrelationId } from '../common/http/correlation';
import { HTTP_STATUS_BY_FAILURE_CODE } from '../common/http/domain-exception.filter';
import type { MoneyProps } from '../common/money/money';
import { InvalidStateError } from '../common/errors/domain.error';
import { parseWagerRequest } from './wager-payload';
import { type ProcessWagerResult, WagerProcessorService } from './wager-processor.service';
import {
  type WagerTransaction,
  type WagerTransactionKind,
  WagerTransactionStatus,
} from './wager-transaction';

export type SubmitWagerResponseView =
  | {
      transactionId: string;
      status: WagerTransactionStatus.Processed;
      balance: MoneyProps;
      idempotentReplay: boolean;
    }
  | {
      transactionId: string;
      status: WagerTransactionStatus.PendingReference;
      idempotentReplay: boolean;
    }
  | {
      transactionId: string;
      status: WagerTransactionStatus.Rejected;
      failureCode: FailureCode;
      balance: MoneyProps;
      idempotentReplay: boolean;
    }
  | {
      transactionId: string;
      status: WagerTransactionStatus.Failed;
      failureCode: FailureCode;
      idempotentReplay: boolean;
    };

export interface WagerTransactionView {
  id: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  status: WagerTransactionStatus;
  money: MoneyProps;
  observedBalance: MoneyProps | null;
  referenceExternalTransactionId: string | null;
  referenceTransactionId: string | null;
  failureCode: FailureCode | null;
  createdAt: string;
  processedAt: string | null;
}

@Controller()
export class WageringController {
  constructor(private readonly processor: WagerProcessorService) {}

  @Post('wagering/transactions')
  async submit(
    @Headers('idempotency-key') idempotencyKey: string | string[] | undefined,
    @Body() body: unknown,
    @CorrelationId() correlationId: string,
    @Res({ passthrough: true }) response: ServerResponse,
  ): Promise<SubmitWagerResponseView> {
    const parsed = parseWagerRequest(body, idempotencyKey);
    const result = await this.processor.process({ ...parsed, correlationId });
    return toSubmitResponse(result, response);
  }

  @Get('wagering/transactions/:transactionId')
  async getById(@Param('transactionId') transactionId: string): Promise<WagerTransactionView> {
    return transactionView(await this.processor.getById(transactionId));
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getByExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ): Promise<WagerTransactionView> {
    return transactionView(await this.processor.getByExternalId(providerId, externalTransactionId));
  }
}

function toSubmitResponse(result: ProcessWagerResult, response: ServerResponse): SubmitWagerResponseView {
  const { transaction, idempotentReplay } = result;
  switch (transaction.status) {
    case WagerTransactionStatus.Processed: {
      if (transaction.observedBalance === undefined) {
        throw new InvalidStateError(`Processed transaction ${transaction.id} is missing observedBalance`);
      }
      response.statusCode = HttpStatus.OK;
      return {
        transactionId: transaction.id,
        status: WagerTransactionStatus.Processed,
        balance: transaction.observedBalance.toJSON(),
        idempotentReplay,
      };
    }
    case WagerTransactionStatus.PendingReference: {
      response.statusCode = HttpStatus.ACCEPTED;
      return {
        transactionId: transaction.id,
        status: WagerTransactionStatus.PendingReference,
        idempotentReplay,
      };
    }
    case WagerTransactionStatus.Rejected: {
      if (transaction.failureCode === undefined || transaction.observedBalance === undefined) {
        throw new InvalidStateError(`Rejected transaction ${transaction.id} is missing failureCode or observedBalance`);
      }
      response.statusCode = HttpStatus.UNPROCESSABLE_ENTITY;
      return {
        transactionId: transaction.id,
        status: WagerTransactionStatus.Rejected,
        failureCode: transaction.failureCode,
        balance: transaction.observedBalance.toJSON(),
        idempotentReplay,
      };
    }
    case WagerTransactionStatus.Failed: {
      if (transaction.failureCode === undefined) {
        throw new InvalidStateError(`Failed transaction ${transaction.id} is missing failureCode`);
      }
      response.statusCode = HTTP_STATUS_BY_FAILURE_CODE[transaction.failureCode];
      return {
        transactionId: transaction.id,
        status: WagerTransactionStatus.Failed,
        failureCode: transaction.failureCode,
        idempotentReplay,
      };
    }
    case WagerTransactionStatus.Pending:
      throw new InvalidStateError(`Transaction ${transaction.id} remained in transient PENDING status`);
  }
}

function transactionView(tx: WagerTransaction): WagerTransactionView {
  return {
    id: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    status: tx.status,
    money: tx.money.toJSON(),
    observedBalance: tx.observedBalance?.toJSON() ?? null,
    referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
    referenceTransactionId: tx.referenceTransactionId ?? null,
    failureCode: tx.failureCode ?? null,
    createdAt: tx.createdAt.toISOString(),
    processedAt: tx.processedAt?.toISOString() ?? null,
  };
}
