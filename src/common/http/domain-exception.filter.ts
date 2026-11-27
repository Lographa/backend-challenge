import type { IncomingMessage, ServerResponse } from 'node:http';
import { type ArgumentsHost, Catch, HttpException, HttpStatus, Inject, Logger, Optional } from '@nestjs/common';
import { BaseExceptionFilter } from '@nestjs/core';
import { DomainError } from '../errors/domain.error';
import { FailureCode } from '../errors/failure-code';
import { isTransientInfrastructureError, lockConflictSqlState } from '../errors/transient-error';
import { safeErrorFields } from '../observability/log-fields';
import { guardMetrics, MetricsRegistry } from '../observability/metrics';
import { assignCorrelationId } from './correlation';

/**
 * 400: the request is malformed and must be fixed. 404: the target does not exist. 409: the
 * request conflicts with one already recorded. 422: well formed, but a business rule refuses it.
 */
export const HTTP_STATUS_BY_FAILURE_CODE: Readonly<Record<FailureCode, HttpStatus>> = {
  [FailureCode.InvalidPayload]: HttpStatus.BAD_REQUEST,
  [FailureCode.InvalidAmount]: HttpStatus.BAD_REQUEST,
  [FailureCode.InvalidCurrency]: HttpStatus.BAD_REQUEST,
  [FailureCode.UnsupportedKind]: HttpStatus.BAD_REQUEST,
  [FailureCode.ReferenceRequired]: HttpStatus.BAD_REQUEST,
  [FailureCode.ReferenceNotAllowed]: HttpStatus.BAD_REQUEST,
  [FailureCode.WalletNotFound]: HttpStatus.NOT_FOUND,
  [FailureCode.TransactionNotFound]: HttpStatus.NOT_FOUND,
  [FailureCode.IdempotencyPayloadConflict]: HttpStatus.CONFLICT,
  [FailureCode.WalletAlreadyExists]: HttpStatus.CONFLICT,
  [FailureCode.InsufficientFunds]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.NegativeBalanceReversal]: HttpStatus.UNPROCESSABLE_ENTITY,
  // Amounts are parsed within range; only a balance that would overflow raises this.
  [FailureCode.AmountOutOfRange]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.CurrencyMismatch]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.WalletPlayerMismatch]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.ReferenceNotFound]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.ReferenceNotProcessed]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.ReferenceKindNotAllowed]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.ReferenceScopeMismatch]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.ReferenceAlreadyReversed]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.AmountMismatch]: HttpStatus.UNPROCESSABLE_ENTITY,
  [FailureCode.PermanentIntegrityError]: HttpStatus.INTERNAL_SERVER_ERROR,
};

/** Codes for failures that are not business outcomes, so they are not FailureCodes. */
export const SERVICE_UNAVAILABLE_CODE = 'SERVICE_UNAVAILABLE';
export const INTERNAL_ERROR_CODE = 'INTERNAL_ERROR';

export interface ErrorBody {
  code: string;
  message: string;
  correlationId: string;
}

export interface ErrorResponse {
  status: HttpStatus;
  body: ErrorBody;
}

/** Response for any error that is not an HttpException. Only domain messages reach the caller. */
export function errorResponse(error: unknown, correlationId: string): ErrorResponse {
  if (error instanceof DomainError) {
    return {
      status: HTTP_STATUS_BY_FAILURE_CODE[error.code],
      body: { code: error.code, message: error.message, correlationId },
    };
  }
  if (isTransientInfrastructureError(error)) {
    return {
      status: HttpStatus.SERVICE_UNAVAILABLE,
      body: { code: SERVICE_UNAVAILABLE_CODE, message: 'A dependency is temporarily unavailable; retry later', correlationId },
    };
  }
  return {
    status: HttpStatus.INTERNAL_SERVER_ERROR,
    body: { code: INTERNAL_ERROR_CODE, message: 'Internal server error', correlationId },
  };
}

/**
 * Domain and infrastructure errors become `{ code, message, correlationId }`. HttpExceptions
 * (routing, readiness) keep Nest's default handling so their bodies are unchanged.
 */
@Catch()
export class DomainExceptionFilter extends BaseExceptionFilter<unknown> {
  private readonly logger = new Logger(DomainExceptionFilter.name);

  constructor(@Optional() @Inject(MetricsRegistry) private readonly metrics?: MetricsRegistry) {
    super();
  }

  override catch(exception: unknown, host: ArgumentsHost): void {
    const http = host.switchToHttp();
    const request = http.getRequest<IncomingMessage>();
    const response = http.getResponse<ServerResponse>();
    const correlationId = assignCorrelationId(request, response);
    if (exception instanceof HttpException || this.isHttpError(exception)) {
      super.catch(exception, host);
      return;
    }

    const { status, body } = errorResponse(exception, correlationId);
    if (status >= HttpStatus.INTERNAL_SERVER_ERROR) {
      this.logFailure(exception, status, body.code, request, correlationId);
    }
    const adapter = this.httpAdapterHost?.httpAdapter ?? this.applicationRef;
    if (adapter === undefined) {
      throw new Error('DomainExceptionFilter must be registered through dependency injection');
    }
    if (adapter.isHeadersSent(response)) {
      adapter.end(response);
      return;
    }
    adapter.reply(response, body, status);
  }

  private logFailure(
    exception: unknown,
    status: HttpStatus,
    code: string,
    request: IncomingMessage,
    correlationId: string,
  ): void {
    const sqlstate = lockConflictSqlState(exception);
    guardMetrics(() => {
      if (sqlstate !== undefined) {
        this.metrics?.increment('wager_lock_conflicts_total', { sqlstate });
      }
    });
    const fields = {
      correlationId,
      method: request.method,
      path: request.url?.split('?')[0],
      status,
      code,
      ...safeErrorFields(exception),
    };
    try {
      if (status === HttpStatus.SERVICE_UNAVAILABLE) {
        this.logger.warn('Request failed', fields);
      } else {
        this.logger.error('Request failed', fields);
      }
    } catch {
      // Logging must not change the HTTP response already decided above.
    }
  }
}
