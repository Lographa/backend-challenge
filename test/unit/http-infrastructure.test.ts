import 'reflect-metadata';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { describe, expect, test } from 'bun:test';
import { DeadlockException, DriverException } from '@mikro-orm/core';
import type { ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { AuthGuard, IS_PUBLIC_KEY, Public } from '../../src/common/auth/auth.guard';
import { DomainError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { isTransientInfrastructureError } from '../../src/common/errors/transient-error';
import { HealthController } from '../../src/common/health/health.controller';
import { assignCorrelationId, CORRELATION_ID_HEADER } from '../../src/common/http/correlation';
import { errorResponse, HTTP_STATUS_BY_FAILURE_CODE } from '../../src/common/http/domain-exception.filter';
import { isUuid } from '../../src/common/ids';

const EXPECTED_STATUS: Record<FailureCode, number> = {
  INVALID_PAYLOAD: 400,
  INVALID_AMOUNT: 400,
  INVALID_CURRENCY: 400,
  UNSUPPORTED_KIND: 400,
  REFERENCE_REQUIRED: 400,
  REFERENCE_NOT_ALLOWED: 400,
  WALLET_NOT_FOUND: 404,
  TRANSACTION_NOT_FOUND: 404,
  IDEMPOTENCY_PAYLOAD_CONFLICT: 409,
  WALLET_ALREADY_EXISTS: 409,
  INSUFFICIENT_FUNDS: 422,
  NEGATIVE_BALANCE_REVERSAL: 422,
  AMOUNT_OUT_OF_RANGE: 422,
  CURRENCY_MISMATCH: 422,
  WALLET_PLAYER_MISMATCH: 422,
  REFERENCE_NOT_FOUND: 422,
  REFERENCE_NOT_PROCESSED: 422,
  REFERENCE_KIND_NOT_ALLOWED: 422,
  REFERENCE_SCOPE_MISMATCH: 422,
  REFERENCE_ALREADY_REVERSED: 422,
  AMOUNT_MISMATCH: 422,
  PERMANENT_INTEGRITY_ERROR: 500,
};

function driverError(code: string): DriverException {
  return new DriverException(Object.assign(new Error(`insert into wallets values ('secret') - ${code}`), { code }));
}

describe('FailureCode to HTTP status', () => {
  test('every failure code has a status', () => {
    expect(Object.keys(HTTP_STATUS_BY_FAILURE_CODE).sort()).toEqual(Object.values(FailureCode).sort());
  });

  test.each(Object.entries(EXPECTED_STATUS))('%s -> %d', (code, status) => {
    const error = new DomainError(code as FailureCode, `because ${code}`);
    expect(errorResponse(error, 'corr-1')).toEqual({
      status,
      body: { code, message: `because ${code}`, correlationId: 'corr-1' },
    });
  });
});

describe('infrastructure errors', () => {
  test.each([
    ['connection failure', driverError('08006')],
    ['too many connections', driverError('53300')],
    ['server shutting down', driverError('57P01')],
    ['statement timeout', driverError('57014')],
    ['lock not available', driverError('55P03')],
    ['deadlock', new DeadlockException(Object.assign(new Error('deadlock detected'), { code: '40P01' }))],
    ['serialization failure', driverError('40001')],
    ['refused socket', Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' })],
    ['refused socket as a cause', new Error('query failed', { cause: Object.assign(new Error('x'), { code: 'ECONNRESET' }) })],
  ])('%s is transient: 503 without internal details', (_case, error) => {
    expect(isTransientInfrastructureError(error)).toBe(true);
    const { status, body } = errorResponse(error, 'corr-2');
    expect(status).toBe(503);
    expect(body).toEqual({
      code: 'SERVICE_UNAVAILABLE',
      message: 'A dependency is temporarily unavailable; retry later',
      correlationId: 'corr-2',
    });
  });

  test.each([
    ['unique violation', driverError('23505')],
    ['permission denied', driverError('42501')],
    ['check violation', driverError('23514')],
    ['a bug', new TypeError('cannot read properties of undefined')],
    ['a thrown string', 'boom'],
  ])('%s is not transient: 500 with a generic message', (_case, error) => {
    expect(isTransientInfrastructureError(error)).toBe(false);
    expect(errorResponse(error, 'corr-3')).toEqual({
      status: 500,
      body: { code: 'INTERNAL_ERROR', message: 'Internal server error', correlationId: 'corr-3' },
    });
  });
});

describe('correlation id', () => {
  function exchange(header?: string | string[]) {
    const request = { headers: header === undefined ? {} : { [CORRELATION_ID_HEADER]: header } } as IncomingMessage;
    const headers: Record<string, string> = {};
    const response = {
      headersSent: false,
      setHeader: (name: string, value: string) => {
        headers[name] = value;
      },
    } as unknown as ServerResponse;
    return { request, response, headers };
  }

  test('keeps a well-formed caller id and echoes it', () => {
    const { request, response, headers } = exchange('provider-a:req.123_X');
    expect(assignCorrelationId(request, response)).toBe('provider-a:req.123_X');
    expect(headers[CORRELATION_ID_HEADER]).toBe('provider-a:req.123_X');
  });

  test.each([
    ['missing', undefined],
    ['empty', ''],
    ['too long', 'a'.repeat(129)],
    ['with spaces', 'a b'],
    ['with a line break', 'a\nb'],
    ['with markup', '<script>'],
    ['repeated', ['a', 'b']],
  ])('replaces a %s id with a new UUID', (_case, header) => {
    const { request, response, headers } = exchange(header);
    const id = assignCorrelationId(request, response);
    expect(isUuid(id)).toBe(true);
    expect(headers[CORRELATION_ID_HEADER]).toBe(id);
  });

  test('is assigned once per request', () => {
    const { request, response } = exchange();
    expect(assignCorrelationId(request, response)).toBe(assignCorrelationId(request, response));
  });
});

describe('AuthGuard', () => {
  const guard = new AuthGuard(new Reflector());
  const context = (target: object) =>
    ({ getHandler: () => () => undefined, getClass: () => target }) as unknown as ExecutionContext;

  test('health checks are public', () => {
    expect(Reflect.getMetadata(IS_PUBLIC_KEY, HealthController)).toBe(true);
  });

  test('is a no-op: public and protected routes are both allowed', () => {
    @Public()
    class OpenController {}
    class ProtectedController {}

    expect(guard.canActivate(context(OpenController))).toBe(true);
    expect(guard.canActivate(context(ProtectedController))).toBe(true);
  });
});
