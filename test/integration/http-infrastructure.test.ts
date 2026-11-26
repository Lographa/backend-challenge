import 'reflect-metadata';
import { MikroORM } from '@mikro-orm/postgresql';
import { afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { Controller, type DynamicModule, Get, type INestApplication, Module, Param } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { DomainError } from '../../src/common/errors/domain.error';
import type { FailureCode } from '../../src/common/errors/failure-code';
import { CorrelationId } from '../../src/common/http/correlation';
import { isUuid } from '../../src/common/ids';
import { ensureTestDatabase, testDatabaseUrl } from '../support/database';

const CLOSED_PORT = 1;

@Controller('probe')
class ProbeController {
  constructor(private readonly orm: MikroORM) {}

  @Get('correlation')
  correlation(@CorrelationId() correlationId: string): { correlationId: string } {
    return { correlationId };
  }

  @Get('domain/:code')
  domain(@Param('code') code: string): never {
    throw new DomainError(code as FailureCode, `probe ${code}`);
  }

  @Get('bug')
  bug(): never {
    throw new Error('secret internal detail');
  }

  @Get('database')
  async database(): Promise<{ ok: true }> {
    await this.orm.em.getConnection().execute('select 1');
    return { ok: true };
  }
}

let app: INestApplication | undefined;

beforeAll(async () => {
  await ensureTestDatabase();
});

afterEach(async () => {
  await app?.close();
  app = undefined;
});

/** The real AppModule (filter, guard, correlation) plus a controller that raises each kind of error. */
async function startApp(overrides: Record<string, string> = {}): Promise<string> {
  const config = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app'), ...overrides });
  @Module({})
  class ProbeModule {}
  const root: DynamicModule = { module: ProbeModule, imports: [AppModule.forRoot(config)], controllers: [ProbeController] };
  app = await NestFactory.create(root, { logger: false });
  await app.listen(0);
  return app.getUrl();
}

describe('correlation id over HTTP', () => {
  test('a well-formed X-Correlation-Id is used by the request and echoed', async () => {
    const url = await startApp();
    const response = await fetch(`${url}/probe/correlation`, { headers: { 'X-Correlation-Id': 'provider-a:abc-123' } });

    expect(response.status).toBe(200);
    expect(response.headers.get('x-correlation-id')).toBe('provider-a:abc-123');
    expect(await response.json()).toEqual({ correlationId: 'provider-a:abc-123' });
  });

  test('a missing or malformed id is replaced by a generated one', async () => {
    const url = await startApp();
    const cases: Record<string, string>[] = [{}, { 'X-Correlation-Id': 'not allowed!' }];
    for (const headers of cases) {
      const response = await fetch(`${url}/probe/correlation`, { headers });
      const { correlationId } = (await response.json()) as { correlationId: string };
      expect(isUuid(correlationId)).toBe(true);
      expect(response.headers.get('x-correlation-id')).toBe(correlationId);
    }
  });

  test('health checks stay open and also carry the header', async () => {
    const url = await startApp();
    const response = await fetch(`${url}/health/live`);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: 'ok' });
    expect(isUuid(response.headers.get('x-correlation-id'))).toBe(true);
  });
});

describe('error responses', () => {
  test.each([
    ['INVALID_PAYLOAD', 400],
    ['WALLET_NOT_FOUND', 404],
    ['IDEMPOTENCY_PAYLOAD_CONFLICT', 409],
    ['INSUFFICIENT_FUNDS', 422],
    ['NEGATIVE_BALANCE_REVERSAL', 422],
    ['PERMANENT_INTEGRITY_ERROR', 500],
  ])('a DomainError %s answers %d with { code, message, correlationId }', async (code, status) => {
    const url = await startApp();
    const response = await fetch(`${url}/probe/domain/${code}`, { headers: { 'X-Correlation-Id': 'corr-1' } });

    expect(response.status).toBe(status);
    expect(response.headers.get('x-correlation-id')).toBe('corr-1');
    expect(await response.json()).toEqual({ code, message: `probe ${code}`, correlationId: 'corr-1' });
  });

  test('an unexpected error is a 500 that hides its message', async () => {
    const url = await startApp();
    const response = await fetch(`${url}/probe/bug`);
    const body = (await response.json()) as Record<string, unknown>;

    expect(response.status).toBe(500);
    expect(body).toEqual({
      code: 'INTERNAL_ERROR',
      message: 'Internal server error',
      correlationId: response.headers.get('x-correlation-id'),
    });
  });

  test('an unreachable database is a 503 the caller can retry', async () => {
    const url = await startApp({ DATABASE_URL: `postgresql://x:y@127.0.0.1:${CLOSED_PORT}/none` });
    const response = await fetch(`${url}/probe/database`);

    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({ code: 'SERVICE_UNAVAILABLE' });
  }, 15_000);

  test('HttpExceptions keep the default Nest body (unknown route)', async () => {
    const url = await startApp();
    const response = await fetch(`${url}/nothing-here`);

    expect(response.status).toBe(404);
    expect(await response.json()).toMatchObject({ statusCode: 404 });
    expect(isUuid(response.headers.get('x-correlation-id'))).toBe(true);
  });
});
