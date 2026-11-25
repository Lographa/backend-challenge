import 'reflect-metadata';
import { afterEach, beforeAll, describe, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { ensureTestDatabase, testDatabaseUrl } from '../support/database';

// A port nothing listens on, so the dependency is genuinely unreachable.
const CLOSED_PORT = 1;

let app: INestApplication | undefined;

beforeAll(async () => {
  await ensureTestDatabase();
});

async function startApp(overrides: Record<string, string> = {}): Promise<string> {
  const config = AppConfig.load({
    ...process.env,
    DATABASE_URL: testDatabaseUrl('app'),
    HEALTH_CHECK_TIMEOUT_MS: '1500',
    ...overrides,
  });
  app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
  await app.listen(0);
  return app.getUrl();
}

afterEach(async () => {
  await app?.close();
  app = undefined;
});

describe('health endpoints (real PostgreSQL and LocalStack)', () => {
  test('liveness answers without touching dependencies', async () => {
    const url = await startApp({ DATABASE_URL: `postgresql://x:y@127.0.0.1:${CLOSED_PORT}/none` });

    const response = await fetch(`${url}/health/live`);

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: 'ok' });
  });

  test('readiness is 200 when PostgreSQL and every SQS queue are reachable', async () => {
    const url = await startApp();

    const response = await fetch(`${url}/health/ready`);

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: 'ok', checks: { postgres: 'up', sqs: 'up' } });
  });

  test('readiness is 503 and names PostgreSQL when the database is unreachable', async () => {
    const url = await startApp({ DATABASE_URL: `postgresql://x:y@127.0.0.1:${CLOSED_PORT}/none` });

    const response = await fetch(`${url}/health/ready`);

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      status: 'unavailable',
      checks: { postgres: 'down', sqs: 'up' },
    });
  });

  test('readiness is 503 and names SQS when a required queue does not exist', async () => {
    const url = await startApp({ SQS_INTEGRATION_EVENTS_QUEUE: 'missing-queue.fifo' });

    const response = await fetch(`${url}/health/ready`);

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      status: 'unavailable',
      checks: { postgres: 'up', sqs: 'down' },
    });
  });
});
