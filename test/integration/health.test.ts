import 'reflect-metadata';
import { SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { afterEach, beforeAll, describe, expect, spyOn, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { ShutdownService } from '../../src/common/http/shutdown.service';
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
    WORKERS_ENABLED: 'false',
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

  test('during shutdown with ample budget, /health/live stays 200 and /health/ready reports 503 shuttingDown with truthful checks', async () => {
    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '5000', HEALTH_CHECK_TIMEOUT_MS: '1500' });
    const shutdown = app!.get(ShutdownService);
    shutdown.beginShutdown('SIGTERM');

    const liveResponse = await fetch(`${url}/health/live`);
    expect(liveResponse.status).toBe(200);
    expect(liveResponse.headers.get('connection')).toBe('close');
    expect(await liveResponse.json()).toEqual({ status: 'ok' });

    const readyResponse = await fetch(`${url}/health/ready`);
    expect(readyResponse.status).toBe(503);
    expect(readyResponse.headers.get('connection')).toBe('close');
    expect(await readyResponse.json()).toEqual({
      status: 'unavailable',
      shuttingDown: true,
      checks: { postgres: 'up', sqs: 'up' },
    });
  });

  test('during shutdown with < 100ms residual budget, slow probes cut off strictly within residual budget as unknown (not down)', async () => {
    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '1000', HEALTH_CHECK_TIMEOUT_MS: '1500' });
    const shutdown = app!.get(ShutdownService);
    const sqs = app!.get(SQSClient);

    // Cooperative budget is 900ms; advance clock by 860ms so strictly 40ms (< 100ms) remains.
    let now = 10_000;
    shutdown.setClockForTesting(() => now);
    shutdown.beginShutdown('SIGTERM');
    now += 860;
    expect(shutdown.remainingCooperativeMs()).toBe(40);

    const sqsTarget = sqs as unknown as {
      send: (command: unknown, options?: { abortSignal?: AbortSignal }) => Promise<unknown>;
    };
    const sqsSendSpy = spyOn(sqsTarget, 'send').mockImplementation(
      (_command: unknown, options?: { abortSignal?: AbortSignal }) =>
        new Promise((_resolve, reject) => {
          const signal = options?.abortSignal;
          if (signal?.aborted) {
            reject(signal.reason ?? new Error('aborted'));
            return;
          }
          const timer = setTimeout(() => reject(new Error('probe exceeded residual window')), 500);
          signal?.addEventListener(
            'abort',
            () => {
              clearTimeout(timer);
              reject(signal.reason ?? new Error('aborted'));
            },
            { once: true },
          );
        }),
    );

    const startedAt = performance.now();
    const response = await fetch(`${url}/health/ready`);
    const durationMs = performance.now() - startedAt;

    sqsSendSpy.mockRestore();

    expect(response.status).toBe(503);
    expect(durationMs).toBeLessThan(200);
    expect(await response.json()).toEqual({
      status: 'unavailable',
      shuttingDown: true,
      checks: { postgres: 'up', sqs: 'unknown' },
    });
  });

  test('during shutdown with 0ms residual cooperative budget, /health/ready executes zero probes and reports unknown', async () => {
    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '1000', HEALTH_CHECK_TIMEOUT_MS: '1500' });
    const shutdown = app!.get(ShutdownService);
    const sqs = app!.get(SQSClient);
    const orm = app!.get(MikroORM);

    let now = 20_000;
    shutdown.setClockForTesting(() => now);
    shutdown.beginShutdown('SIGTERM');
    // Exhaust the entire 900ms cooperative budget.
    now += 900;
    expect(shutdown.remainingCooperativeMs()).toBe(0);

    const sqsSpy = spyOn(sqs, 'send');
    const pgSpy = spyOn(orm.em.getConnection(), 'execute');

    const response = await fetch(`${url}/health/ready`);

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      status: 'unavailable',
      shuttingDown: true,
      checks: { postgres: 'unknown', sqs: 'unknown' },
    });
    expect(sqsSpy).not.toHaveBeenCalled();
    expect(pgSpy).not.toHaveBeenCalled();

    sqsSpy.mockRestore();
    pgSpy.mockRestore();
  });

  test('accepted in-flight /health/ready finishes before ORM and SQS clients are torn down', async () => {
    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '3000', HEALTH_CHECK_TIMEOUT_MS: '1500' });
    const shutdown = app!.get(ShutdownService);
    const sqs = app!.get(SQSClient);
    const orm = app!.get(MikroORM);

    const events: string[] = [];
    let markProbeEntered!: () => void;
    const probeEntered = new Promise<void>((resolve) => {
      markProbeEntered = resolve;
    });

    const sqsTarget = sqs as unknown as {
      send: (command: unknown, options?: unknown) => Promise<unknown>;
    };
    const originalSend = sqsTarget.send.bind(sqs);
    const sendSpy = spyOn(sqsTarget, 'send').mockImplementation(async (command: unknown, options?: unknown) => {
      markProbeEntered();
      await Bun.sleep(90);
      const result = await originalSend(command, options);
      events.push('sqs-probe-done');
      return result;
    });

    const originalOrmClose = orm.close.bind(orm);
    const ormCloseSpy = spyOn(orm, 'close').mockImplementation(async (force?: boolean) => {
      events.push('orm-close');
      await originalOrmClose(force);
    });

    const originalSqsDestroy = sqs.destroy.bind(sqs);
    const sqsDestroySpy = spyOn(sqs, 'destroy').mockImplementation(() => {
      events.push('sqs-destroy');
      originalSqsDestroy();
    });

    const readyPromise = fetch(`${url}/health/ready`);
    await probeEntered;

    // Begin application close while /health/ready is already accepted and executing probes.
    const closePromise = shutdown.closeApplication(app!, 'SIGTERM');
    const response = await readyPromise;
    await closePromise;
    app = undefined;

    // Shutdown started while probes were in flight: probes still completed cleanly ('up') before
    // ORM/SQS teardown, and readiness truthfully reports 503 shuttingDown instead of advertising 200 ok.
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      status: 'unavailable',
      shuttingDown: true,
      checks: { postgres: 'up', sqs: 'up' },
    });

    const lastProbeIdx = events.lastIndexOf('sqs-probe-done');
    const ormCloseIdx = events.indexOf('orm-close');
    const sqsDestroyIdx = events.indexOf('sqs-destroy');
    expect(lastProbeIdx).toBeGreaterThanOrEqual(0);
    expect(ormCloseIdx).toBeGreaterThan(lastProbeIdx);
    expect(sqsDestroyIdx).toBeGreaterThan(lastProbeIdx);

    sendSpy.mockRestore();
    ormCloseSpy.mockRestore();
    sqsDestroySpy.mockRestore();
  });

  test('F3: PostgreSQL probe exceeding residual shutdown budget answers 503 unknown promptly while tracking real query until settlement before orm.close()', async () => {
    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '1000', HEALTH_CHECK_TIMEOUT_MS: '1500' });
    const shutdown = app!.get(ShutdownService);
    const orm = app!.get(MikroORM);

    // Warm the lazy pool on wagering_test first so the timed probe measures query execution.
    await orm.em.getConnection().execute('select 1');

    let now = 50_000;
    shutdown.setClockForTesting(() => now);
    shutdown.beginShutdown('SIGTERM');
    // 900ms cooperative budget - 850ms elapsed = 50ms residual budget (< 1500ms full timeout).
    now += 850;
    expect(shutdown.remainingCooperativeMs()).toBe(50);

    const connection = orm.em.getConnection();
    const originalExecute = connection.execute.bind(connection);
    const lifecycleEvents: string[] = [];
    let markQueryStarted!: () => void;
    const queryStarted = new Promise<void>((resolve) => {
      markQueryStarted = resolve;
    });

    // Execute a REAL 220ms PostgreSQL query (`select pg_sleep(0.22)`) on wagering_test so the query genuinely
    // holds a pooled `pg` client well past the 50ms residual shutdown HTTP cutoff.
    const executeSpy = spyOn(connection, 'execute').mockImplementation((async (
      queryOrKnex: unknown,
      params?: unknown[],
      method?: 'all' | 'get' | 'run',
      ctx?: unknown,
      loggerContext?: unknown,
    ) => {
      if (queryOrKnex === 'select 1') {
        lifecycleEvents.push('pg-probe-started');
        markQueryStarted();
        try {
          return await originalExecute(
            'select pg_sleep(0.22)',
            params,
            method,
            ctx as never,
            loggerContext as never,
          );
        } finally {
          lifecycleEvents.push('pg-probe-settled');
        }
      }
      return originalExecute(queryOrKnex as string, params, method, ctx as never, loggerContext as never);
    }) as typeof connection.execute);

    const originalOrmClose = orm.close.bind(orm);
    let activePgOpsAtOrmClose = -1;
    const ormCloseSpy = spyOn(orm, 'close').mockImplementation(async (force?: boolean) => {
      await shutdown.waitForResourceIdle('postgres');
      activePgOpsAtOrmClose = shutdown.activeCriticalOperationCount('postgres');
      lifecycleEvents.push('orm-close');
      await originalOrmClose(force);
    });

    const startedAt = performance.now();
    const readyPromise = fetch(`${url}/health/ready`);
    await queryStarted;
    const response = await readyPromise;
    const httpElapsedMs = performance.now() - startedAt;

    // 1. HTTP response returned 503 unknown promptly (~50ms), yet the real PostgreSQL query on wagering_test is STILL active!
    expect(response.status).toBe(503);
    expect(httpElapsedMs).toBeLessThan(180);
    expect(await response.json()).toEqual({
      status: 'unavailable',
      shuttingDown: true,
      checks: { postgres: 'unknown', sqs: 'up' },
    });
    expect(shutdown.inFlightHttpCount()).toBe(0);
    expect(shutdown.activeCriticalOperationCount('postgres')).toBe(1);
    expect(lifecycleEvents).toEqual(['pg-probe-started']);

    // 2. Start full app teardown while `select pg_sleep(0.22)` is still executing on wagering_test;
    //    `orm.close()` MUST wait until `pg-probe-settled` before closing the pool!
    await shutdown.closeApplication(app!, 'SIGTERM');
    app = undefined;

    expect(activePgOpsAtOrmClose).toBe(0);
    expect(lifecycleEvents).toEqual(['pg-probe-started', 'pg-probe-settled', 'orm-close']);
    // And the settled snapshot truthfully records 'completed' (NOT falsely 'cancelled'), since `pg_sleep` ran to completion.
    const pgProbeSnap = shutdown
      .recentSettledOperationSnapshots()
      .find((s) => s.name === 'HealthController.pingPostgres');
    expect(pgProbeSnap?.state).toBe('completed');

    executeSpy.mockRestore();
    ormCloseSpy.mockRestore();
  });

  test('F3: real PostgreSQL errors and non-shutdown probe timeouts remain down (never unknown)', async () => {
    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '5000', HEALTH_CHECK_TIMEOUT_MS: '200' });
    const shutdown = app!.get(ShutdownService);
    const orm = app!.get(MikroORM);
    const connection = orm.em.getConnection();
    const originalExecute = connection.execute.bind(connection);

    // Case A: Real PostgreSQL SQL error during shutdown (with shortened budget) MUST still be classified as 'down', not 'unknown'.
    let now = 60_000;
    shutdown.setClockForTesting(() => now);
    shutdown.beginShutdown('SIGTERM');
    // cooperative budget = 5000 - 250 = 4750ms -> advance 4630ms so 120ms remains (< 200ms full timeout -> budgetShortened = true)
    now += 4_630;
    expect(shutdown.remainingCooperativeMs()).toBe(120);

    const sqlErrorSpy = spyOn(connection, 'execute').mockImplementation(((queryOrKnex: unknown) => {
      if (queryOrKnex === 'select 1') {
        return originalExecute('select * from nonexistent_health_probe_table_f3');
      }
      return originalExecute(queryOrKnex as string);
    }) as typeof connection.execute);

    const errorResponse = await fetch(`${url}/health/ready`);
    sqlErrorSpy.mockRestore();

    expect(errorResponse.status).toBe(503);
    expect(await errorResponse.json()).toEqual({
      status: 'unavailable',
      shuttingDown: true,
      checks: { postgres: 'down', sqs: 'up' },
    });

    await app!.close();
    app = undefined;

    // Case B: Slow real PostgreSQL query (`pg_sleep(0.25)`) when NOT shortened by shutdown MUST be 'down', never 'unknown'.
    const normalUrl = await startApp({ SHUTDOWN_TIMEOUT_MS: '5000', HEALTH_CHECK_TIMEOUT_MS: '100' });
    const normalOrm = app!.get(MikroORM);
    const normalConn = normalOrm.em.getConnection();
    const origNormalExec = normalConn.execute.bind(normalConn);
    await origNormalExec('select 1');

    const slowNormalSpy = spyOn(normalConn, 'execute').mockImplementation(((
      queryOrKnex: unknown,
      params?: unknown[],
      method?: 'all' | 'get' | 'run',
      ctx?: unknown,
      loggerContext?: unknown,
    ) => {
      if (queryOrKnex === 'select 1') {
        return origNormalExec('select pg_sleep(0.25)', params, method, ctx as never, loggerContext as never);
      }
      return origNormalExec(queryOrKnex as string, params, method, ctx as never, loggerContext as never);
    }) as typeof normalConn.execute);

    const normalTimeoutRes = await fetch(`${normalUrl}/health/ready`);
    slowNormalSpy.mockRestore();

    expect(normalTimeoutRes.status).toBe(503);
    expect(await normalTimeoutRes.json()).toEqual({
      status: 'unavailable',
      checks: { postgres: 'down', sqs: 'up' },
    });
  });

  test('F4: after beforeApplicationShutdown completes HTTP drain, late /health/ready before httpServer.close() starts zero probes while /health/live stays 200', async () => {
    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '5000', HEALTH_CHECK_TIMEOUT_MS: '1500' });
    const shutdown = app!.get(ShutdownService);
    const sqs = app!.get(SQSClient);
    const orm = app!.get(MikroORM);

    // Complete HTTP drain while HTTP server is still open (exact window between beforeApplicationShutdown and dispose/httpServer.close).
    shutdown.beginShutdown('SIGTERM');
    await shutdown.beforeApplicationShutdown();
    expect(shutdown.isHttpDrainCompleted()).toBe(true);
    expect(shutdown.remainingCooperativeMs()).toBeGreaterThan(1_000);

    const pgSpy = spyOn(orm.em.getConnection(), 'execute');
    const sqsSpy = spyOn(sqs, 'send');

    const liveRes = await fetch(`${url}/health/live`);
    expect(liveRes.status).toBe(200);
    expect(await liveRes.json()).toEqual({ status: 'ok' });

    const readyRes = await fetch(`${url}/health/ready`);
    expect(readyRes.status).toBe(503);
    expect(readyRes.headers.get('connection')).toBe('close');
    expect(await readyRes.json()).toEqual({
      status: 'unavailable',
      shuttingDown: true,
      checks: { postgres: 'unknown', sqs: 'unknown' },
    });

    expect(pgSpy).not.toHaveBeenCalled();
    expect(sqsSpy).not.toHaveBeenCalled();
    expect(shutdown.activeCriticalOperationCount()).toBe(0);

    pgSpy.mockRestore();
    sqsSpy.mockRestore();
  });
});
