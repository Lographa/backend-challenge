import 'reflect-metadata';
import { createServer as createHttpServer, type Server, type ServerResponse } from 'node:http';
import { type AddressInfo, connect as netConnect, createServer } from 'node:net';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { MikroOrmMiddleware } from '@mikro-orm/nestjs';
import { MikroORM } from '@mikro-orm/postgresql';
import { afterEach, beforeAll, describe, expect, spyOn, test } from 'bun:test';
import {
  Controller,
  type DynamicModule,
  Get,
  type INestApplication,
  Injectable,
  Module,
  type OnModuleDestroy,
  Param,
  Res,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { DomainError } from '../../src/common/errors/domain.error';
import type { FailureCode } from '../../src/common/errors/failure-code';
import { CorrelationId } from '../../src/common/http/correlation';
import { ShutdownService } from '../../src/common/http/shutdown.service';
import { isUuid, newId } from '../../src/common/ids';
import { OutboxPublisherService } from '../../src/messaging/outbox-publisher.service';
import { toWagerTransactionSendInput, WAGER_REQUESTED_MESSAGE_TYPE } from '../../src/messaging/wager-message';
import { WagerTransactionsConsumer } from '../../src/messaging/wager-transactions.consumer';
import { PendingReferenceWorker } from '../../src/wagering/pending-reference.worker';
import { WagerProcessorService } from '../../src/wagering/wager-processor.service';
import { WagerTransactionKind } from '../../src/wagering/wager-transaction';
import { WalletsService } from '../../src/wallets/wallets.service';
import { createTestOrm, createTestSql, testDatabaseUrl } from '../support/database';

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

  @Get('gated-db')
  async gatedDb(): Promise<{ ok: true }> {
    ProbeController.onGatedEntered?.();
    await ProbeController.gatePromise;
    await this.orm.em.getConnection().execute('select 1');
    return { ok: true };
  }

  @Get('hang')
  hang(@Res() res: ServerResponse): void {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.flushHeaders();
    res.write('partial');
    ProbeController.onHangEntered?.();
    // Intentionally never calls res.end() to exercise client abort and drain timeout destruction.
  }

  static gatePromise: Promise<void> = Promise.resolve();
  static onGatedEntered: (() => void) | undefined;
  static onHangEntered: (() => void) | undefined;
}

let app: INestApplication | undefined;

beforeAll(async () => {
  const migratorOrm = await createTestOrm('migrator');
  try {
    await migratorOrm.migrator.up();
  } finally {
    await migratorOrm.close(true);
  }
});

afterEach(async () => {
  await app?.close();
  app = undefined;
});

/** The real AppModule (filter, guard, correlation) plus a controller that raises each kind of error. */
async function startApp(overrides: Record<string, string> = {}): Promise<string> {
  const config = AppConfig.load({
    ...process.env,
    DATABASE_URL: testDatabaseUrl('app'),
    WORKERS_ENABLED: 'false',
    ...overrides,
  });
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

function parseHostPort(urlString: string): { host: string; port: number } {
  const parsed = new URL(urlString);
  const host = parsed.hostname.replace(/^\[|\]$/g, '');
  return { host, port: Number(parsed.port) };
}

async function allocateFreePort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as AddressInfo;
      server.close((err) => (err ? reject(err) : resolve(port)));
    });
  });
}

function childTestEnv(overrides: Record<string, string> = {}): Record<string, string> {
  const base: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      base[key] = value;
    }
  }
  return {
    ...base,
    DATABASE_URL: testDatabaseUrl('app'),
    MIGRATION_DATABASE_URL: testDatabaseUrl('migrator'),
    DATABASE_ADMIN_URL: testDatabaseUrl('admin'),
    LOG_LEVEL: 'log',
    ...overrides,
  };
}

describe('graceful HTTP shutdown & drain (Increment 7.1)', () => {
  afterEach(() => {
    ProbeController.gatePromise = Promise.resolve();
    ProbeController.onGatedEntered = undefined;
    ProbeController.onHangEntered = undefined;
  });

  test('drains in-flight DB request before ORM close, rejects new business requests with 503, preserves health, and shares closeApplication Promise', async () => {
    let releaseGate!: () => void;
    ProbeController.gatePromise = new Promise<void>((resolve) => {
      releaseGate = resolve;
    });
    let markEntered!: () => void;
    const entered = new Promise<void>((resolve) => {
      markEntered = resolve;
    });
    ProbeController.onGatedEntered = markEntered;

    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '3000' });
    const shutdown = app!.get(ShutdownService);
    const orm = app!.get(MikroORM);

    let ormClosed = false;
    const originalOrmClose = orm.close.bind(orm);
    const ormCloseSpy = spyOn(orm, 'close').mockImplementation(async (force?: boolean) => {
      ormClosed = true;
      await originalOrmClose(force);
    });

    const inFlightPromise = fetch(`${url}/probe/gated-db`);
    await entered;
    expect(shutdown.inFlightHttpCount()).toBe(1);

    // Trigger shutdown via two concurrent calls; both must share the exact same Promise.
    const closeA = shutdown.closeApplication(app!, 'SIGTERM');
    const closeB = shutdown.closeApplication(app!, 'SIGINT');
    expect(closeA).toBe(closeB);
    expect(shutdown.isShuttingDown()).toBe(true);
    expect(shutdown.reason()).toBe('SIGTERM');

    // New business requests during drain must be rejected immediately with 503 + Connection: close + X-Correlation-Id.
    const rejected = await fetch(`${url}/probe/correlation`, {
      headers: { 'X-Correlation-Id': 'corr-during-drain' },
    });
    expect(rejected.status).toBe(503);
    expect(rejected.headers.get('connection')).toBe('close');
    expect(rejected.headers.get('x-correlation-id')).toBe('corr-during-drain');
    expect(await rejected.json()).toEqual({
      code: 'SERVICE_UNAVAILABLE',
      message: 'Service is shutting down; retry later',
      correlationId: 'corr-during-drain',
    });

    // Health endpoints must remain reachable during drain and ORM must STILL be open.
    const liveDuringDrain = await fetch(`${url}/health/live`);
    expect(liveDuringDrain.status).toBe(200);
    expect(liveDuringDrain.headers.get('connection')).toBe('close');

    const readyDuringDrain = await fetch(`${url}/health/ready`);
    expect(readyDuringDrain.status).toBe(503);
    expect(readyDuringDrain.headers.get('connection')).toBe('close');
    expect(await readyDuringDrain.json()).toEqual({
      status: 'unavailable',
      shuttingDown: true,
      checks: { postgres: 'up', sqs: 'up' },
    });
    expect(ormClosed).toBe(false);

    // Release the in-flight request; it must execute its query on wagering_test BEFORE orm.close() runs.
    releaseGate();
    const inFlightResponse = await inFlightPromise;
    expect(inFlightResponse.status).toBe(200);
    expect(await inFlightResponse.json()).toEqual({ ok: true });

    await closeA;
    app = undefined;

    expect(ormClosed).toBe(true);
    expect(shutdown.didTimeout()).toBe(false);
    expect(shutdown.hasFailed()).toBe(false);
    ormCloseSpy.mockRestore();
  });

  test('explicitly destroys hung responses and sockets on drain timeout without summing worker and HTTP budgets', async () => {
    @Injectable()
    class SlowWorker implements OnModuleDestroy {
      constructor(private readonly shutdown: ShutdownService) {}

      async onModuleDestroy(): Promise<void> {
        await this.shutdown.runWithinCooperativeBudget('SlowWorker', async () => {
          await Bun.sleep(450);
        });
      }
    }

    const config = AppConfig.load({
      ...process.env,
      DATABASE_URL: testDatabaseUrl('app'),
      SHUTDOWN_TIMEOUT_MS: '1000', // cooperative budget = 900ms
      WORKERS_ENABLED: 'false',
    });

    let markHangEntered!: () => void;
    const hangEntered = new Promise<void>((resolve) => {
      markHangEntered = resolve;
    });
    ProbeController.onHangEntered = markHangEntered;

    @Module({})
    class SlowShutdownModule {}
    const root: DynamicModule = {
      module: SlowShutdownModule,
      imports: [AppModule.forRoot(config)],
      controllers: [ProbeController],
      providers: [SlowWorker],
    };
    app = await NestFactory.create(root, { logger: false });
    await app.listen(0, '127.0.0.1');
    const { host, port } = parseHostPort(await app.getUrl());
    const shutdown = app.get(ShutdownService);

    const clientSocket = netConnect({ host, port });
    clientSocket.on('error', () => undefined);
    clientSocket.resume();
    const socketClosed = new Promise<void>((resolve) => {
      clientSocket.once('close', () => resolve());
    });
    await new Promise<void>((resolve, reject) => {
      clientSocket.once('connect', () => resolve());
      clientSocket.once('error', reject);
    });
    clientSocket.write(`GET /probe/hang HTTP/1.1\r\nHost: ${host}:${port}\r\nConnection: keep-alive\r\n\r\n`);

    await hangEntered;
    expect(shutdown.inFlightHttpCount()).toBe(1);

    const startedAt = performance.now();
    await shutdown.closeApplication(app, 'SIGTERM');
    const elapsedMs = performance.now() - startedAt;
    app = undefined;

    await socketClosed;
    clientSocket.destroy();

    // SlowWorker (450ms) + residual HTTP drain (~450ms) share the single 900ms cooperative budget (never 450 + 900 = 1350ms+).
    expect(elapsedMs).toBeGreaterThanOrEqual(850);
    expect(elapsedMs).toBeLessThan(1200);
    expect(shutdown.didTimeout()).toBe(true);
    expect(shutdown.hasFailed()).toBe(true);
    expect(shutdown.inFlightHttpCount()).toBe(0);
  });

  test('releases tracking immediately when a client aborts mid-response so shutdown does not stall', async () => {
    let markHangEntered!: () => void;
    const hangEntered = new Promise<void>((resolve) => {
      markHangEntered = resolve;
    });
    ProbeController.onHangEntered = markHangEntered;

    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '2000' });
    const { host, port } = parseHostPort(url);
    const shutdown = app!.get(ShutdownService);

    const clientSocket = netConnect({ host, port });
    await new Promise<void>((resolve, reject) => {
      clientSocket.once('connect', () => resolve());
      clientSocket.once('error', reject);
    });
    clientSocket.write(`GET /probe/hang HTTP/1.1\r\nHost: ${host}:${port}\r\n\r\n`);

    await hangEntered;
    expect(shutdown.inFlightHttpCount()).toBe(1);

    // Abruptly abort from client side.
    clientSocket.destroy();

    const waitDeadline = performance.now() + 500;
    while (shutdown.inFlightHttpCount() > 0 && performance.now() < waitDeadline) {
      await Bun.sleep(10);
    }
    expect(shutdown.inFlightHttpCount()).toBe(0);

    const closeStart = performance.now();
    await shutdown.closeApplication(app!, 'SIGTERM');
    const closeDurationMs = performance.now() - closeStart;
    app = undefined;

    expect(closeDurationMs).toBeLessThan(250);
    expect(shutdown.didTimeout()).toBe(false);
    expect(shutdown.hasFailed()).toBe(false);
  });

  test('closes idle HTTP/1.1 keep-alive connections promptly during shutdown', async () => {
    const url = await startApp({ SHUTDOWN_TIMEOUT_MS: '2000' });
    const { host, port } = parseHostPort(url);
    const shutdown = app!.get(ShutdownService);

    const keepAliveSocket = netConnect({ host, port });
    const socketClosed = new Promise<void>((resolve) => {
      keepAliveSocket.once('close', () => resolve());
    });
    await new Promise<void>((resolve, reject) => {
      keepAliveSocket.once('connect', () => resolve());
      keepAliveSocket.once('error', reject);
    });

    const firstReply = new Promise<string>((resolve) => {
      let buffer = '';
      const onData = (chunk: Buffer) => {
        buffer += chunk.toString('utf8');
        if (buffer.includes('\r\n\r\n') && buffer.includes('"correlationId"')) {
          keepAliveSocket.off('data', onData);
          resolve(buffer);
        }
      };
      keepAliveSocket.on('data', onData);
    });

    keepAliveSocket.write(
      `GET /probe/correlation HTTP/1.1\r\nHost: ${host}:${port}\r\nConnection: keep-alive\r\nX-Correlation-Id: ka-req-1\r\n\r\n`,
    );
    const rawResponse = await firstReply;
    expect(rawResponse).toContain('HTTP/1.1 200 OK');
    expect(shutdown.inFlightHttpCount()).toBe(0);
    expect(keepAliveSocket.destroyed).toBe(false);

    // Socket is now idle in keep-alive state; shutdown must close it immediately without waiting for keepAliveTimeout.
    const startedAt = performance.now();
    await shutdown.closeApplication(app!, 'SIGTERM');
    const closeDurationMs = performance.now() - startedAt;
    app = undefined;

    await socketClosed;
    keepAliveSocket.destroy();

    expect(closeDurationMs).toBeLessThan(300);
    expect(shutdown.didTimeout()).toBe(false);
  });

  test('records teardown errors from SQSClient.destroy() and MikroORM.close() even when NestJS swallows hook rejections', async () => {
    // 1. SQSClient.destroy() failure
    await startApp({ SHUTDOWN_TIMEOUT_MS: '2000' });
    const sqsShutdown = app!.get(ShutdownService);
    const sqs = app!.get(SQSClient);
    const originalDestroy = sqs.destroy.bind(sqs);
    const sqsDestroySpy = spyOn(sqs, 'destroy').mockImplementation(() => {
      originalDestroy();
      throw new Error('simulated sqs destroy failure');
    });

    await sqsShutdown.closeApplication(app!, 'SIGTERM');
    app = undefined;
    sqsDestroySpy.mockRestore();

    expect(sqsShutdown.hasTeardownError()).toBe(true);
    expect(sqsShutdown.hasFailed()).toBe(true);
    expect(sqsShutdown.failureReasons().some((r) => r.includes('simulated sqs destroy failure'))).toBe(true);

    // 2. MikroORM.close() failure
    await startApp({ SHUTDOWN_TIMEOUT_MS: '2000' });
    const ormShutdown = app!.get(ShutdownService);
    const orm = app!.get(MikroORM);
    const originalClose = orm.close.bind(orm);
    orm.close = async (force?: boolean): Promise<void> => {
      await originalClose(force);
      // DatabaseModule wrapped orm.close on init; invoke markTeardownError path via throwing inside originalClose:
      throw new Error('unreachable');
    };
    // Restore and fail inside the underlying driver close so DatabaseModule's wrapper catches it:
    orm.close = originalClose;
    const driverCloseSpy = spyOn(orm.driver, 'close').mockImplementation(async () => {
      throw new Error('simulated orm driver close failure');
    });

    await ormShutdown.closeApplication(app!, 'SIGTERM');
    app = undefined;
    driverCloseSpy.mockRestore();
    await orm.driver.close(true).catch(() => undefined);

    expect(ormShutdown.hasTeardownError()).toBe(true);
    expect(ormShutdown.hasFailed()).toBe(true);
    expect(ormShutdown.failureReasons().some((r) => r.includes('simulated orm driver close failure'))).toBe(true);
  });

  test('real subprocess running src/main.ts absorbs concurrent signals and exits naturally with code 0 on wagering_test', async () => {
    const port = await allocateFreePort();
    const baseConfig = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app') });
    const sqsClient = new SQSClient({
      region: baseConfig.awsRegion,
      ...(baseConfig.awsEndpointUrl !== undefined ? { endpoint: baseConfig.awsEndpointUrl } : {}),
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      },
    });
    const isolatedQueueName = `wager-tx-shutdown-${newId().slice(0, 8)}.fifo`;
    const createdQueue = await sqsClient.send(
      new CreateQueueCommand({
        QueueName: isolatedQueueName,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }),
    );

    // Bootstrap wrapper attaches an unref'd stdin trigger on Windows (where proc.kill uses TerminateProcess)
    // and then imports the unmodified src/main.ts entrypoint.
    const bootstrapScript = `
      process.stdin.on('data', (chunk) => {
        const text = chunk.toString('utf8');
        if (text.includes('SHUTDOWN_CONCURRENT')) {
          setTimeout(() => {
            console.log('NATURAL_EVENT_LOOP_DRAIN_VERIFIED');
          }, 120);
          process.emit('SIGTERM', 'SIGTERM');
          process.emit('SIGINT', 'SIGINT');
        }
      });
      process.stdin.unref();
      await import('./src/main.ts');
    `;

    const child = Bun.spawn(['bun', '-e', bootstrapScript], {
      cwd: process.cwd(),
      env: childTestEnv({
        PORT: String(port),
        WORKERS_ENABLED: 'true',
        SQS_WAIT_TIME_SECONDS: '0',
        SQS_WAGER_TRANSACTIONS_QUEUE: isolatedQueueName,
        SHUTDOWN_TIMEOUT_MS: '4000',
      }),
      stdin: 'pipe',
      stdout: 'pipe',
      stderr: 'pipe',
    });

    try {
      const baseUrl = `http://127.0.0.1:${port}`;
      const deadline = performance.now() + 10_000;
      let ready = false;
      while (performance.now() < deadline) {
        try {
          const res = await fetch(`${baseUrl}/health/ready`);
          if (res.status === 200) {
            ready = true;
            break;
          }
        } catch {
          // Server still starting.
        }
        await Bun.sleep(50);
      }
      expect(ready).toBe(true);

      child.stdin.write('SHUTDOWN_CONCURRENT\n');
      child.stdin.end();

      const exitCode = await child.exited;
      const stdout = await new Response(child.stdout).text();
      const stderr = await new Response(child.stderr).text();
      const combinedLogs = `${stdout}\n${stderr}`;

      expect(exitCode).toBe(0);
      expect(combinedLogs).toContain('Shutdown signal received');
      expect(combinedLogs).toContain('Duplicate shutdown signal received while shutdown is already in progress');
      expect(combinedLogs).toContain('Graceful shutdown completed');
      expect(combinedLogs).toContain('NATURAL_EVENT_LOOP_DRAIN_VERIFIED');
      expect(combinedLogs.indexOf('Graceful shutdown completed')).toBeLessThan(
        combinedLogs.indexOf('NATURAL_EVENT_LOOP_DRAIN_VERIFIED'),
      );
    } finally {
      child.kill();
      if (createdQueue.QueueUrl !== undefined) {
        await sqsClient.send(new DeleteQueueCommand({ QueueUrl: createdQueue.QueueUrl })).catch(() => undefined);
      }
      sqsClient.destroy();
    }
  }, 20_000);

  test('real subprocess active watchdog (without .unref()) exits with code 1 instead of silent exit 0 when teardown hangs on unref/deadlocked Promise', async () => {
    const port = await allocateFreePort();
    const hungTeardownScript = `
      import 'reflect-metadata';
      import { Injectable, Logger, Module } from '@nestjs/common';
      import { NestFactory } from '@nestjs/core';
      import { AppModule } from './src/app.module.ts';
      import { AppConfig } from './src/common/config/env.ts';
      import { installProcessShutdownHandlers, ShutdownService } from './src/common/http/shutdown.service.ts';
      import { createLogger } from './src/common/observability/logger.ts';

      @Injectable()
      class DeadlockedTeardownHook {
        async onApplicationShutdown() {
          // After HTTP server closes (zero ref'd network handles left), hang on a Promise with no ref'd handle.
          // An unref'd watchdog in Bun 1.4.5 would let the process exit silently with code 0 here!
          await new Promise(() => {});
        }
      }

      const config = AppConfig.load(process.env);
      @Module({
        imports: [AppModule.forRoot(config)],
        providers: [DeadlockedTeardownHook],
      })
      class HungRootModule {}

      const app = await NestFactory.create(HungRootModule, { logger: createLogger('log') });
      const logger = new Logger('Bootstrap');
      installProcessShutdownHandlers({ app, shutdown: app.get(ShutdownService), logger });
      await app.listen(config.port, '127.0.0.1');
      console.log('CHILD_LISTENING');

      setTimeout(() => {
        process.emit('SIGTERM', 'SIGTERM');
      }, 30);
    `;

    const child = Bun.spawn(['bun', '-e', hungTeardownScript], {
      cwd: process.cwd(),
      env: childTestEnv({
        PORT: String(port),
        WORKERS_ENABLED: 'false',
        SHUTDOWN_TIMEOUT_MS: '1000',
      }),
      stdout: 'pipe',
      stderr: 'pipe',
    });

    try {
      const startedAt = performance.now();
      const exitCode = await child.exited;
      const totalElapsedMs = performance.now() - startedAt;
      const stdout = await new Response(child.stdout).text();
      const stderr = await new Response(child.stderr).text();
      const combinedLogs = `${stdout}\n${stderr}`;

      expect(stdout).toContain('CHILD_LISTENING');
      expect(exitCode).toBe(1);
      // Proves the active watchdog actually waited ~1000ms and fired, rather than premature silent exit 0.
      expect(totalElapsedMs).toBeGreaterThanOrEqual(950);
      expect(combinedLogs).toContain('Graceful shutdown timed out; forcing exit');
    } finally {
      child.kill();
    }
  }, 15_000);

  test('real subprocess exits with code 1 when a teardown hook fails during shutdown', async () => {
    const port = await allocateFreePort();
    const failingTeardownScript = `
      import 'reflect-metadata';
      import { SQSClient } from '@aws-sdk/client-sqs';
      import { Logger } from '@nestjs/common';
      import { NestFactory } from '@nestjs/core';
      import { AppModule } from './src/app.module.ts';
      import { AppConfig } from './src/common/config/env.ts';
      import { installProcessShutdownHandlers, ShutdownService } from './src/common/http/shutdown.service.ts';
      import { createLogger } from './src/common/observability/logger.ts';

      const config = AppConfig.load(process.env);
      const app = await NestFactory.create(AppModule.forRoot(config), { logger: createLogger('log') });
      const sqs = app.get(SQSClient);
      const origDestroy = sqs.destroy.bind(sqs);
      sqs.destroy = () => {
        origDestroy();
        throw new Error('fatal sqs teardown failure');
      };

      const logger = new Logger('Bootstrap');
      installProcessShutdownHandlers({ app, shutdown: app.get(ShutdownService), logger });
      await app.listen(config.port, '127.0.0.1');

      setTimeout(() => {
        process.emit('SIGTERM', 'SIGTERM');
      }, 20);
    `;

    const child = Bun.spawn(['bun', '-e', failingTeardownScript], {
      cwd: process.cwd(),
      env: childTestEnv({
        PORT: String(port),
        WORKERS_ENABLED: 'false',
        SHUTDOWN_TIMEOUT_MS: '2000',
      }),
      stdout: 'pipe',
      stderr: 'pipe',
    });

    try {
      const exitCode = await child.exited;
      const stdout = await new Response(child.stdout).text();
      const stderr = await new Response(child.stderr).text();
      const combinedLogs = `${stdout}\n${stderr}`;

      expect(exitCode).toBe(1);
      expect(combinedLogs).toContain('Shutdown completed with timeout or teardown failure');
    } finally {
      child.kill();
    }
  }, 15_000);
});

async function isTcpPortOpen(port: number, host = '127.0.0.1', timeoutMs = 150): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const socket = netConnect({ host, port });
    let done = false;
    const finish = (open: boolean): void => {
      if (done) {
        return;
      }
      done = true;
      socket.destroy();
      resolve(open);
    };
    const timer = setTimeout(() => finish(false), timeoutMs);
    socket.once('connect', () => {
      clearTimeout(timer);
      finish(true);
    });
    socket.once('error', () => {
      clearTimeout(timer);
      finish(false);
    });
  });
}

function createLocalstackSqsClient(baseConfig: AppConfig): SQSClient {
  return new SQSClient({
    region: baseConfig.awsRegion,
    ...(baseConfig.awsEndpointUrl !== undefined ? { endpoint: baseConfig.awsEndpointUrl } : {}),
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
}

describe('F1 — critical PostgreSQL, SQS & disconnected HTTP containment (real wagering_test & LocalStack)', () => {
  test('F1.1: worker PostgreSQL operation blocked on real row lock past cooperative budget holds orm.close() & sqs.destroy(), preserves ACK-after-commit and financial balance', async () => {
    const baseConfig = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app') });
    const extSqs = createLocalstackSqsClient(baseConfig);
    const isolatedQueueName = `wager-tx-f1-pg-${newId().slice(0, 8)}.fifo`;
    const createdQueue = await extSqs.send(
      new CreateQueueCommand({
        QueueName: isolatedQueueName,
        Attributes: {
          FifoQueue: 'true',
          ContentBasedDeduplication: 'false',
          VisibilityTimeout: '1',
        },
      }),
    );
    const queueUrl = createdQueue.QueueUrl!;
    const lockSql = createTestSql('app');

    try {
      const [{ db }] = await lockSql`select current_database() as db`;
      expect(db).toBe('wagering_test');

      const config = AppConfig.load({
        ...process.env,
        DATABASE_URL: testDatabaseUrl('app'),
        WORKERS_ENABLED: 'false',
        SQS_WAIT_TIME_SECONDS: '0',
        SQS_WAGER_TRANSACTIONS_QUEUE: isolatedQueueName,
        SHUTDOWN_TIMEOUT_MS: '1000', // cooperative budget = 900ms
      });
      app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
      await app.listen(0, '127.0.0.1');
      const url = await app.getUrl();

      // 1. Open a real wallet ($100.00 USD) on wagering_test.
      const playerId = newId();
      const openRes = await fetch(`${url}/wallets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'USD' } }),
      });
      expect(openRes.status).toBe(201);
      const wallet = (await openRes.json()) as { id: string };

      // 2. Enqueue a real $25.00 USD BET command onto the isolated LocalStack FIFO queue.
      const messageId = `msg-f1-${newId()}`;
      const extTxId = `ext-f1-${newId()}`;
      const idempotencyKey = `idem-f1-${newId()}`;
      await extSqs.send(
        new SendMessageCommand(
          toWagerTransactionSendInput(queueUrl, {
            messageId,
            type: WAGER_REQUESTED_MESSAGE_TYPE,
            occurredAt: new Date().toISOString(),
            correlationId: 'corr-f1-worker-pg',
            data: {
              providerId: 'prov-f1',
              externalTransactionId: extTxId,
              idempotencyKey,
              playerId,
              walletId: wallet.id,
              roundId: 'round-f1-1',
              gameId: 'slots-f1',
              kind: WagerTransactionKind.Bet,
              money: { amount: '25.00', currency: 'USD' },
            },
          }),
        ),
      );

      const shutdown = app.get(ShutdownService);
      const consumer = app.get(WagerTransactionsConsumer);
      const orm = app.get(MikroORM);
      const appSqs = app.get(SQSClient);

      const events: string[] = [];
      let ormClosed = false;
      let sqsDestroyed = false;
      let deleteMessageCalls = 0;

      const origSend = appSqs.send.bind(appSqs) as (cmd: unknown, opts?: unknown) => Promise<unknown>;
      const sendSpy = spyOn(appSqs as unknown as { send: typeof origSend }, 'send').mockImplementation(
        async (cmd: unknown, opts?: unknown) => {
          if (cmd instanceof DeleteMessageCommand) {
            deleteMessageCalls += 1;
            events.push('sqs-delete-message');
          }
          return origSend(cmd, opts);
        },
      );

      const origSqsDestroy = appSqs.destroy.bind(appSqs);
      const sqsDestroySpy = spyOn(appSqs, 'destroy').mockImplementation(() => {
        sqsDestroyed = true;
        events.push('sqs-destroy');
        origSqsDestroy();
      });

      const origOrmClose = orm.close.bind(orm);
      const ormCloseSpy = spyOn(orm, 'close').mockImplementation(async (force?: boolean) => {
        await shutdown.waitForResourceIdle('postgres');
        ormClosed = true;
        events.push('orm-close');
        await origOrmClose(force);
      });

      // 3. Hold a REAL pessimistic row lock (`SELECT ... FOR UPDATE`) on the wallet in wagering_test.
      let releaseWalletRowLock!: () => void;
      const walletLockGate = new Promise<void>((resolve) => {
        releaseWalletRowLock = resolve;
      });
      let markLockHeld!: () => void;
      const lockHeld = new Promise<void>((resolve) => {
        markLockHeld = resolve;
      });

      const lockHolderTask = lockSql.begin(async (tx) => {
        await tx`select id from wallets where id = ${wallet.id} for update`;
        markLockHeld();
        await walletLockGate;
      });
      await lockHeld;

      // 4. Start real consumer poll; wait until PostgreSQL pg_stat_activity confirms real Lock contention on wagering_test.
      const pollPromise = consumer.pollOnce({ waitTimeSeconds: 0 }).then((stats) => {
        events.push('consumer-poll-settled');
        return stats;
      });

      const lockWaitDeadline = performance.now() + 4_000;
      let blockedInPostgres = false;
      while (performance.now() < lockWaitDeadline) {
        const waitingRows = await lockSql`
          select count(*)::int as cnt
          from pg_stat_activity
          where datname = current_database()
            and wait_event_type = 'Lock'
            and state = 'active'
        `;
        if ((waitingRows[0]?.cnt ?? 0) > 0) {
          blockedInPostgres = true;
          break;
        }
        await Bun.sleep(15);
      }
      expect(blockedInPostgres).toBe(true);
      expect(deleteMessageCalls).toBe(0);

      // 5. Begin shutdown and wait past the 900ms cooperative budget while the SQL lock is STILL held!
      const closePromise = shutdown.closeApplication(app, 'SIGTERM');
      await Bun.sleep(960);

      expect(shutdown.didTimeout()).toBe(true);
      expect(shutdown.remainingCooperativeMs()).toBe(0);
      expect(ormClosed).toBe(false);
      expect(sqsDestroyed).toBe(false);
      expect(deleteMessageCalls).toBe(0);
      const activeSnaps = shutdown.activeOperationSnapshots();
      expect(activeSnaps.some((s) => s.name === 'WagerTransactionsConsumer.pollOnce' && s.state === 'exceeded_budget_active')).toBe(
        true,
      );

      // 6. Release the real PostgreSQL row lock: consumer commits BET ($75.00), skips 0ms post-commit DeleteMessage,
      //    and ONLY THEN do sqs.destroy() and orm.close() execute!
      releaseWalletRowLock();
      await lockHolderTask;
      const pollStats = await pollPromise;
      await closePromise;
      app = undefined;

      expect(pollStats.processed).toBe(1);
      expect(deleteMessageCalls).toBe(0); // Cooperative budget was 0ms after commit -> zero 0ms SQS DeleteMessage call!
      expect(events).toEqual(['consumer-poll-settled', 'sqs-destroy', 'orm-close']);

      sendSpy.mockRestore();
      sqsDestroySpy.mockRestore();
      ormCloseSpy.mockRestore();

      // 7. Wait for 1s SQS visibility timeout to expire and poll with a fresh app instance:
      //    redelivery hits `inbox_messages`, ACKs cleanly AFTER commit, and NEVER double-debits the wallet!
      await Bun.sleep(1_150);
      const recoveryApp = await NestFactory.create(
        AppModule.forRoot(
          AppConfig.load({
            ...process.env,
            DATABASE_URL: testDatabaseUrl('app'),
            WORKERS_ENABLED: 'false',
            SQS_WAIT_TIME_SECONDS: '0',
            SQS_WAGER_TRANSACTIONS_QUEUE: isolatedQueueName,
          }),
        ),
        { logger: false },
      );
      try {
        await recoveryApp.init();
        const recoverySqs = recoveryApp.get(SQSClient);
        let recoveryDeleteCalls = 0;
        const origRecoverySend = recoverySqs.send.bind(recoverySqs) as (cmd: unknown, opts?: unknown) => Promise<unknown>;
        const recoverySendSpy = spyOn(
          recoverySqs as unknown as { send: typeof origRecoverySend },
          'send',
        ).mockImplementation(async (cmd: unknown, opts?: unknown) => {
          if (cmd instanceof DeleteMessageCommand) {
            recoveryDeleteCalls += 1;
          }
          return origRecoverySend(cmd, opts);
        });

        const recoveryConsumer = recoveryApp.get(WagerTransactionsConsumer);
        const redeliveryStats = await recoveryConsumer.pollOnce({ waitTimeSeconds: 2 });
        recoverySendSpy.mockRestore();

        expect(redeliveryStats.duplicates).toBe(1);
        expect(recoveryDeleteCalls).toBe(1);

        const walletsService = recoveryApp.get(WalletsService);
        const report = await walletsService.reconcile(wallet.id, 'corr-f1-reconcile');
        expect(report.consistent).toBe(true);
        expect(report.storedBalance.amount).toBe('75.00');
        expect(report.calculatedBalance.amount).toBe('75.00');
        expect(report.checkedEntries).toBe(2); // OPENING (+100.00) + single BET (-25.00)
      } finally {
        await recoveryApp.close();
      }
    } finally {
      await lockSql.close();
      await extSqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl })).catch(() => undefined);
      extSqs.destroy();
    }
  }, 20_000);

  test('F1.2: OutboxPublisher SQS operation active past cooperative budget holds sqs.destroy() & orm.close(), and 0ms budget initiates zero SQS calls', async () => {
    const baseConfig = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app') });
    const extSqs = createLocalstackSqsClient(baseConfig);
    const isolatedEventsQueue = `integration-events-f1-${newId().slice(0, 8)}.fifo`;
    const createdQueue = await extSqs.send(
      new CreateQueueCommand({
        QueueName: isolatedEventsQueue,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }),
    );
    const queueUrl = createdQueue.QueueUrl!;

    try {
      const config = AppConfig.load({
        ...process.env,
        DATABASE_URL: testDatabaseUrl('app'),
        WORKERS_ENABLED: 'false',
        SQS_INTEGRATION_EVENTS_QUEUE: isolatedEventsQueue,
        SHUTDOWN_TIMEOUT_MS: '1000', // cooperative budget = 900ms
      });
      app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
      await app.listen(0, '127.0.0.1');
      const url = await app.getUrl();

      // Open a wallet with positive balance so real outbox_messages rows are queued in wagering_test.
      const openRes = await fetch(`${url}/wallets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerId: newId(), initialBalance: { amount: '50.00', currency: 'USD' } }),
      });
      expect(openRes.status).toBe(201);

      const shutdown = app.get(ShutdownService);
      const publisher = app.get(OutboxPublisherService);
      const appSqs = app.get(SQSClient);
      const orm = app.get(MikroORM);

      const events: string[] = [];
      let sqsDestroyed = false;
      let ormClosed = false;
      let releaseSqsBatch!: () => void;
      const sqsBatchGate = new Promise<void>((resolve) => {
        releaseSqsBatch = resolve;
      });
      let markSqsBatchEntered!: () => void;
      const sqsBatchEntered = new Promise<void>((resolve) => {
        markSqsBatchEntered = resolve;
      });
      const observedSignals: AbortSignal[] = [];

      const origSend = appSqs.send.bind(appSqs) as (cmd: unknown, opts?: { abortSignal?: AbortSignal }) => Promise<unknown>;
      const sendSpy = spyOn(appSqs as unknown as { send: typeof origSend }, 'send').mockImplementation(
        async (cmd: unknown, opts?: { abortSignal?: AbortSignal }) => {
          if (opts?.abortSignal !== undefined) {
            observedSignals.push(opts.abortSignal);
          }
          if (cmd instanceof SendMessageBatchCommand) {
            events.push('sqs-batch-started');
            markSqsBatchEntered();
            await sqsBatchGate;
            // Forward to REAL LocalStack SQS without the expired cooperative signal once we verify containment.
            const realResult = await origSend(cmd);
            events.push('sqs-batch-completed');
            return realResult;
          }
          return origSend(cmd, opts);
        },
      );

      const origDestroy = appSqs.destroy.bind(appSqs);
      const destroySpy = spyOn(appSqs, 'destroy').mockImplementation(() => {
        sqsDestroyed = true;
        events.push('sqs-destroy');
        origDestroy();
      });

      const origClose = orm.close.bind(orm);
      const closeSpy = spyOn(orm, 'close').mockImplementation(async (force?: boolean) => {
        await shutdown.waitForResourceIdle('postgres');
        ormClosed = true;
        events.push('orm-close');
        await origClose(force);
      });

      // Start real Outbox publishBatch; wait until it enters SendMessageBatchCommand while holding PostgreSQL FOR UPDATE SKIP LOCKED.
      const batchPromise = publisher.publishBatch().then((stats) => {
        events.push('publish-batch-settled');
        return stats;
      });
      await sqsBatchEntered;

      // Trigger shutdown and wait 960ms (past the 900ms cooperative budget).
      const closePromise = shutdown.closeApplication(app, 'SIGTERM');
      await Bun.sleep(960);

      expect(shutdown.didTimeout()).toBe(true);
      expect(sqsDestroyed).toBe(false);
      expect(ormClosed).toBe(false);
      expect(
        shutdown
          .activeOperationSnapshots()
          .some((s) => s.name === 'OutboxPublisherService.publishBatch' && s.state === 'exceeded_budget_active'),
      ).toBe(true);

      // Verify 0ms budget guard: attempting another SQS call with 0ms cooperative budget left must return exhausted!
      expect(shutdown.acquireSqsCallBudget(5_000, 'zero-budget-check')).toEqual({ exhausted: true });

      // Release SQS batch: real LocalStack receives the batch, PostgreSQL commits `published_at`, and ONLY THEN do sqs.destroy() & orm.close() execute!
      releaseSqsBatch();
      const stats = await batchPromise;
      await closePromise;
      app = undefined;

      expect(stats.published).toBeGreaterThanOrEqual(1);
      expect(events).toEqual([
        'sqs-batch-started',
        'sqs-batch-completed',
        'publish-batch-settled',
        'sqs-destroy',
        'orm-close',
      ]);
      expect(observedSignals.length).toBeGreaterThanOrEqual(1);

      sendSpy.mockRestore();
      destroySpy.mockRestore();
      closeSpy.mockRestore();
    } finally {
      await extSqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl })).catch(() => undefined);
      extSqs.destroy();
    }
  }, 15_000);

  test('F1.3: HTTP financial handler continues executing after client TCP disconnect, is NOT treated as cancelled, holds orm.close(), and preserves ledger consistency', async () => {
    const lockSql = createTestSql('app');
    try {
      const config = AppConfig.load({
        ...process.env,
        DATABASE_URL: testDatabaseUrl('app'),
        WORKERS_ENABLED: 'false',
        SHUTDOWN_TIMEOUT_MS: '1000', // cooperative budget = 900ms
      });
      app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
      await app.listen(0, '127.0.0.1');
      const url = await app.getUrl();
      const { host, port } = parseHostPort(url);

      // 1. Open real wallet ($100.00 USD) on wagering_test.
      const playerId = newId();
      const openRes = await fetch(`${url}/wallets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerId, initialBalance: { amount: '100.00', currency: 'USD' } }),
      });
      expect(openRes.status).toBe(201);
      const wallet = (await openRes.json()) as { id: string };

      const shutdown = app.get(ShutdownService);
      const orm = app.get(MikroORM);
      let ormClosed = false;
      let activePgOpsWhenOrmClosed = -1;
      const origClose = orm.close.bind(orm);
      const closeSpy = spyOn(orm, 'close').mockImplementation(async (force?: boolean) => {
        await shutdown.waitForResourceIdle('postgres');
        activePgOpsWhenOrmClosed = shutdown.activeCriticalOperationCount('postgres');
        ormClosed = true;
        await origClose(force);
      });

      // 2. Hold a real PostgreSQL row lock on `wallets` in wagering_test.
      let releaseRowLock!: () => void;
      const rowLockGate = new Promise<void>((resolve) => {
        releaseRowLock = resolve;
      });
      let markHeld!: () => void;
      const held = new Promise<void>((resolve) => {
        markHeld = resolve;
      });
      const lockTask = lockSql.begin(async (tx) => {
        await tx`select id from wallets where id = ${wallet.id} for update`;
        markHeld();
        await rowLockGate;
      });
      await held;

      // 3. Send a real HTTP POST /wagering/transactions ($30.00 BET) over a raw TCP socket.
      const betPayload = JSON.stringify({
        providerId: 'prov-f1-http',
        externalTransactionId: `ext-http-${newId()}`,
        playerId,
        walletId: wallet.id,
        roundId: 'round-http-1',
        gameId: 'roulette-1',
        kind: WagerTransactionKind.Bet,
        money: { amount: '30.00', currency: 'USD' },
      });
      const idempotencyKey = `idem-http-${newId()}`;

      const clientSocket = netConnect({ host, port });
      clientSocket.on('error', () => undefined);
      await new Promise<void>((resolve, reject) => {
        clientSocket.once('connect', () => resolve());
        clientSocket.once('error', reject);
      });
      clientSocket.write(
        `POST /wagering/transactions HTTP/1.1\r\n` +
          `Host: ${host}:${port}\r\n` +
          `Content-Type: application/json\r\n` +
          `Idempotency-Key: ${idempotencyKey}\r\n` +
          `X-Correlation-Id: corr-http-disconnect-f1\r\n` +
          `Content-Length: ${Buffer.byteLength(betPayload)}\r\n\r\n` +
          betPayload,
      );

      // Wait until PostgreSQL pg_stat_activity confirms WagerProcessorService.process is blocked on the wallet row lock.
      const waitDeadline = performance.now() + 4_000;
      let blockedOnLock = false;
      while (performance.now() < waitDeadline) {
        const rows = await lockSql`
          select count(*)::int as cnt
          from pg_stat_activity
          where datname = current_database()
            and wait_event_type = 'Lock'
            and state = 'active'
        `;
        if ((rows[0]?.cnt ?? 0) > 0 && shutdown.activeCriticalOperationCount('postgres') === 1) {
          blockedOnLock = true;
          break;
        }
        await Bun.sleep(15);
      }
      expect(blockedOnLock).toBe(true);
      expect(shutdown.inFlightHttpCount()).toBe(1);

      // 4. Abruptly destroy the client TCP socket!
      clientSocket.destroy();
      const disconnectDeadline = performance.now() + 1_000;
      while (shutdown.inFlightHttpCount() > 0 && performance.now() < disconnectDeadline) {
        await Bun.sleep(10);
      }

      // Socket is gone (`inFlightHttpCount === 0`), YET `WagerProcessorService.process` MUST remain tracked!
      expect(shutdown.inFlightHttpCount()).toBe(0);
      expect(shutdown.activeCriticalOperationCount('postgres')).toBe(1);

      // 5. Initiate shutdown and wait past the 900ms cooperative budget while the financial TX is still active!
      const closePromise = shutdown.closeApplication(app, 'SIGTERM');
      await Bun.sleep(960);

      expect(ormClosed).toBe(false);
      const activeOps = shutdown.activeOperationSnapshots();
      expect(activeOps.length).toBe(1);
      expect(activeOps[0]?.name).toBe('WagerProcessorService.process');
      expect(activeOps[0]?.state).toBe('exceeded_budget_active');
      // Verify sensitive payload fields (money/amount) are NOT leaked into diagnostic context!
      expect(activeOps[0]?.context).toEqual({
        walletId: wallet.id,
        correlationId: 'corr-http-disconnect-f1',
      });

      // 6. Release the wallet row lock: the BET commits ($100 - $30 = $70), settles as 'completed' (never 'cancelled'),
      //    and ONLY THEN does orm.close() execute!
      releaseRowLock();
      await lockTask;
      await closePromise;
      app = undefined;
      closeSpy.mockRestore();

      expect(ormClosed).toBe(true);
      expect(activePgOpsWhenOrmClosed).toBe(0);
      const settledOp = shutdown
        .recentSettledOperationSnapshots()
        .find((s) => s.name === 'WagerProcessorService.process');
      expect(settledOp?.state).toBe('completed');

      // 7. Verify financial integrity directly in wagering_test: exact $70.00 balance, version 2, 2 ledger entries, 0 duplicates.
      const walletRows = await lockSql`select balance_amount, version from wallets where id = ${wallet.id}`;
      expect(Number(walletRows[0]?.balance_amount)).toBe(70);
      expect(Number(walletRows[0]?.version)).toBe(2);

      const txRows = await lockSql`
        select kind, status, amount
        from wager_transactions
        where wallet_id = ${wallet.id}
        order by created_at asc
      `;
      expect(txRows.length).toBe(2); // OPENING + BET
      expect(txRows[1]?.kind).toBe('BET');
      expect(txRows[1]?.status).toBe('PROCESSED');
      expect(Number(txRows[1]?.amount)).toBe(30);
    } finally {
      await lockSql.close();
    }
  }, 20_000);

  test('F1.4: real subprocess exits with code 1 at hard deadline when a critical PostgreSQL operation never settles', async () => {
    const port = await allocateFreePort();
    const lockSql = createTestSql('app');
    const advisoryLockKey = 710_991;

    let releaseAdvisoryLock!: () => void;
    const advisoryGate = new Promise<void>((resolve) => {
      releaseAdvisoryLock = resolve;
    });
    let markLocked!: () => void;
    const locked = new Promise<void>((resolve) => {
      markLocked = resolve;
    });

    const holderPromise = lockSql.begin(async (tx) => {
      await tx`select pg_advisory_xact_lock(${advisoryLockKey})`;
      markLocked();
      await advisoryGate;
    });
    await locked;

    const childScript = `
      import 'reflect-metadata';
      import { MikroORM } from '@mikro-orm/postgresql';
      import { Logger } from '@nestjs/common';
      import { NestFactory } from '@nestjs/core';
      import { AppModule } from './src/app.module.ts';
      import { AppConfig } from './src/common/config/env.ts';
      import { installProcessShutdownHandlers, ShutdownService } from './src/common/http/shutdown.service.ts';
      import { createLogger } from './src/common/observability/logger.ts';

      const config = AppConfig.load(process.env);
      const app = await NestFactory.create(AppModule.forRoot(config), { logger: createLogger('log') });
      const shutdown = app.get(ShutdownService);
      const orm = app.get(MikroORM);
      installProcessShutdownHandlers({ app, shutdown, logger: new Logger('Bootstrap') });
      await app.listen(config.port, '127.0.0.1');

      // Start a REAL PostgreSQL operation blocked on wagering_test advisory lock.
      void shutdown.trackCriticalOperation(
        {
          kind: 'pending_reference',
          name: 'PendingReferenceWorker.processDueBatch',
          resources: ['postgres'],
        },
        () => orm.em.getConnection().execute('select pg_advisory_lock(${advisoryLockKey})'),
      ).catch(() => undefined);

      await Bun.sleep(60);
      process.emit('SIGTERM', 'SIGTERM');
    `;

    const child = Bun.spawn(['bun', '-e', childScript], {
      cwd: process.cwd(),
      env: childTestEnv({
        PORT: String(port),
        WORKERS_ENABLED: 'false',
        SHUTDOWN_TIMEOUT_MS: '1000',
      }),
      stdout: 'pipe',
      stderr: 'pipe',
    });

    try {
      const exitCode = await child.exited;
      const stdout = await new Response(child.stdout).text();
      const stderr = await new Response(child.stderr).text();
      const combined = `${stdout}\n${stderr}`;

      expect(exitCode).toBe(1);
      expect(combined).toContain('Critical operations remain active past cooperative shutdown budget');
      expect(combined).toContain('Graceful shutdown timed out; forcing exit');
      expect(combined).not.toContain('Graceful shutdown completed');
    } finally {
      child.kill();
      releaseAdvisoryLock();
      await holderPromise;
      await lockSql.close();
    }
  }, 15_000);

  test('F1.5: auxiliary SQS calls in WagerTransactionsConsumer and OutboxPublisherService cap AbortSignal to residual budget and skip 0ms calls without burning outbox attempts', async () => {
    const baseConfig = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app') });
    const extSqs = createLocalstackSqsClient(baseConfig);
    const isolatedMainQueue = `wager-tx-f1-aux-${newId().slice(0, 8)}.fifo`;
    const isolatedDlq = `wager-dlq-f1-aux-${newId().slice(0, 8)}.fifo`;
    const isolatedEventsQueue = `events-f1-aux-${newId().slice(0, 8)}.fifo`;

    const [createdMain, createdDlq, createdEvents] = await Promise.all([
      extSqs.send(
        new CreateQueueCommand({
          QueueName: isolatedMainQueue,
          Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
        }),
      ),
      extSqs.send(
        new CreateQueueCommand({
          QueueName: isolatedDlq,
          Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
        }),
      ),
      extSqs.send(
        new CreateQueueCommand({
          QueueName: isolatedEventsQueue,
          Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
        }),
      ),
    ]);
    const sql = createTestSql('app');

    try {
      const config = AppConfig.load({
        ...process.env,
        DATABASE_URL: testDatabaseUrl('app'),
        WORKERS_ENABLED: 'false',
        SQS_CALL_TIMEOUT_MS: '5000',
        SQS_WAGER_TRANSACTIONS_QUEUE: isolatedMainQueue,
        SQS_WAGER_TRANSACTIONS_DLQ: isolatedDlq,
        SQS_INTEGRATION_EVENTS_QUEUE: isolatedEventsQueue,
        SHUTDOWN_TIMEOUT_MS: '1000', // cooperative budget = 900ms
      });
      app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
      await app.listen(0, '127.0.0.1');
      const url = await app.getUrl();

      // Open wallet ($40.00) so outbox_messages has unpublished rows for this wallet.
      const playerId = newId();
      const openRes = await fetch(`${url}/wallets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerId, initialBalance: { amount: '40.00', currency: 'USD' } }),
      });
      expect(openRes.status).toBe(201);
      const wallet = (await openRes.json()) as { id: string };

      const shutdown = app.get(ShutdownService);
      const consumer = app.get(WagerTransactionsConsumer);
      const publisher = app.get(OutboxPublisherService);
      const appSqs = app.get(SQSClient);

      let now = 80_000;
      shutdown.setClockForTesting(() => now);
      shutdown.beginShutdown('SIGTERM');
      // Exhaust 100% of cooperative budget (900ms) -> 0ms remaining!
      now += 900;
      expect(shutdown.remainingCooperativeMs()).toBe(0);

      const sqsSpy = spyOn(appSqs, 'send');

      // 1. With 0ms residual budget:
      //    - OutboxPublisherService.publishBatch (getIntegrationQueueUrl + SendMessageBatchCommand) makes ZERO SQS calls and does NOT burn attempts!
      const zeroOutboxStats = await publisher.publishBatch();
      expect(zeroOutboxStats.selected).toBe(0);
      expect(sqsSpy).not.toHaveBeenCalled();

      const outboxRowsAfterZero = await sql`
        select attempts from outbox_messages where aggregate_id = ${wallet.id}
      `;
      expect(outboxRowsAfterZero.every((r: { attempts: number }) => Number(r.attempts) === 0)).toBe(true);

      //    - WagerTransactionsConsumer.handleMessage on a poison message (getDlqUrl + forwardPoisonToDlqAndAck) makes ZERO SQS calls and withholds ACK (`retry_later`)!
      const poisonOutcomeZero = await consumer.handleMessage({
        MessageId: `poison-zero-${newId()}`,
        ReceiptHandle: 'fake-receipt-handle',
        Body: '{not-valid-json',
      });
      expect(poisonOutcomeZero.status).toBe('retry_later');
      expect(sqsSpy).not.toHaveBeenCalled();
      sqsSpy.mockRestore();

      // 2. Rewind virtual clock to 120ms remaining (< 5000ms sqsCallTimeoutMs):
      //    Verify getDlqUrl + forwardPoisonToDlqAndAck + getMainQueueUrl + DeleteMessage + getIntegrationQueueUrl + SendMessageBatchCommand
      //    all execute with bounded AbortSignal and preserve DLQ-before-ACK / commit-before-ACK!
      now = 80_780; // 900 - 780 = 120ms remaining
      expect(shutdown.remainingCooperativeMs()).toBe(120);

      const recordedCommands: string[] = [];
      const origSend = appSqs.send.bind(appSqs) as (cmd: { constructor: { name: string } }, opts?: { abortSignal?: AbortSignal }) => Promise<unknown>;
      const boundedSpy = spyOn(appSqs as unknown as { send: typeof origSend }, 'send').mockImplementation(
        async (cmd: { constructor: { name: string } }, opts?: { abortSignal?: AbortSignal }) => {
          expect(opts?.abortSignal).toBeDefined();
          expect(opts?.abortSignal?.aborted).toBe(false);
          recordedCommands.push(cmd.constructor.name);
          return origSend(cmd, opts);
        },
      );

      // Send a real poison message to main queue, receive its real ReceiptHandle, and exercise handleMessage -> getDlqUrl -> SendMessageCommand(DLQ) -> getMainQueueUrl -> DeleteMessageCommand(main)!
      await extSqs.send(
        new SendMessageCommand({
          QueueUrl: createdMain.QueueUrl!,
          MessageBody: '{"broken":true}',
          MessageGroupId: wallet.id,
          MessageDeduplicationId: `dedup-poison-${newId()}`,
        }),
      );
      const { ReceiveMessageCommand: RecvCmd } = await import('@aws-sdk/client-sqs');
      const recv = await extSqs.send(
        new RecvCmd({
          QueueUrl: createdMain.QueueUrl!,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 0,
          AttributeNames: ['All'],
        }),
      );
      expect(recv.Messages?.length).toBe(1);

      const poisonHandled = await consumer.handleMessage(recv.Messages![0]!);
      expect(poisonHandled.status).toBe('dead_lettered');

      // Exercise OutboxPublisherService getIntegrationQueueUrl + SendMessageBatchCommand under bounded 120ms signal:
      const publishedUnderResidual = await publisher.publishBatch();
      expect(publishedUnderResidual.published).toBeGreaterThanOrEqual(1);

      boundedSpy.mockRestore();

      // Confirm DLQ SendMessage occurred strictly BEFORE DeleteMessage!
      const dlqSendIdx = recordedCommands.indexOf('SendMessageCommand');
      const mainDeleteIdx = recordedCommands.indexOf('DeleteMessageCommand');
      expect(dlqSendIdx).toBeGreaterThanOrEqual(0);
      expect(mainDeleteIdx).toBeGreaterThan(dlqSendIdx);
      expect(recordedCommands).toContain('GetQueueUrlCommand');
      expect(recordedCommands).toContain('SendMessageBatchCommand');
    } finally {
      await sql.close();
      await Promise.all([
        createdMain.QueueUrl ? extSqs.send(new DeleteQueueCommand({ QueueUrl: createdMain.QueueUrl })).catch(() => undefined) : Promise.resolve(),
        createdDlq.QueueUrl ? extSqs.send(new DeleteQueueCommand({ QueueUrl: createdDlq.QueueUrl })).catch(() => undefined) : Promise.resolve(),
        createdEvents.QueueUrl ? extSqs.send(new DeleteQueueCommand({ QueueUrl: createdEvents.QueueUrl })).catch(() => undefined) : Promise.resolve(),
      ]);
      extSqs.destroy();
    }
  }, 20_000);
});

describe('F2 — coordinated bootstrap, signals during init/listen, and EADDRINUSE teardown (real subprocesses)', () => {
  test.each(['SIGTERM', 'SIGINT'] as const)(
    'F2.1/F2.2: %s received during initialization prevents HTTP port from opening and exits cleanly without orphan resources',
    async (signal) => {
      const port = await allocateFreePort();

      // Instrument DatabaseModule.onModuleInit (which runs inside `await app.init()` BEFORE `app.listen()`)
      // and then execute the real `src/main.ts` bootstrap flow!
      const script = `
        import { DatabaseModule } from './src/database/database.module.ts';
        const origOnModuleInit = DatabaseModule.prototype.onModuleInit;
        DatabaseModule.prototype.onModuleInit = function () {
          origOnModuleInit.call(this);
          console.log('SIGNAL_DURING_APP_INIT');
          process.emit('${signal}', '${signal}');
        };
        await import('./src/main.ts');
      `;

      const child = Bun.spawn(['bun', '-e', script], {
        cwd: process.cwd(),
        env: childTestEnv({
          PORT: String(port),
          WORKERS_ENABLED: 'true',
          SQS_WAIT_TIME_SECONDS: '0',
          SHUTDOWN_TIMEOUT_MS: '3000',
        }),
        stdout: 'pipe',
        stderr: 'pipe',
      });

      try {
        const exitCode = await child.exited;
        const stdout = await new Response(child.stdout).text();
        const stderr = await new Response(child.stderr).text();
        const combined = `${stdout}\n${stderr}`;

        expect(exitCode).toBe(0);
        expect(combined).toContain('SIGNAL_DURING_APP_INIT');
        expect(combined).toContain('Skipping HTTP listen because shutdown started during application initialization');
        expect(combined).not.toContain('HTTP server listening');
        expect(combined).toContain('Graceful shutdown completed');
        expect(await isTcpPortOpen(port)).toBe(false);
      } finally {
        child.kill();
      }
    },
    15_000,
  );

  test('F2.3: concurrent SIGTERM + SIGINT during app.listen() serialize teardown behind bootstrapBarrier without concurrent app.close()', async () => {
    const port = await allocateFreePort();
    const script = `
      import { ExpressAdapter } from '@nestjs/platform-express';
      import { MikroORM } from '@mikro-orm/postgresql';
      import { DatabaseModule } from './src/database/database.module.ts';

      let ormClosedAt = 0;
      const origDbInit = DatabaseModule.prototype.onModuleInit;
      DatabaseModule.prototype.onModuleInit = function () {
        origDbInit.call(this);
        const orm = (this as unknown as { orm: MikroORM }).orm;
        const wrappedClose = orm.close.bind(orm);
        orm.close = async (force?: boolean) => {
          ormClosedAt = performance.now();
          console.log('ORM_CLOSED_DURING_SIGNAL_IN_LISTEN');
          await wrappedClose(force);
        };
      };

      const origListen = ExpressAdapter.prototype.listen;
      ExpressAdapter.prototype.listen = function (...args: unknown[]) {
        console.log('EMITTING_CONCURRENT_SIGNALS_DURING_LISTEN');
        process.emit('SIGTERM', 'SIGTERM');
        process.emit('SIGINT', 'SIGINT');
        return origListen.apply(this, args as never);
      };

      await import('./src/main.ts');
    `;

    const child = Bun.spawn(['bun', '-e', script], {
      cwd: process.cwd(),
      env: childTestEnv({
        PORT: String(port),
        WORKERS_ENABLED: 'true',
        SQS_WAIT_TIME_SECONDS: '0',
        SHUTDOWN_TIMEOUT_MS: '4000',
      }),
      stdout: 'pipe',
      stderr: 'pipe',
    });

    try {
      const exitCode = await child.exited;
      const stdout = await new Response(child.stdout).text();
      const stderr = await new Response(child.stderr).text();
      const combined = `${stdout}\n${stderr}`;

      expect(exitCode).toBe(0);
      expect(combined).toContain('EMITTING_CONCURRENT_SIGNALS_DURING_LISTEN');
      expect(combined).toContain('Duplicate shutdown signal received while shutdown is already in progress');
      expect(combined).toContain('ORM_CLOSED_DURING_SIGNAL_IN_LISTEN');
      expect(combined).toContain('Graceful shutdown completed');
      expect(await isTcpPortOpen(port)).toBe(false);
    } finally {
      child.kill();
    }
  }, 15_000);

  test('F2.4: EADDRINUSE during app.listen() stops all bootstrapped workers, closes SQS & PostgreSQL, and exits with code 1', async () => {
    // Occupy a real HTTP server port using `node:http`'s `createHttpServer` (matching Express/NestJS dual-stack binding on Windows Bun 1.4.5).
    const blocker = createHttpServer();
    await new Promise<void>((resolve, reject) => {
      blocker.once('error', reject);
      blocker.listen({ port: 0, exclusive: true }, () => resolve());
    });
    const occupiedPort = (blocker.address() as AddressInfo).port;

    const baseConfig = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app') });
    const extSqs = createLocalstackSqsClient(baseConfig);
    const isolatedQueueName = `wager-tx-f2-bind-${newId().slice(0, 8)}.fifo`;
    const createdQueue = await extSqs.send(
      new CreateQueueCommand({
        QueueName: isolatedQueueName,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }),
    );

    const script = `
      import { MikroORM } from '@mikro-orm/postgresql';
      import { DatabaseModule } from './src/database/database.module.ts';
      import { MessagingModule } from './src/messaging/messaging.module.ts';
      import { WagerTransactionsConsumer } from './src/messaging/wager-transactions.consumer.ts';
      import { OutboxPublisherService } from './src/messaging/outbox-publisher.service.ts';
      import { PendingReferenceWorker } from './src/wagering/pending-reference.worker.ts';

      const origDbInit = DatabaseModule.prototype.onModuleInit;
      DatabaseModule.prototype.onModuleInit = function () {
        origDbInit.call(this);
        const orm = (this as unknown as { orm: MikroORM }).orm;
        const wrappedClose = orm.close.bind(orm);
        orm.close = async (force?: boolean) => {
          await wrappedClose(force);
          console.log('ORM_CLOSED_ON_BIND_FAILURE');
        };
      };

      const origMsgShutdown = MessagingModule.prototype.onApplicationShutdown;
      MessagingModule.prototype.onApplicationShutdown = async function () {
        await origMsgShutdown.call(this);
        console.log('SQS_DESTROYED_ON_BIND_FAILURE');
      };

      const origConsumerStart = WagerTransactionsConsumer.prototype.start;
      WagerTransactionsConsumer.prototype.start = function () {
        console.log('WORKER_STARTED:WagerTransactionsConsumer');
        return origConsumerStart.call(this);
      };
      const origConsumerStop = WagerTransactionsConsumer.prototype.stop;
      WagerTransactionsConsumer.prototype.stop = async function () {
        await origConsumerStop.call(this);
        console.log('WORKER_STOPPED:WagerTransactionsConsumer');
      };

      const origOutboxStart = OutboxPublisherService.prototype.start;
      OutboxPublisherService.prototype.start = function () {
        console.log('WORKER_STARTED:OutboxPublisherService');
        return origOutboxStart.call(this);
      };
      const origOutboxStop = OutboxPublisherService.prototype.stop;
      OutboxPublisherService.prototype.stop = async function () {
        await origOutboxStop.call(this);
        console.log('WORKER_STOPPED:OutboxPublisherService');
      };

      const origPendingStart = PendingReferenceWorker.prototype.start;
      PendingReferenceWorker.prototype.start = function () {
        console.log('WORKER_STARTED:PendingReferenceWorker');
        return origPendingStart.call(this);
      };
      const origPendingStop = PendingReferenceWorker.prototype.stop;
      PendingReferenceWorker.prototype.stop = async function () {
        await origPendingStop.call(this);
        console.log('WORKER_STOPPED:PendingReferenceWorker');
      };

      await import('./src/main.ts');
    `;

    const child = Bun.spawn(['bun', '-e', script], {
      cwd: process.cwd(),
      env: childTestEnv({
        PORT: String(occupiedPort),
        WORKERS_ENABLED: 'true',
        SQS_WAIT_TIME_SECONDS: '0',
        SQS_WAGER_TRANSACTIONS_QUEUE: isolatedQueueName,
        SHUTDOWN_TIMEOUT_MS: '4000',
      }),
      stdout: 'pipe',
      stderr: 'pipe',
    });

    try {
      const exitCode = await child.exited;
      const stdout = await new Response(child.stdout).text();
      const stderr = await new Response(child.stderr).text();
      const combined = `${stdout}\n${stderr}`;

      expect(exitCode).toBe(1);
      // Confirm workers had actually started during `app.init()` BEFORE `app.listen()` failed with EADDRINUSE:
      expect(combined).toContain('WORKER_STARTED:WagerTransactionsConsumer');
      expect(combined).toContain('WORKER_STARTED:OutboxPublisherService');
      expect(combined).toContain('WORKER_STARTED:PendingReferenceWorker');
      // Confirm every started worker was stopped BEFORE SQS & PostgreSQL closed during bootstrap-failure teardown:
      expect(combined).toContain('WORKER_STOPPED:WagerTransactionsConsumer');
      expect(combined).toContain('WORKER_STOPPED:OutboxPublisherService');
      expect(combined).toContain('WORKER_STOPPED:PendingReferenceWorker');
      expect(combined).toContain('SQS_DESTROYED_ON_BIND_FAILURE');
      expect(combined).toContain('ORM_CLOSED_ON_BIND_FAILURE');
      expect(combined.lastIndexOf('WORKER_STOPPED:')).toBeLessThan(combined.indexOf('SQS_DESTROYED_ON_BIND_FAILURE'));
      expect(combined.lastIndexOf('WORKER_STOPPED:')).toBeLessThan(combined.indexOf('ORM_CLOSED_ON_BIND_FAILURE'));
      expect(combined).toContain('Application bootstrap failed; tearing down initialized resources');
      expect(combined).toContain('EADDRINUSE');
    } finally {
      child.kill();
      await new Promise<void>((resolve) => blocker.close(() => resolve()));
      if (createdQueue.QueueUrl !== undefined) {
        await extSqs.send(new DeleteQueueCommand({ QueueUrl: createdQueue.QueueUrl })).catch(() => undefined);
      }
      extSqs.destroy();
    }
  }, 20_000);
});

describe('F5, F6 & F7 — pre-parser shutdown hook, referenced post-failure exit timer & full lifecycle order', () => {
  test('F5: slow/large POST JSON upload during shutdown receives immediate 503 before express.json() and without MikroOrmMiddleware RequestContext', async () => {
    const config = AppConfig.load({
      ...process.env,
      DATABASE_URL: testDatabaseUrl('app'),
      WORKERS_ENABLED: 'false',
      SHUTDOWN_TIMEOUT_MS: '3000',
    });
    app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
    await app.listen(0, '127.0.0.1');
    const url = await app.getUrl();
    const { host, port } = parseHostPort(url);

    // 1. Sanity check: normal JSON POST /wallets works normally before shutdown.
    const normalRes = await fetch(`${url}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Correlation-Id': 'corr-f5-normal' },
      body: JSON.stringify({ playerId: newId(), initialBalance: { amount: '10.00', currency: 'USD' } }),
    });
    expect(normalRes.status).toBe(201);
    expect(normalRes.headers.get('x-correlation-id')).toBe('corr-f5-normal');

    // 2. Spy on MikroOrmMiddleware (RequestContext creation) and WagerProcessorService (business handler).
    const mikroOrmMiddlewareSpy = spyOn(MikroOrmMiddleware.prototype, 'use');
    const wagerProcessSpy = spyOn(WagerProcessorService.prototype, 'process');

    const shutdown = app.get(ShutdownService);
    shutdown.beginShutdown('SIGTERM');

    // 3. Open a raw TCP socket advertising a 1 MB JSON body, but send only 20 bytes and stall!
    const slowSocket = netConnect({ host, port });
    slowSocket.on('error', () => undefined);
    await new Promise<void>((resolve, reject) => {
      slowSocket.once('connect', () => resolve());
      slowSocket.once('error', reject);
    });

    const responseReceived = new Promise<string>((resolve, reject) => {
      let raw = '';
      const timer = setTimeout(() => reject(new Error('Timed out waiting for immediate 503 on stalled POST body')), 1_500);
      slowSocket.on('data', (chunk: Buffer) => {
        raw += chunk.toString('utf8');
        if (raw.includes('\r\n\r\n') && raw.includes('"SERVICE_UNAVAILABLE"')) {
          clearTimeout(timer);
          resolve(raw);
        }
      });
    });

    const startedAt = performance.now();
    slowSocket.write(
      `POST /wagering/transactions HTTP/1.1\r\n` +
        `Host: ${host}:${port}\r\n` +
        `Content-Type: application/json\r\n` +
        `Content-Length: 1048576\r\n` +
        `X-Correlation-Id: corr-f5-slow-upload\r\n\r\n` +
        `{"walletId":"stalled`,
    );

    const rawHttp503 = await responseReceived;
    const elapsedMs = performance.now() - startedAt;
    slowSocket.destroy();

    expect(elapsedMs).toBeLessThan(300);
    expect(rawHttp503).toContain('HTTP/1.1 503 Service Unavailable');
    expect(rawHttp503.toLowerCase()).toContain('connection: close');
    expect(rawHttp503.toLowerCase()).toContain('x-correlation-id: corr-f5-slow-upload');
    expect(rawHttp503).toContain('"correlationId":"corr-f5-slow-upload"');

    // Neither MikroOrmMiddleware (RequestContext) nor WagerProcessorService was invoked!
    expect(mikroOrmMiddlewareSpy).not.toHaveBeenCalled();
    expect(wagerProcessSpy).not.toHaveBeenCalled();

    mikroOrmMiddlewareSpy.mockRestore();
    wagerProcessSpy.mockRestore();
  });

  test('F6: referenced post-failure exit timer exits subprocess deterministically with code 1 even with zero other active handles', async () => {
    const script = `
      import 'reflect-metadata';
      import { Logger } from '@nestjs/common';
      import { AppConfig } from './src/common/config/env.ts';
      import { createProcessShutdownController, ShutdownService } from './src/common/http/shutdown.service.ts';

      const shutdown = new ShutdownService({ shutdownTimeoutMs: 1000 } as AppConfig);
      // Fake app with zero open network/DB handles; markTimedOut simulates an overrun that settled right before app.close finished.
      const fakeApp = {
        getHttpServer: () => ({}),
        close: async () => {
          shutdown.markTimedOut('simulated overrun that settled during teardown');
        },
        flushLogs: () => undefined,
      };

      const controller = createProcessShutdownController({
        app: fakeApp as never,
        shutdown,
        logger: new Logger('F6Subprocess'),
      });

      await controller.triggerSignal('SIGTERM');
      // At this point main watchdog is cleared and zero other handles exist — ONLY the referenced F6 timer keeps the loop alive to exit(1)!
      console.log('AWAITING_REFERENCED_POST_FAILURE_EXIT_TIMER');
    `;

    const child = Bun.spawn(['bun', '-e', script], {
      cwd: process.cwd(),
      env: childTestEnv(),
      stdout: 'pipe',
      stderr: 'pipe',
    });

    try {
      const exitCode = await child.exited;
      const stdout = await new Response(child.stdout).text();
      const stderr = await new Response(child.stderr).text();
      const combined = `${stdout}\n${stderr}`;

      expect(exitCode).toBe(1);
      expect(combined).toContain('AWAITING_REFERENCED_POST_FAILURE_EXIT_TIMER');
      expect(combined).toContain('Shutdown completed with timeout or teardown failure');
    } finally {
      child.kill();
    }
  }, 10_000);

  test('F7: full end-to-end NestJS lifecycle hook ordering stops workers, drains HTTP & critical ops, closes HTTP server, and only then closes SQS & PostgreSQL', async () => {
    const baseConfig = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app') });
    const extSqs = createLocalstackSqsClient(baseConfig);
    const isolatedQueueName = `wager-tx-f7-order-${newId().slice(0, 8)}.fifo`;
    const createdQueue = await extSqs.send(
      new CreateQueueCommand({
        QueueName: isolatedQueueName,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
      }),
    );

    try {
      const config = AppConfig.load({
        ...process.env,
        DATABASE_URL: testDatabaseUrl('app'),
        WORKERS_ENABLED: 'true',
        SQS_WAIT_TIME_SECONDS: '0',
        SQS_WAGER_TRANSACTIONS_QUEUE: isolatedQueueName,
        SHUTDOWN_TIMEOUT_MS: '4000',
      });
      app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
      await app.listen(0, '127.0.0.1');

      const shutdown = app.get(ShutdownService);
      const consumer = app.get(WagerTransactionsConsumer);
      const publisher = app.get(OutboxPublisherService);
      const pendingWorker = app.get(PendingReferenceWorker);
      const sqs = app.get(SQSClient);
      const orm = app.get(MikroORM);
      const httpServer = app.getHttpServer() as Server;

      const sequence: string[] = [];

      const origConsumerDestroy = consumer.onModuleDestroy.bind(consumer);
      const consumerSpy = spyOn(consumer, 'onModuleDestroy').mockImplementation(async () => {
        await origConsumerDestroy();
        sequence.push('worker:WagerTransactionsConsumer.onModuleDestroy');
      });

      const origPublisherDestroy = publisher.onModuleDestroy.bind(publisher);
      const publisherSpy = spyOn(publisher, 'onModuleDestroy').mockImplementation(async () => {
        await origPublisherDestroy();
        sequence.push('worker:OutboxPublisherService.onModuleDestroy');
      });

      const origPendingDestroy = pendingWorker.onModuleDestroy.bind(pendingWorker);
      const pendingSpy = spyOn(pendingWorker, 'onModuleDestroy').mockImplementation(async () => {
        await origPendingDestroy();
        sequence.push('worker:PendingReferenceWorker.onModuleDestroy');
      });

      const origBeforeShutdown = shutdown.beforeApplicationShutdown.bind(shutdown);
      const beforeShutdownSpy = spyOn(shutdown, 'beforeApplicationShutdown').mockImplementation(async () => {
        sequence.push('drain:ShutdownService.beforeApplicationShutdown:start');
        await origBeforeShutdown();
        sequence.push('drain:ShutdownService.beforeApplicationShutdown:end');
      });

      httpServer.once('close', () => {
        sequence.push('http:httpServer.close');
      });

      const origSqsDestroy = sqs.destroy.bind(sqs);
      const sqsSpy = spyOn(sqs, 'destroy').mockImplementation(() => {
        expect(shutdown.activeCriticalOperationCount('sqs')).toBe(0);
        sequence.push('resource:SQSClient.destroy');
        origSqsDestroy();
      });

      const origOrmClose = orm.close.bind(orm);
      const ormSpy = spyOn(orm, 'close').mockImplementation(async (force?: boolean) => {
        await shutdown.waitForResourceIdle('postgres');
        expect(shutdown.activeCriticalOperationCount('postgres')).toBe(0);
        sequence.push('resource:MikroORM.close');
        await origOrmClose(force);
      });

      await shutdown.closeApplication(app, 'SIGTERM');
      app = undefined;

      consumerSpy.mockRestore();
      publisherSpy.mockRestore();
      pendingSpy.mockRestore();
      beforeShutdownSpy.mockRestore();
      sqsSpy.mockRestore();
      ormSpy.mockRestore();

      const workerIndices = [
        sequence.indexOf('worker:WagerTransactionsConsumer.onModuleDestroy'),
        sequence.indexOf('worker:OutboxPublisherService.onModuleDestroy'),
        sequence.indexOf('worker:PendingReferenceWorker.onModuleDestroy'),
      ];
      const drainStartIdx = sequence.indexOf('drain:ShutdownService.beforeApplicationShutdown:start');
      const drainEndIdx = sequence.indexOf('drain:ShutdownService.beforeApplicationShutdown:end');
      const httpCloseIdx = sequence.indexOf('http:httpServer.close');
      const sqsDestroyIdx = sequence.indexOf('resource:SQSClient.destroy');
      const ormCloseIdx = sequence.indexOf('resource:MikroORM.close');

      for (const idx of workerIndices) {
        expect(idx).toBeGreaterThanOrEqual(0);
        expect(idx).toBeLessThan(drainStartIdx);
      }
      expect(drainStartIdx).toBeLessThan(drainEndIdx);
      expect(drainEndIdx).toBeLessThan(httpCloseIdx);
      expect(httpCloseIdx).toBeLessThan(sqsDestroyIdx);
      expect(httpCloseIdx).toBeLessThan(ormCloseIdx);
      expect(shutdown.isHttpDrainCompleted()).toBe(true);
      expect(shutdown.hasFailed()).toBe(false);
    } finally {
      if (createdQueue.QueueUrl !== undefined) {
        await extSqs.send(new DeleteQueueCommand({ QueueUrl: createdQueue.QueueUrl })).catch(() => undefined);
      }
      extSqs.destroy();
    }
  }, 20_000);
});
