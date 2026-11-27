import 'reflect-metadata';
import { EventEmitter } from 'node:events';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { Writable } from 'node:stream';
import { describe, expect, test } from 'bun:test';
import { ServiceUnavailableException } from '@nestjs/common';
import { AppConfig } from '../../src/common/config/env';
import { HealthController } from '../../src/common/health/health.controller';
import {
  CooperativeBudgetExhaustedError,
  createHttpDrainMiddleware,
  ShutdownService,
} from '../../src/common/http/shutdown.service';
import { newId } from '../../src/common/ids';
import { Money } from '../../src/common/money/money';
import { OutboxPublisherService } from '../../src/messaging/outbox-publisher.service';
import { BatchVisibilityLeaseManager } from '../../src/messaging/sqs-visibility-lease';
import { PendingReferenceWorker } from '../../src/wagering/pending-reference.worker';
import { parseWagerRequest } from '../../src/wagering/wager-payload';
import { WagerProcessorService } from '../../src/wagering/wager-processor.service';
import { WagerTransactionKind } from '../../src/wagering/wager-transaction';
import { WalletsService } from '../../src/wallets/wallets.service';

function makeConfig(overrides: Record<string, string> = {}): AppConfig {
  return AppConfig.load({
    NODE_ENV: 'test',
    PORT: '0',
    DATABASE_URL: 'postgres://wagering_app:wagering_app@localhost:5432/wagering_test',
    AWS_REGION: 'us-east-1',
    AWS_ENDPOINT_URL: 'http://localhost:4566',
    AWS_ACCESS_KEY_ID: 'test',
    AWS_SECRET_ACCESS_KEY: 'test',
    SQS_WAGER_TRANSACTIONS_QUEUE: 'wager-transactions.fifo',
    SQS_WAGER_TRANSACTIONS_DLQ: 'wager-transactions-dlq.fifo',
    SQS_INTEGRATION_EVENTS_QUEUE: 'wagering-events.fifo',
    SHUTDOWN_TIMEOUT_MS: '2000',
    OUTBOX_POLL_INTERVAL_MS: '60000',
    PENDING_REFERENCE_POLL_INTERVAL_MS: '60000',
    ...overrides,
  });
}

describe('Stage 7.5 — Shutdown race guards & telemetry unit checks', () => {
  test('HealthController.ready() returns 503 with shuttingDown: true when beginShutdown() races in-flight dependency probes', async () => {
    const config = makeConfig();
    const shutdown = new ShutdownService(config);

    let releasePgProbe!: () => void;
    const pgProbeGate = new Promise<void>((resolve) => {
      releasePgProbe = resolve;
    });

    const ormMock = {
      em: {
        getConnection: () => ({
          execute: async () => {
            await pgProbeGate;
            return [{ '?column?': 1 }];
          },
        }),
      },
    };
    const sqsMock = {
      send: async () => ({ QueueUrl: 'http://localhost:4566/000000000000/q.fifo' }),
    };

    const controller = new HealthController(ormMock as never, sqsMock as never, config, shutdown);

    // Start /health/ready BEFORE shutdown begins:
    const readyPromise = controller.ready();
    await Promise.resolve();

    // Trigger shutdown WHILE pingPostgres is still awaiting:
    expect(shutdown.beginShutdown('SIGTERM')).toBe(true);
    releasePgProbe();

    let caught: unknown;
    try {
      await readyPromise;
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ServiceUnavailableException);
    const body = (caught as ServiceUnavailableException).getResponse() as Record<string, unknown>;
    expect(body.status).toBe('unavailable');
    expect(body.shuttingDown).toBe(true);
  });

  test('WagerProcessorService & WalletsService reject new SQL transactions with 503 once cooperative HTTP drain expires, while draining within budget', async () => {
    let monoNow = 1_000;
    const config = makeConfig({ SHUTDOWN_TIMEOUT_MS: '1000' });
    const shutdown = new ShutdownService(config);
    shutdown.setClockForTesting(() => monoNow);

    let txInvocations = 0;
    const ormMock = {
      em: {
        fork: () => ({
          transactional: async () => {
            txInvocations += 1;
            return { transaction: {} as never, idempotentReplay: false };
          },
        }),
      },
    };

    const processor = new WagerProcessorService(ormMock as never, config, shutdown);
    const wallets = new WalletsService(ormMock as never, shutdown);

    const parsed = parseWagerRequest(
      {
        providerId: 'prov-u',
        externalTransactionId: `ext-${newId()}`,
        playerId: newId(),
        walletId: newId(),
        roundId: 'round-1',
        gameId: 'slots',
        kind: WagerTransactionKind.Bet,
        money: { amount: '10.00', currency: 'BRL' },
      },
      `idem-${newId()}`,
    );

    // Begin shutdown with cooperative budget STILL remaining (900ms left):
    shutdown.beginShutdown('SIGTERM');
    expect(shutdown.remainingCooperativeMs()).toBe(900);

    // Admitted request reaching processor within cooperative budget IS allowed to drain:
    await processor.process({ ...parsed, correlationId: 'corr-within-budget' });
    expect(txInvocations).toBe(1);

    // Advance clock past cooperative deadline (remainingCooperativeMs() === 0):
    monoNow += 950;
    expect(shutdown.remainingCooperativeMs()).toBe(0);

    let processorErr: unknown;
    try {
      await processor.process({ ...parsed, correlationId: 'corr-expired' });
    } catch (err) {
      processorErr = err;
    }
    expect(processorErr).toBeInstanceOf(ServiceUnavailableException);
    expect((processorErr as ServiceUnavailableException).getResponse()).toEqual({
      code: 'SERVICE_UNAVAILABLE',
      message: 'Service is shutting down; retry later',
      correlationId: 'corr-expired',
    });

    let walletErr: unknown;
    try {
      await wallets.open(newId(), Money.from({ amount: '50.00', currency: 'BRL' }), 'corr-wallet-expired');
    } catch (err) {
      walletErr = err;
    }
    expect(walletErr).toBeInstanceOf(ServiceUnavailableException);
    // Zero new SQL transactions opened after cooperative deadline expired!
    expect(txInvocations).toBe(1);
  });

  test('OutboxPublisherService and PendingReferenceWorker autonomous loops wake and exit immediately when shutdownSignal fires', async () => {
    const config = makeConfig({
      OUTBOX_POLL_INTERVAL_MS: '60000',
      PENDING_REFERENCE_POLL_INTERVAL_MS: '60000',
    });
    const shutdown = new ShutdownService(config);

    let outboxCycles = 0;
    const ormForOutbox = {
      em: {
        fork: () => ({
          transactional: async () => ({
            selected: 0,
            published: 0,
            failed: 0,
            quarantined: 0,
            publishedIds: [],
          }),
          execute: async () => [],
        }),
      },
    };
    const sqsForOutbox = {
      send: async () => {
        outboxCycles += 1;
        return { QueueUrl: 'http://localhost:4566/000000000000/wagering-events.fifo' };
      },
    };

    const outbox = new OutboxPublisherService(ormForOutbox as never, sqsForOutbox as never, config, shutdown);
    const pendingWorker = new PendingReferenceWorker(
      ormForOutbox as never,
      {} as never,
      config,
      shutdown,
    );

    outbox.start();
    pendingWorker.start();
    await Bun.sleep(20);
    expect(outboxCycles).toBe(1);

    // Both workers are now sleeping on a 60s timer; beginShutdown must wake and end both loops immediately (< 100ms):
    const started = performance.now();
    shutdown.beginShutdown('SIGTERM');
    await Promise.all([outbox.stop(), pendingWorker.stop()]);
    const elapsed = performance.now() - started;
    expect(elapsed).toBeLessThan(250);
  });

  test('ShutdownService.shutdownSummary and BatchVisibilityLeaseManager.leaseStateCounts accurately report drained, aborted, and rejected work', async () => {
    const config = makeConfig({ SHUTDOWN_TIMEOUT_MS: '2000' });
    const shutdown = new ShutdownService(config);
    const middleware = createHttpDrainMiddleware(shutdown);

    const makeMockHttpPair = (url: string) => {
      const emitter = new EventEmitter();
      const socket = Object.assign(emitter, {
        destroyed: false,
        destroy() {
          socket.destroyed = true;
          emitter.emit('close');
        },
      });
      const req = Object.assign(new EventEmitter(), {
        url,
        method: 'POST',
        headers: {},
        complete: true,
        socket,
      }) as unknown as IncomingMessage;
      const headers = new Map<string, string>();
      const writable = new Writable({
        write(_chunk, _enc, cb) {
          cb();
        },
      });
      const res = Object.assign(writable, {
        statusCode: 200,
        headersSent: false,
        setHeader(k: string, v: string) {
          headers.set(k.toLowerCase(), v);
        },
        getHeader(k: string) {
          return headers.get(k.toLowerCase());
        },
      }) as unknown as ServerResponse;
      return { req, res };
    };

    // Admit 2 HTTP requests and 2 critical operations BEFORE shutdown:
    const admitted1 = makeMockHttpPair('/wagering/transactions');
    const admitted2 = makeMockHttpPair('/wagering/transactions');
    middleware(admitted1.req, admitted1.res, () => undefined);
    middleware(admitted2.req, admitted2.res, () => undefined);

    let finishOp1!: () => void;
    let cancelOp2!: (err: Error) => void;
    const op1 = shutdown.trackCriticalOperation(
      { kind: 'http_financial', name: 'op1', resources: ['postgres'] },
      () => new Promise<void>((resolve) => { finishOp1 = resolve; }),
    );
    const op2 = shutdown
      .trackCriticalOperation(
        { kind: 'worker', name: 'op2', resources: ['sqs'] },
        () => new Promise<void>((_resolve, reject) => { cancelOp2 = reject; }),
      )
      .catch(() => undefined);

    await Promise.resolve();
    shutdown.beginShutdown('SIGTERM');

    // Re-invoking middleware on already-admitted request (post-body-parser route layer) must advance to next(), NOT reject 503:
    let routeLayerNextCalled = false;
    middleware(admitted1.req, admitted1.res, () => {
      routeLayerNextCalled = true;
    });
    expect(routeLayerNextCalled).toBe(true);
    expect(admitted1.res.statusCode).toBe(200);
    expect(admitted1.res.getHeader('connection')).toBe('close');

    // Reject 1 new HTTP request arriving during shutdown:
    const rejectedReq = makeMockHttpPair('/wagering/transactions');
    middleware(rejectedReq.req, rejectedReq.res, () => undefined);
    expect(rejectedReq.res.statusCode).toBe(503);

    // Drain admitted1 + op1; abort admitted2 + op2:
    admitted1.res.end();
    admitted2.res.destroy();
    finishOp1();
    cancelOp2(new CooperativeBudgetExhaustedError('op2'));
    await Promise.all([op1, op2]);
    await Bun.sleep(10);
    shutdown.markShutdownCompleted();

    const summary = shutdown.shutdownSummary();
    expect(summary.trigger).toBe('SIGTERM');
    expect(summary.startedAt).not.toBeNull();
    expect(summary.completedAt).not.toBeNull();
    expect(summary.inFlightHttpAtStart).toBe(2);
    expect(summary.drainedHttpRequests).toBe(1);
    expect(summary.abortedHttpRequests).toBe(1);
    expect(summary.rejectedHttpRequests).toBe(1);
    expect(summary.inFlightOperationsAtStart).toBe(2);
    expect(summary.drainedCriticalOperations).toBe(1);
    expect(summary.abortedCriticalOperations).toBe(1);

    // Lease state counts + no redundant timer scheduling when shutting down with 0 in_flight entries:
    const leaseMgr = new BatchVisibilityLeaseManager({
      sqs: { send: async () => ({ Successful: [], Failed: [] }) },
      queueUrl: 'http://localhost:4566/000000000000/q.fifo',
      messages: [
        { MessageId: 'm1', ReceiptHandle: 'rh1', Attributes: { MessageGroupId: 'g1' } },
        { MessageId: 'm2', ReceiptHandle: 'rh2', Attributes: { MessageGroupId: 'g2' } },
      ],
      visibilityTimeoutSeconds: 3,
      defaultCallTimeoutMs: 500,
      logger: { warn: () => undefined },
      shutdown,
      isStopping: () => true,
      isGroupBlocked: () => false,
    });
    expect(leaseMgr.claimForExecution(0)).toBe(true);
    leaseMgr.markCompleted(0);
    leaseMgr.markReleasedFrom(1);
    expect(leaseMgr.leaseStateCounts()).toEqual({
      queued: 0,
      in_flight: 0,
      completed: 1,
      released: 1,
      lost: 0,
    });
    await leaseMgr.stop();
  });
});
