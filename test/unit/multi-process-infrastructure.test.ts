import { describe, expect, test } from 'bun:test';
import { ChangeMessageVisibilityBatchCommand, type Message } from '@aws-sdk/client-sqs';
import { AppConfig, InvalidEnvironmentError } from '../../src/common/config/env';
import { ShutdownService } from '../../src/common/http/shutdown.service';
import {
  computeJitteredDelayMs,
  generateDefaultInstanceId,
  INSTANCE_ID_HEADER,
  isValidInstanceId,
  toPostgresApplicationName,
} from '../../src/common/instance/instance-identity';
import { createLogger } from '../../src/common/observability/logger';
import { createOrmOptions } from '../../src/database/mikro-orm.config';
import { resolveBoundPort } from '../../src/main';
import {
  BatchVisibilityLeaseManager,
  deriveVisibilityHeartbeatIntervalMs,
} from '../../src/messaging/sqs-visibility-lease';

const BASE_ENV: Record<string, string> = {
  DATABASE_URL: 'postgresql://wagering_app:secret@localhost:5433/wagering_test',
  AWS_REGION: 'us-east-1',
  AWS_ENDPOINT_URL: 'http://localhost:4566',
};

describe('Instance identity, PORT=0 binding, logger attribution, and ORM application_name (Stage 7.2)', () => {
  test('generates unique valid default instance IDs and clamps PostgreSQL application_name to <= 63 bytes', () => {
    const id1 = generateDefaultInstanceId(1234, 'Worker.Host_01.Internal.Local');
    const id2 = generateDefaultInstanceId(1234, 'Worker.Host_01.Internal.Local');

    expect(id1).not.toBe(id2);
    expect(isValidInstanceId(id1)).toBe(true);
    expect(isValidInstanceId(id2)).toBe(true);
    expect(INSTANCE_ID_HEADER).toBe('x-instance-id');

    const longId = `n${'a'.repeat(62)}`;
    expect(isValidInstanceId(longId)).toBe(true);
    const pgAppName = toPostgresApplicationName(longId);
    expect(pgAppName.startsWith('wagering:')).toBe(true);
    expect(pgAppName.length).toBeLessThanOrEqual(63);
  });

  test('AppConfig accepts PORT=0 (ephemeral OS port) and validates INSTANCE_ID strictly', () => {
    const ephemeral = AppConfig.load({
      ...BASE_ENV,
      PORT: '0',
      INSTANCE_ID: 'wagering-node-01:az1',
    });
    expect(ephemeral.port).toBe(0);
    expect(ephemeral.instanceId).toBe('wagering-node-01:az1');

    const autoAssigned = AppConfig.load(BASE_ENV);
    expect(isValidInstanceId(autoAssigned.instanceId)).toBe(true);

    expect(() =>
      AppConfig.load({
        ...BASE_ENV,
        INSTANCE_ID: '-invalid-leading-dash',
      }),
    ).toThrow(InvalidEnvironmentError);

    expect(() =>
      AppConfig.load({
        ...BASE_ENV,
        INSTANCE_ID: 'invalid space in id',
      }),
    ).toThrow(InvalidEnvironmentError);

    expect(() =>
      AppConfig.load({
        ...BASE_ENV,
        INSTANCE_ID: `x${'1'.repeat(63)}`,
      }),
    ).toThrow(InvalidEnvironmentError);
  });

  test('createOrmOptions wires driverOptions.application_name per instance without global context', () => {
    const withInstance = createOrmOptions({
      databaseUrl: BASE_ENV.DATABASE_URL!,
      databasePoolMax: 7,
      instanceId: 'worker-alpha',
    });
    expect(withInstance.allowGlobalContext).toBe(false);
    expect(withInstance.pool).toEqual({ min: 0, max: 7 });
    expect((withInstance.driverOptions as Record<string, unknown> | undefined)?.application_name).toBe(
      'wagering:worker-alpha',
    );

    const withoutInstance = createOrmOptions({
      databaseUrl: BASE_ENV.DATABASE_URL!,
      databasePoolMax: 5,
    });
    expect((withoutInstance.driverOptions as Record<string, unknown> | undefined)?.application_name).toBeUndefined();
    expect((withoutInstance.driverOptions as Record<string, unknown> | undefined)?.options).toBe(
      '-c client_connection_check_interval=250',
    );
    expect((withoutInstance.driverOptions as Record<string, unknown> | undefined)?.keepAlive).toBe(true);
  });

  test('computeJitteredDelayMs stays within bounded [0.8x, 1.2x] window and handles zero delay', () => {
    expect(computeJitteredDelayMs(0)).toBe(0);
    expect(computeJitteredDelayMs(200, () => 0)).toBe(160);
    expect(computeJitteredDelayMs(200, () => 0.5)).toBe(200);
    expect(computeJitteredDelayMs(200, () => 1)).toBe(240);
    expect(computeJitteredDelayMs(6, () => 0)).toBe(5);
  });

  test('resolveBoundPort extracts OS-assigned port from server.address() and falls back safely', () => {
    expect(resolveBoundPort({ address: () => ({ address: '0.0.0.0', family: 'IPv4', port: 54123 }) }, 0)).toBe(54123);
    expect(resolveBoundPort({ address: () => null }, 3000)).toBe(3000);
    expect(resolveBoundPort(undefined, 3000)).toBe(3000);
  });

  test('createLogger attaches instanceId to structured JSON log objects when provided', () => {
    const captured: string[] = [];
    const origWrite = process.stdout.write.bind(process.stdout);
    process.stdout.write = ((chunk: string | Uint8Array): boolean => {
      captured.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
      return true;
    }) as typeof process.stdout.write;

    try {
      const tagged = createLogger('log', 'node-42');
      tagged.log({ message: 'ready' }, 'Bootstrap');

      const untagged = createLogger('log');
      untagged.log({ message: 'plain' }, 'Bootstrap');
    } finally {
      process.stdout.write = origWrite;
    }

    expect(captured).toHaveLength(2);
    const first = JSON.parse(captured[0]!) as Record<string, unknown>;
    const second = JSON.parse(captured[1]!) as Record<string, unknown>;
    expect(first.instanceId).toBe('node-42');
    expect(first.pid).toBe(process.pid);
    expect(second.instanceId).toBeUndefined();
  });

  test('createLogger flattens structured fields onto the JSON log object', () => {
    const captured: string[] = [];
    const original = process.stdout.write.bind(process.stdout);
    process.stdout.write = ((chunk: string | Uint8Array): boolean => {
      captured.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
      return true;
    }) as typeof process.stdout.write;

    try {
      createLogger('log', 'node-42').warn('Wallet balance diverges from its ledger', {
        correlationId: 'corr-1',
        walletId: 'wallet-1',
        checkedEntries: 1,
      });
    } finally {
      process.stdout.write = original;
    }

    const flat = JSON.parse(captured[0]!) as Record<string, unknown>;
    expect(flat.message).toBe('Wallet balance diverges from its ledger');
    expect(flat.correlationId).toBe('corr-1');
    expect(flat.walletId).toBe('wallet-1');
    expect(flat.checkedEntries).toBe(1);
    expect(flat.instanceId).toBe('node-42');
    expect(flat.difference).toBeUndefined();
    expect(flat.storedBalance).toBeUndefined();
    expect(flat.calculatedBalance).toBeUndefined();
    expect(JSON.stringify(flat)).not.toMatch(/\d+\.\d{2}/);
    expect(JSON.stringify(flat).toLowerCase()).not.toContain('brl');
  });
});

describe('BatchVisibilityLeaseManager — SQS visibility heartbeat, FIFO wave renewal, and ownership loss', () => {
  function makeMessage(id: string, groupId: string, receiptHandle = `rh-${id}`): Message {
    return {
      MessageId: id,
      ReceiptHandle: receiptHandle,
      Attributes: { MessageGroupId: groupId },
      Body: '{}',
    };
  }

  test('deriveVisibilityHeartbeatIntervalMs scales to 1/3 of VisibilityTimeout clamped to [250ms, 10000ms]', () => {
    expect(deriveVisibilityHeartbeatIntervalMs(1)).toBe(333);
    expect(deriveVisibilityHeartbeatIntervalMs(3)).toBe(1_000);
    expect(deriveVisibilityHeartbeatIntervalMs(30)).toBe(10_000);
    expect(deriveVisibilityHeartbeatIntervalMs(90)).toBe(10_000);
  });

  test('renews in_flight and unblocked queued messages in FIFO group waves while skipping blocked groups', async () => {
    let monoNow = 1_000;
    const sentBatches: Array<Array<{ receiptHandle: string; timeout: number }>> = [];

    const messages = [
      makeMessage('m0', 'wallet-a'),
      makeMessage('m1', 'wallet-a'),
      makeMessage('m2', 'wallet-b'),
      makeMessage('m3', 'wallet-blocked'),
    ];

    const blockedGroups = new Set<string>(['wallet-blocked']);
    const manager = new BatchVisibilityLeaseManager({
      sqs: {
        send: async (cmd: unknown) => {
          expect(cmd).toBeInstanceOf(ChangeMessageVisibilityBatchCommand);
          const entries = (cmd as ChangeMessageVisibilityBatchCommand).input.Entries ?? [];
          sentBatches.push(entries.map((e) => ({ receiptHandle: e.ReceiptHandle!, timeout: e.VisibilityTimeout! })));
          return {
            Successful: entries.map((e) => ({ Id: e.Id! })),
            Failed: [],
          };
        },
      },
      queueUrl: 'http://localhost:4566/000000000000/test.fifo',
      messages,
      visibilityTimeoutSeconds: 3,
      defaultCallTimeoutMs: 2_000,
      logger: { warn: () => undefined },
      isStopping: () => false,
      isGroupBlocked: (gid) => blockedGroups.has(gid),
      clock: () => monoNow,
    });

    try {
      expect(manager.claimForExecution(0)).toBe(true);

      // Advance 2.2s (< 3.0s initial lease) and run heartbeat renewal:
      monoNow = 3_200;
      await manager.runRenewalCycle();

      // Wave 1 must contain heads [rh-m0 (wallet-a), rh-m2 (wallet-b)] — never two of wallet-a in one batch, and never wallet-blocked!
      expect(sentBatches).toHaveLength(2);
      expect(sentBatches[0]).toEqual([
        { receiptHandle: 'rh-m0', timeout: 3 },
        { receiptHandle: 'rh-m2', timeout: 3 },
      ]);
      // Wave 2 renews the second message of wallet-a after m0 succeeded:
      expect(sentBatches[1]).toEqual([{ receiptHandle: 'rh-m1', timeout: 3 }]);

      manager.markCompleted(0);

      // Advance to 5.5s (past original 4.0s expiry, but within renewed 6.2s expiry!):
      monoNow = 5_500;
      expect(manager.claimForExecution(1)).toBe(true);
      expect(manager.claimForExecution(2)).toBe(true);
    } finally {
      await manager.stop();
    }
  });

  test('revokes local ownership for entire FIFO group tail when head fails renewal with ReceiptHandleIsInvalid', async () => {
    let monoNow = 0;
    const sentReceiptHandles: string[][] = [];
    const warnings: unknown[] = [];

    const messages = [
      makeMessage('a1', 'wallet-a'),
      makeMessage('a2', 'wallet-a'),
      makeMessage('b1', 'wallet-b'),
    ];

    const manager = new BatchVisibilityLeaseManager({
      sqs: {
        send: async (cmd: unknown) => {
          const entries = (cmd as ChangeMessageVisibilityBatchCommand).input.Entries ?? [];
          sentReceiptHandles.push(entries.map((e) => e.ReceiptHandle!));
          return {
            Successful: entries.filter((e) => e.ReceiptHandle !== 'rh-a1').map((e) => ({ Id: e.Id! })),
            Failed: entries
              .filter((e) => e.ReceiptHandle === 'rh-a1')
              .map((e) => ({
                Id: e.Id!,
                Code: 'ReceiptHandleIsInvalid',
                SenderFault: true,
                Message: 'Receipt handle has expired',
              })),
          };
        },
      },
      queueUrl: 'http://localhost:4566/000000000000/test.fifo',
      messages,
      visibilityTimeoutSeconds: 3,
      defaultCallTimeoutMs: 2_000,
      logger: { warn: (msg) => warnings.push(msg) },
      isStopping: () => false,
      isGroupBlocked: () => false,
      clock: () => monoNow,
    });

    try {
      monoNow = 1_000;
      await manager.runRenewalCycle();

      // Wave 1 sent [rh-a1, rh-b1]. Because rh-a1 failed with ReceiptHandleIsInvalid, rh-a2 MUST NOT be sent in Wave 2!
      expect(sentReceiptHandles).toEqual([['rh-a1', 'rh-b1']]);
      expect(manager.lostGroupIds().has('wallet-a')).toBe(true);
      expect(manager.lostGroupIds().has('wallet-b')).toBe(false);

      // Both a1 and a2 are refused for local execution, while b1 remains owned and executable:
      expect(manager.claimForExecution(0)).toBe(false);
      expect(manager.claimForExecution(1)).toBe(false);
      expect(manager.claimForExecution(2)).toBe(true);
      expect(warnings.length).toBeGreaterThanOrEqual(1);
    } finally {
      await manager.stop();
    }
  });

  test('refuses claimForExecution when visibility timeout already elapsed before execution started', async () => {
    let monoNow = 0;
    const messages = [makeMessage('m1', 'wallet-expired')];

    const manager = new BatchVisibilityLeaseManager({
      sqs: {
        send: async () => {
          throw new Error('SQS unreachable during renewal');
        },
      },
      queueUrl: 'http://localhost:4566/000000000000/test.fifo',
      messages,
      visibilityTimeoutSeconds: 2,
      defaultCallTimeoutMs: 1_000,
      logger: { warn: () => undefined },
      isStopping: () => false,
      isGroupBlocked: () => false,
      clock: () => monoNow,
    });

    try {
      monoNow = 2_050; // > 2000ms initial visibility timeout!
      expect(manager.claimForExecution(0)).toBe(false);
      expect(manager.lostGroupIds().has('wallet-expired')).toBe(true);
    } finally {
      await manager.stop();
    }
  });

  test('cooperates with ShutdownService: withholds renewal of queued messages during shutdown and respects 0ms budget', async () => {
    let monoNow = 1_000;
    const config = AppConfig.load({
      ...BASE_ENV,
      SHUTDOWN_TIMEOUT_MS: '2000', // 1800ms cooperative budget
    });
    const shutdown = new ShutdownService(config);
    shutdown.setClockForTesting(() => monoNow);

    const renewedHandles: string[][] = [];
    const messages = [makeMessage('inflight-0', 'wallet-1'), makeMessage('queued-1', 'wallet-2')];

    const manager = new BatchVisibilityLeaseManager({
      sqs: {
        send: async (cmd: unknown) => {
          // Verify critical sqs operation is actively tracked in ShutdownService during send:
          expect(shutdown.activeCriticalOperationCount('sqs')).toBe(1);
          const entries = (cmd as ChangeMessageVisibilityBatchCommand).input.Entries ?? [];
          renewedHandles.push(entries.map((e) => e.ReceiptHandle!));
          return {
            Successful: entries.map((e) => ({ Id: e.Id! })),
            Failed: [],
          };
        },
      },
      queueUrl: 'http://localhost:4566/000000000000/test.fifo',
      messages,
      visibilityTimeoutSeconds: 3,
      defaultCallTimeoutMs: 2_000,
      logger: { warn: () => undefined },
      shutdown,
      isStopping: () => false,
      isGroupBlocked: () => false,
      clock: () => monoNow,
    });

    try {
      expect(manager.claimForExecution(0)).toBe(true);
      shutdown.beginShutdown('SIGTERM');

      // 500ms into shutdown (1300ms cooperative budget left): ONLY inflight-0 is renewed; queued-1 is NOT renewed!
      monoNow = 1_500;
      await manager.runRenewalCycle();
      expect(renewedHandles).toEqual([['rh-inflight-0']]);
      expect(shutdown.activeCriticalOperationCount('sqs')).toBe(0);

      // Advance past cooperative deadline (1000 + 1800 = 2800ms -> 0ms cooperative budget remaining):
      monoNow = 2_900;
      await manager.runRenewalCycle();
      // Zero additional SQS calls made once cooperative budget is exhausted!
      expect(renewedHandles).toEqual([['rh-inflight-0']]);
    } finally {
      await manager.stop();
    }
  });

  test('does not falsely revoke group tail when head message finishes and deletes concurrently during Wave 1 renewal', async () => {
    let monoNow = 1_000;
    const sentBatches: string[][] = [];
    const messages = [makeMessage('a0', 'wallet-a'), makeMessage('a1', 'wallet-a')];

    let manager!: BatchVisibilityLeaseManager;
    manager = new BatchVisibilityLeaseManager({
      sqs: {
        send: async (cmd: unknown) => {
          const entries = (cmd as ChangeMessageVisibilityBatchCommand).input.Entries ?? [];
          const handles = entries.map((e) => e.ReceiptHandle!);
          sentBatches.push(handles);

          if (handles.includes('rh-a0')) {
            // Simulate consumer committing and deleting a0 concurrently while Wave 1 is in flight:
            manager.markCompleted(0);
            return {
              Successful: [],
              Failed: [
                {
                  Id: '0',
                  Code: 'ReceiptHandleIsInvalid',
                  SenderFault: true,
                  Message: 'Message already deleted',
                },
              ],
            };
          }
          return {
            Successful: entries.map((e) => ({ Id: e.Id! })),
            Failed: [],
          };
        },
      },
      queueUrl: 'http://localhost:4566/000000000000/test.fifo',
      messages,
      visibilityTimeoutSeconds: 3,
      defaultCallTimeoutMs: 2_000,
      logger: { warn: () => undefined },
      isStopping: () => false,
      isGroupBlocked: () => false,
      clock: () => monoNow,
    });

    try {
      expect(manager.claimForExecution(0)).toBe(true);
      monoNow = 2_000;
      await manager.runRenewalCycle();

      // Wave 1 sent rh-a0 (which completed concurrently), and Wave 2 STILL renewed rh-a1!
      expect(sentBatches).toEqual([['rh-a0'], ['rh-a1']]);
      expect(manager.lostGroupIds().has('wallet-a')).toBe(false);
      monoNow = 4_500;
      expect(manager.claimForExecution(1)).toBe(true);
    } finally {
      await manager.stop();
    }
  });

  test('never resurrects an already-expired queued lease on late renewal tick and fences group tail when in-flight head expires before completing', async () => {
    let monoNow = 1_000;
    const sentBatches: string[][] = [];
    const messages = [
      makeMessage('a0', 'wallet-a'),
      makeMessage('a1', 'wallet-a'),
      makeMessage('b0', 'wallet-b'),
    ];

    const manager = new BatchVisibilityLeaseManager({
      sqs: {
        send: async (cmd: unknown) => {
          const entries = (cmd as ChangeMessageVisibilityBatchCommand).input.Entries ?? [];
          sentBatches.push(entries.map((e) => e.ReceiptHandle!));
          return {
            Successful: entries.map((e) => ({ Id: e.Id! })),
            Failed: [],
          };
        },
      },
      queueUrl: 'http://localhost:4566/000000000000/test.fifo',
      messages,
      visibilityTimeoutSeconds: 2, // initial lease expires at monoNow = 3_000
      defaultCallTimeoutMs: 1_000,
      logger: { warn: () => undefined },
      isStopping: () => false,
      isGroupBlocked: () => false,
      clock: () => monoNow,
    });

    try {
      expect(manager.claimForExecution(0)).toBe(true);

      // Simulate a0 finishing at monoNow = 3_050 AFTER its 2s lease already elapsed at 3_000:
      monoNow = 3_050;
      manager.markCompleted(0);
      expect(manager.lostGroupIds().has('wallet-a')).toBe(true);

      // A late heartbeat tick at monoNow = 3_100 must NOT call ChangeMessageVisibilityBatch on expired a1 or b0:
      monoNow = 3_100;
      await manager.runRenewalCycle();
      expect(sentBatches).toEqual([]);
      expect(manager.lostGroupIds().has('wallet-b')).toBe(true);

      // Both a1 and b0 remain strictly fenced against stale local execution:
      expect(manager.claimForExecution(1)).toBe(false);
      expect(manager.claimForExecution(2)).toBe(false);
    } finally {
      await manager.stop();
    }
  });

  test('anchors initial lease expiry to receivedAtMono so post-receive metadata latency cannot mask an already-expired SQS lease', async () => {
    const receivedAtMono = 1_000;
    let monoNow = 3_100; // e.g., GetQueueAttributes took 2.1s while VisibilityTimeout is 2s!
    const warnings: Array<Record<string, unknown>> = [];

    const manager = new BatchVisibilityLeaseManager({
      sqs: {
        send: async () => ({ Successful: [], Failed: [] }),
      },
      queueUrl: 'http://localhost:4566/000000000000/test.fifo',
      messages: [makeMessage('cold-0', 'wallet-cold')],
      visibilityTimeoutSeconds: 2,
      receivedAtMono,
      defaultCallTimeoutMs: 1_000,
      logger: {
        warn: (obj: unknown) => {
          warnings.push(obj as Record<string, unknown>);
        },
      },
      isStopping: () => false,
      isGroupBlocked: () => false,
      clock: () => monoNow,
    });

    try {
      expect(manager.claimForExecution(0)).toBe(false);
      expect(manager.lostGroupIds().has('wallet-cold')).toBe(true);
      expect(warnings.length).toBe(1);
    } finally {
      await manager.stop();
    }
  });
});
