import {
  CreateQueueCommand,
  DeleteMessageBatchCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  type Message,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import type { SQL } from 'bun';
import { AppConfig } from '../../src/common/config/env';
import { newId } from '../../src/common/ids';
import { INSTANCE_ID_HEADER, toPostgresApplicationName } from '../../src/common/instance/instance-identity';
import { testDatabaseUrl } from './database';

export interface IsolatedClusterQueues {
  readonly txQueueName: string;
  readonly txQueueUrl: string;
  readonly dlqName: string;
  readonly dlqUrl: string;
  readonly eventsQueueName: string;
  readonly eventsQueueUrl: string;
  readonly visibilityTimeoutSeconds: number;
  readonly destroy: () => Promise<void>;
}

export interface CreateClusterQueuesOptions {
  readonly visibilityTimeoutSeconds?: number;
  readonly maxReceiveCount?: number;
}

export async function createClusterQueues(
  sqs: SQSClient,
  options: CreateClusterQueuesOptions = {},
): Promise<IsolatedClusterQueues> {
  const suffix = newId().slice(0, 8);
  const visibilityTimeoutSeconds = options.visibilityTimeoutSeconds ?? 30;
  const maxReceiveCount = options.maxReceiveCount ?? 5;

  const dlqName = `mp-wager-dlq-${suffix}.fifo`;
  const txQueueName = `mp-wager-tx-${suffix}.fifo`;
  const eventsQueueName = `mp-wager-events-${suffix}.fifo`;

  const createdDlq = await sqs.send(
    new CreateQueueCommand({
      QueueName: dlqName,
      Attributes: {
        FifoQueue: 'true',
        ContentBasedDeduplication: 'false',
        VisibilityTimeout: String(visibilityTimeoutSeconds),
      },
    }),
  );
  if (!createdDlq.QueueUrl) {
    throw new Error(`Missing QueueUrl for ${dlqName}`);
  }

  // LocalStack deterministic FIFO DLQ ARN pattern for us-east-1/configured region:
  const awsRegion = process.env.AWS_REGION ?? 'us-east-1';
  const dlqArn = `arn:aws:sqs:${awsRegion}:000000000000:${dlqName}`;

  const createdTx = await sqs.send(
    new CreateQueueCommand({
      QueueName: txQueueName,
      Attributes: {
        FifoQueue: 'true',
        ContentBasedDeduplication: 'false',
        VisibilityTimeout: String(visibilityTimeoutSeconds),
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: dlqArn,
          maxReceiveCount: String(maxReceiveCount),
        }),
      },
    }),
  );
  if (!createdTx.QueueUrl) {
    throw new Error(`Missing QueueUrl for ${txQueueName}`);
  }

  const createdEvents = await sqs.send(
    new CreateQueueCommand({
      QueueName: eventsQueueName,
      Attributes: {
        FifoQueue: 'true',
        ContentBasedDeduplication: 'false',
        VisibilityTimeout: String(visibilityTimeoutSeconds),
      },
    }),
  );
  if (!createdEvents.QueueUrl) {
    throw new Error(`Missing QueueUrl for ${eventsQueueName}`);
  }

  return {
    txQueueName,
    txQueueUrl: createdTx.QueueUrl,
    dlqName,
    dlqUrl: createdDlq.QueueUrl,
    eventsQueueName,
    eventsQueueUrl: createdEvents.QueueUrl,
    visibilityTimeoutSeconds,
    destroy: async () => {
      await Promise.all([
        sqs.send(new DeleteQueueCommand({ QueueUrl: createdTx.QueueUrl })).catch(() => undefined),
        sqs.send(new DeleteQueueCommand({ QueueUrl: createdDlq.QueueUrl })).catch(() => undefined),
        sqs.send(new DeleteQueueCommand({ QueueUrl: createdEvents.QueueUrl })).catch(() => undefined),
      ]);
    },
  };
}

export function createTestSqsClient(): SQSClient {
  const config = AppConfig.load({
    ...process.env,
    DATABASE_URL: testDatabaseUrl('app'),
  });
  return new SQSClient({
    region: config.awsRegion,
    ...(config.awsEndpointUrl !== undefined ? { endpoint: config.awsEndpointUrl } : {}),
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
}

export interface SpawnInstanceOptions {
  readonly instanceId?: string;
  readonly port?: number;
  readonly queues: IsolatedClusterQueues;
  readonly workersEnabled?: boolean;
  readonly sqsWaitTimeSeconds?: number;
  readonly outboxPollIntervalMs?: number;
  readonly pendingReferencePollIntervalMs?: number;
  readonly shutdownTimeoutMs?: number;
  readonly databasePoolMax?: number;
  readonly envOverrides?: Readonly<Record<string, string>>;
  readonly startupTimeoutMs?: number;
}

export interface AbruptKillEvidence {
  readonly instanceId: string;
  readonly pid: number;
  readonly pgApplicationName: string;
  readonly mechanism: 'SIGKILL' | 'TerminateProcess';
  readonly exitCode: number | null;
  readonly signalCode: string | number | null;
  readonly killedAtMonoMs: number;
  readonly terminatedDurationMs: number;
  readonly hadGracefulShutdownLogs: boolean;
  readonly osProcessAliveAfterKill: boolean;
}

export function isOsProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException | undefined)?.code;
    if (code === 'EPERM') {
      return true;
    }
    return false;
  }
}

export interface GracefulShutdownEvidence {
  readonly instanceId: string;
  readonly pid: number;
  readonly pgApplicationName: string;
  readonly signal: 'SIGTERM' | 'SIGINT';
  readonly signalSentAtIso: string;
  readonly processExitedAtIso: string;
  readonly exitCode: number;
  readonly osProcessAliveAfterExit: boolean;
  readonly startedAt: string | null;
  readonly completedAt: string | null;
  readonly inFlightHttpAtStart: number;
  readonly drainedHttpRequests: number;
  readonly abortedHttpRequests: number;
  readonly rejectedHttpRequests: number;
  readonly inFlightOperationsAtStart: number;
  readonly drainedCriticalOperations: number;
  readonly abortedCriticalOperations: number;
  readonly inFlightMessagesAtSignal: number;
  readonly sqsVisibilityReleased: number;
  readonly sqsWithheldBehindPredecessor: number;
  readonly leaseStates: Readonly<Record<string, number>>;
  readonly elapsedMs: number;
}

export interface RunningServiceInstance {
  readonly instanceId: string;
  readonly pid: number;
  readonly port: number;
  readonly baseUrl: string;
  readonly pgApplicationName: string;
  readonly logs: () => readonly Record<string, unknown>[];
  readonly rawOutput: () => { readonly stdout: string; readonly stderr: string };
  readonly isExited: () => boolean;
  readonly exitCode: () => number | null;
  readonly signalCode: () => string | number | null;
  readonly request: (path: string, init?: RequestInit) => Promise<Response>;
  readonly sendSignal: (signal?: 'SIGTERM' | 'SIGINT') => void;
  readonly stopGracefully: (signal?: 'SIGTERM' | 'SIGINT', timeoutMs?: number) => Promise<number>;
  readonly stopGracefullyWithEvidence: (
    signal?: 'SIGTERM' | 'SIGINT',
    timeoutMs?: number,
  ) => Promise<GracefulShutdownEvidence>;
  readonly killAbruptly: (timeoutMs?: number) => Promise<AbruptKillEvidence>;
  readonly forceKill: () => Promise<void>;
}

/**
 * Assertions guaranteeing subprocess environment variables NEVER target any non-`_test` database.
 */
function buildSafeSubprocessEnv(options: SpawnInstanceOptions, instanceId: string): Record<string, string> {
  const safeAppDbUrl = testDatabaseUrl('app');
  const parsedSafeUrl = new URL(safeAppDbUrl);
  const dbName = decodeURIComponent(parsedSafeUrl.pathname.slice(1));
  if (!dbName.endsWith('_test')) {
    throw new Error(`Refusing to spawn subprocess against non-test database "${dbName}"`);
  }

  if (options.envOverrides?.DATABASE_URL !== undefined) {
    const overrideDb = decodeURIComponent(new URL(options.envOverrides.DATABASE_URL).pathname.slice(1));
    if (overrideDb !== dbName) {
      throw new Error(`Refusing DATABASE_URL override pointing to "${overrideDb}" (expected "${dbName}")`);
    }
  }

  const base: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && key !== 'DATABASE_ADMIN_URL' && key !== 'MIGRATION_DATABASE_URL') {
      base[key] = value;
    }
  }

  return {
    ...base,
    NODE_ENV: 'production',
    PORT: String(options.port ?? 0),
    INSTANCE_ID: instanceId,
    LOG_LEVEL: 'log',
    DATABASE_URL: safeAppDbUrl,
    DATABASE_POOL_MAX: String(options.databasePoolMax ?? 5),
    WORKERS_ENABLED: String(options.workersEnabled ?? true),
    SQS_WAGER_TRANSACTIONS_QUEUE: options.queues.txQueueName,
    SQS_WAGER_TRANSACTIONS_DLQ: options.queues.dlqName,
    SQS_INTEGRATION_EVENTS_QUEUE: options.queues.eventsQueueName,
    SQS_WAIT_TIME_SECONDS: String(options.sqsWaitTimeSeconds ?? 1),
    OUTBOX_POLL_INTERVAL_MS: String(options.outboxPollIntervalMs ?? 80),
    PENDING_REFERENCE_POLL_INTERVAL_MS: String(options.pendingReferencePollIntervalMs ?? 80),
    SHUTDOWN_TIMEOUT_MS: String(options.shutdownTimeoutMs ?? 6_000),
    ...options.envOverrides,
  };
}

export async function spawnServiceInstance(options: SpawnInstanceOptions): Promise<RunningServiceInstance> {
  const instanceId = options.instanceId ?? `inst-${newId().slice(0, 8)}`;
  const startupTimeoutMs = options.startupTimeoutMs ?? 12_000;
  const env = buildSafeSubprocessEnv(options, instanceId);

  const command =
    process.platform === 'win32'
      ? ['bun', 'run', '--preload', './test/support/win32-signal-bridge.ts', 'src/main.ts']
      : ['bun', 'run', 'src/main.ts'];

  const child = Bun.spawn(command, {
    cwd: process.cwd(),
    env,
    stdin: 'pipe',
    stdout: 'pipe',
    stderr: 'pipe',
  });

  const structuredLogs: Record<string, unknown>[] = [];
  const stdoutChunks: string[] = [];
  const stderrChunks: string[] = [];
  let boundPort: number | undefined;
  let exited = false;
  let recordedExitCode: number | null = null;

  const exitPromise = child.exited.then((code) => {
    exited = true;
    recordedExitCode = code;
    return code;
  });

  let resolvePortReady!: (port: number) => void;
  let rejectPortReady!: (error: Error) => void;
  const portReadyPromise = new Promise<number>((resolve, reject) => {
    resolvePortReady = resolve;
    rejectPortReady = reject;
  });

  const consumeStream = async (
    stream: ReadableStream<Uint8Array>,
    targetChunks: string[],
    parseJsonLines: boolean,
  ): Promise<void> => {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let lineBuffer = '';
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        const text = decoder.decode(value, { stream: true });
        targetChunks.push(text);
        if (!parseJsonLines) {
          continue;
        }
        lineBuffer += text;
        let idx = lineBuffer.indexOf('\n');
        while (idx !== -1) {
          const rawLine = lineBuffer.slice(0, idx).trim();
          lineBuffer = lineBuffer.slice(idx + 1);
          if (rawLine.startsWith('{') && rawLine.endsWith('}')) {
            try {
              const parsed = JSON.parse(rawLine) as Record<string, unknown>;
              structuredLogs.push(parsed);
              const msgObj = parsed.message as Record<string, unknown> | string | undefined;
              if (
                boundPort === undefined &&
                typeof msgObj === 'object' &&
                msgObj !== null &&
                msgObj.message === 'HTTP server listening' &&
                typeof msgObj.port === 'number'
              ) {
                boundPort = msgObj.port;
                resolvePortReady(boundPort);
              }
            } catch {
              // Ignore non-JSON stdout fragments.
            }
          }
          idx = lineBuffer.indexOf('\n');
        }
      }
    } finally {
      reader.releaseLock();
    }
  };

  const stdoutPump = consumeStream(child.stdout, stdoutChunks, true);
  const stderrPump = consumeStream(child.stderr, stderrChunks, false);

  void exitPromise.then((code) => {
    if (boundPort === undefined) {
      rejectPortReady(
        new Error(
          `Instance ${instanceId} (pid=${child.pid}) exited prematurely with code ${code}.\n` +
            `STDOUT:\n${stdoutChunks.join('')}\nSTDERR:\n${stderrChunks.join('')}`,
        ),
      );
    }
  });

  const cleanupFailedSpawn = async (): Promise<void> => {
    if (!exited) {
      try {
        child.kill();
      } catch {
        // Ignore kill errors.
      }
    }
    try {
      child.stdin.end();
    } catch {
      // Ignore broken pipe.
    }
    await exitPromise.catch(() => undefined);
    await Promise.all([stdoutPump.catch(() => undefined), stderrPump.catch(() => undefined)]);
  };

  let startupTimer: ReturnType<typeof setTimeout> | undefined;
  let port: number;
  try {
    port = await Promise.race([
      portReadyPromise,
      new Promise<never>((_resolve, reject) => {
        startupTimer = setTimeout(() => {
          reject(
            new Error(
              `Timed out (${startupTimeoutMs}ms) waiting for instance ${instanceId} to bind HTTP port.\n` +
                `STDOUT:\n${stdoutChunks.join('')}\nSTDERR:\n${stderrChunks.join('')}`,
            ),
          );
        }, startupTimeoutMs);
      }),
    ]).finally(() => {
      clearTimeout(startupTimer);
    });
  } catch (error) {
    await cleanupFailedSpawn();
    throw error;
  }

  const baseUrl = `http://127.0.0.1:${port}`;

  // Confirm HTTP readiness and X-Instance-Id attribution before returning handle:
  const readyDeadline = Date.now() + startupTimeoutMs;
  let isReady = false;
  while (Date.now() < readyDeadline && !exited) {
    try {
      const res = await fetch(`${baseUrl}/health/ready`, {
        headers: { Connection: 'close' },
      });
      if (res.status === 200) {
        const headerId = res.headers.get(INSTANCE_ID_HEADER);
        if (headerId !== instanceId) {
          throw new Error(`Expected ${INSTANCE_ID_HEADER}="${instanceId}", got "${headerId}"`);
        }
        isReady = true;
        break;
      }
    } catch (error) {
      if (error instanceof Error && error.message.startsWith(`Expected ${INSTANCE_ID_HEADER}`)) {
        await cleanupFailedSpawn();
        throw error;
      }
    }
    await Bun.sleep(40);
  }

  if (!isReady) {
    await cleanupFailedSpawn();
    throw new Error(
      `Instance ${instanceId} bound port ${port} but never passed /health/ready within ${startupTimeoutMs}ms.\n` +
        `STDOUT:\n${stdoutChunks.join('')}\nSTDERR:\n${stderrChunks.join('')}`,
    );
  }

  let signalSentAtIso: string | undefined;
  let lastSignalSent: 'SIGTERM' | 'SIGINT' | undefined;

  const sendGracefulSignal = (signal: 'SIGTERM' | 'SIGINT' = 'SIGTERM'): void => {
    if (exited) {
      return;
    }
    if (signalSentAtIso === undefined) {
      signalSentAtIso = new Date().toISOString();
      lastSignalSent = signal;
    }
    if (process.platform === 'win32') {
      child.stdin.write(`SIGNAL:${signal}\n`);
      child.stdin.flush();
    } else {
      child.kill(signal);
    }
  };

  const stopGracefully = async (signal: 'SIGTERM' | 'SIGINT' = 'SIGTERM', timeoutMs = 10_000): Promise<number> => {
    if (exited && recordedExitCode !== null) {
      await Promise.all([stdoutPump, stderrPump]);
      return recordedExitCode;
    }
    if (lastSignalSent === undefined) {
      sendGracefulSignal(signal);
    }

    let killFallbackTimer: ReturnType<typeof setTimeout> | undefined;
    try {
      const code = await Promise.race([
        exitPromise,
        new Promise<number>((_resolve, reject) => {
          killFallbackTimer = setTimeout(() => {
            try {
              child.kill();
            } catch {
              // Ignore kill errors.
            }
            reject(
              new Error(
                `Instance ${instanceId} (pid=${child.pid}) did not exit within ${timeoutMs}ms after ${signal}.\n` +
                  `STDOUT:\n${stdoutChunks.join('')}\nSTDERR:\n${stderrChunks.join('')}`,
              ),
            );
          }, timeoutMs);
        }),
      ]);
      try {
        child.stdin.end();
      } catch {
        // Ignore broken pipe on closed stdin.
      }
      await Promise.all([stdoutPump, stderrPump]);
      return code;
    } finally {
      clearTimeout(killFallbackTimer);
    }
  };

  const pgApplicationName = toPostgresApplicationName(instanceId);

  const extractGracefulShutdownEvidence = (
    effectiveSignal: 'SIGTERM' | 'SIGINT',
    sentAtIso: string,
    exitedAtIso: string,
    code: number,
  ): GracefulShutdownEvidence => {
    let summary: Record<string, unknown> | undefined;
    let inFlightMessagesAtSignal = 0;
    let sqsVisibilityReleased = 0;
    let sqsWithheldBehindPredecessor = 0;
    const leaseStates: Record<string, number> = {
      queued: 0,
      in_flight: 0,
      completed: 0,
      released: 0,
      lost: 0,
    };

    for (const entry of structuredLogs) {
      const payload =
        typeof entry.message === 'object' && entry.message !== null
          ? (entry.message as Record<string, unknown>)
          : entry;
      const msgText = typeof payload.message === 'string' ? payload.message : entry.message;

      if (
        (msgText === 'Graceful shutdown completed' ||
          msgText === 'Shutdown completed with timeout or teardown failure' ||
          msgText === 'Graceful shutdown timed out; forcing exit') &&
        typeof payload.shutdownSummary === 'object' &&
        payload.shutdownSummary !== null
      ) {
        summary = payload.shutdownSummary as Record<string, unknown>;
      }

      if (msgText === 'SQS consumer shutdown drain summary') {
        inFlightMessagesAtSignal += Number(payload.inFlightMessagesAtSignal ?? 0);
        sqsVisibilityReleased += Number(payload.visibilityReleased ?? 0);
        sqsWithheldBehindPredecessor += Number(payload.withheldBehindPredecessor ?? 0);
        if (typeof payload.leaseStates === 'object' && payload.leaseStates !== null) {
          for (const [k, v] of Object.entries(payload.leaseStates as Record<string, unknown>)) {
            leaseStates[k] = (leaseStates[k] ?? 0) + Number(v ?? 0);
          }
        }
      }
    }

    return {
      instanceId,
      pid: child.pid,
      pgApplicationName,
      signal: effectiveSignal,
      signalSentAtIso: sentAtIso,
      processExitedAtIso: exitedAtIso,
      exitCode: code,
      osProcessAliveAfterExit: isOsProcessAlive(child.pid),
      startedAt: typeof summary?.startedAt === 'string' ? summary.startedAt : sentAtIso,
      completedAt: typeof summary?.completedAt === 'string' ? summary.completedAt : exitedAtIso,
      inFlightHttpAtStart: Number(summary?.inFlightHttpAtStart ?? 0),
      drainedHttpRequests: Number(summary?.drainedHttpRequests ?? 0),
      abortedHttpRequests: Number(summary?.abortedHttpRequests ?? 0),
      rejectedHttpRequests: Number(summary?.rejectedHttpRequests ?? 0),
      inFlightOperationsAtStart: Number(summary?.inFlightOperationsAtStart ?? 0),
      drainedCriticalOperations: Number(summary?.drainedCriticalOperations ?? 0),
      abortedCriticalOperations: Number(summary?.abortedCriticalOperations ?? 0),
      inFlightMessagesAtSignal,
      sqsVisibilityReleased,
      sqsWithheldBehindPredecessor,
      leaseStates,
      elapsedMs: Number(summary?.elapsedMs ?? 0),
    };
  };

  const stopGracefullyWithEvidence = async (
    signal: 'SIGTERM' | 'SIGINT' = 'SIGTERM',
    timeoutMs = 10_000,
  ): Promise<GracefulShutdownEvidence> => {
    const effectiveSignal = lastSignalSent ?? signal;
    const sentAt = signalSentAtIso ?? new Date().toISOString();
    const code = await stopGracefully(effectiveSignal, timeoutMs);
    const pidGoneDeadline = Date.now() + 1_000;
    while (isOsProcessAlive(child.pid) && Date.now() < pidGoneDeadline) {
      await Bun.sleep(10);
    }
    const exitedAt = new Date().toISOString();
    return extractGracefulShutdownEvidence(effectiveSignal, sentAt, exitedAt, code);
  };

  const hasAnyGracefulShutdownLog = (): boolean => {
    const shutdownMarkers = new Set([
      'Shutdown signal received',
      'Graceful shutdown completed',
      'Shutdown completed with timeout or teardown failure',
      'Graceful shutdown timed out; forcing exit',
      'Withholding SQS visibility release during shutdown to preserve FIFO order behind in-flight predecessor',
    ]);
    for (const entry of structuredLogs) {
      const msg = entry.message as Record<string, unknown> | string | undefined;
      if (typeof msg === 'string' && shutdownMarkers.has(msg)) {
        return true;
      }
      if (typeof msg === 'object' && msg !== null && typeof msg.message === 'string' && shutdownMarkers.has(msg.message)) {
        return true;
      }
    }
    const rawStdout = stdoutChunks.join('');
    for (const marker of shutdownMarkers) {
      if (rawStdout.includes(marker)) {
        return true;
      }
    }
    return false;
  };

  const killAbruptly = async (timeoutMs = 5_000): Promise<AbruptKillEvidence> => {
    const mechanism: 'SIGKILL' | 'TerminateProcess' =
      process.platform === 'win32' ? 'TerminateProcess' : 'SIGKILL';
    const killedAtMonoMs = performance.now();

    if (!exited) {
      // Intentionally NEVER write to child.stdin (no win32 signal bridge SIGTERM/SIGINT):
      // invokes direct OS kernel process termination (`TerminateProcess` on win32, `SIGKILL` on POSIX).
      if (process.platform === 'win32') {
        child.kill();
      } else {
        child.kill('SIGKILL');
      }
    }

    let killTimer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        exitPromise,
        new Promise<never>((_resolve, reject) => {
          killTimer = setTimeout(() => {
            reject(
              new Error(
                `Instance ${instanceId} (pid=${child.pid}) did not terminate within ${timeoutMs}ms after ${mechanism}.`,
              ),
            );
          }, timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(killTimer);
    }

    try {
      child.stdin.end();
    } catch {
      // Ignore broken pipe on abruptly terminated process.
    }
    await Promise.all([stdoutPump.catch(() => undefined), stderrPump.catch(() => undefined)]);

    // On Windows kernel handle teardown, wait briefly if needed for PID table cleanup (deterministic bounded poll):
    const pidGoneDeadline = Date.now() + 1_000;
    while (isOsProcessAlive(child.pid) && Date.now() < pidGoneDeadline) {
      await Bun.sleep(10);
    }

    const terminatedDurationMs = Math.max(0, Math.round(performance.now() - killedAtMonoMs));
    return {
      instanceId,
      pid: child.pid,
      pgApplicationName,
      mechanism,
      exitCode: recordedExitCode,
      signalCode: child.signalCode ?? null,
      killedAtMonoMs,
      terminatedDurationMs,
      hadGracefulShutdownLogs: hasAnyGracefulShutdownLog(),
      osProcessAliveAfterKill: isOsProcessAlive(child.pid),
    };
  };

  const forceKill = async (): Promise<void> => {
    if (!exited) {
      try {
        if (process.platform === 'win32') {
          child.kill();
        } else {
          child.kill('SIGKILL');
        }
      } catch {
        // Ignore kill errors.
      }
    }
    try {
      child.stdin.end();
    } catch {
      // Ignore broken pipe.
    }
    await exitPromise.catch(() => undefined);
    await Promise.all([stdoutPump.catch(() => undefined), stderrPump.catch(() => undefined)]);
  };

  return {
    instanceId,
    pid: child.pid,
    port,
    baseUrl,
    pgApplicationName,
    logs: () => structuredLogs,
    rawOutput: () => ({ stdout: stdoutChunks.join(''), stderr: stderrChunks.join('') }),
    isExited: () => exited,
    exitCode: () => recordedExitCode,
    signalCode: () => child.signalCode ?? null,
    request: (path: string, init?: RequestInit) => fetch(`${baseUrl}${path}`, init),
    sendSignal: sendGracefulSignal,
    stopGracefully,
    stopGracefullyWithEvidence,
    killAbruptly,
    forceKill,
  };
}

export class MultiProcessCluster {
  private readonly instances: RunningServiceInstance[] = [];

  async spawn(options: SpawnInstanceOptions): Promise<RunningServiceInstance> {
    const instance = await spawnServiceInstance(options);
    this.instances.push(instance);
    return instance;
  }

  activeInstances(): readonly RunningServiceInstance[] {
    return this.instances.filter((inst) => !inst.isExited());
  }

  async stopAllGracefully(timeoutMs = 10_000): Promise<void> {
    await this.stopAllGracefullyWithEvidence(timeoutMs);
  }

  async stopAllGracefullyWithEvidence(timeoutMs = 10_000): Promise<GracefulShutdownEvidence[]> {
    const errors: unknown[] = [];
    const evidences: GracefulShutdownEvidence[] = [];
    await Promise.all(
      this.instances.map(async (inst) => {
        try {
          const ev = await inst.stopGracefullyWithEvidence('SIGTERM', timeoutMs);
          evidences.push(ev);
        } catch (error) {
          errors.push(error);
          await inst.forceKill();
        }
      }),
    );
    this.instances.length = 0;
    if (errors.length > 0) {
      throw errors[0];
    }
    return evidences;
  }

  async forceKillAll(): Promise<void> {
    await Promise.all(this.instances.map((inst) => inst.forceKill()));
    this.instances.length = 0;
  }
}

export interface PollUntilOptions {
  readonly timeoutMs?: number;
  readonly intervalMs?: number;
  readonly description?: string;
}

/**
 * Deterministic bounded polling helper that resolves as soon as `probe()` returns a non-undefined
 * value, avoiding arbitrary fixed `sleep` delays in multi-process tests.
 */
export async function pollUntil<T>(
  probe: () => Promise<T | undefined>,
  options: PollUntilOptions = {},
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? 10_000;
  const intervalMs = options.intervalMs ?? 20;
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const value = await probe();
      if (value !== undefined) {
        return value;
      }
      lastError = undefined;
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(intervalMs);
  }

  const label = options.description ?? 'condition';
  const suffix =
    lastError !== undefined ? ` (last error: ${lastError instanceof Error ? lastError.message : String(lastError)})` : '';
  throw new Error(`Timed out after ${timeoutMs}ms waiting for ${label}${suffix}`);
}

export interface LockWaiterSnapshot {
  readonly pid: number;
  readonly applicationName: string;
}

export interface WalletLockBarrierEvidence<T> {
  readonly result: T;
  readonly gatePid: number;
  readonly waiters: readonly LockWaiterSnapshot[];
  readonly distinctApplicationNames: readonly string[];
}

export interface WalletLockBarrierOptions {
  readonly walletId: string;
  readonly expectedWaiters: number;
  readonly expectedDistinctInstances?: number;
  readonly timeoutMs?: number;
}

/**
 * Deterministic multi-process PostgreSQL row-lock barrier:
 * 1. Acquires `SELECT id FROM wallets WHERE id = $walletId FOR UPDATE` on `gateSql`.
 * 2. Launches `launchContenders()` across multiple OS processes.
 * 3. Polls `pg_stat_activity` + `pg_blocking_pids()` transitively until `expectedWaiters` backends
 *    (across `expectedDistinctInstances` distinct `application_name`s) are simultaneously queued on
 *    that exact wallet row lock inside PostgreSQL.
 * 4. Releases the gate so all contenders race inside PostgreSQL simultaneously, returning both the
 *    contenders' result and concrete simultaneous-concurrency evidence.
 */
export async function withMultiProcessWalletLockBarrier<T>(
  gateSql: SQL,
  adminSql: SQL,
  options: WalletLockBarrierOptions,
  launchContenders: () => Promise<T>,
): Promise<WalletLockBarrierEvidence<T>> {
  const timeoutMs = options.timeoutMs ?? 10_000;
  const minInstances = options.expectedDistinctInstances ?? 1;

  let resolveGateLocked!: (pid: number) => void;
  let rejectGateLocked!: (err: unknown) => void;
  const gateLocked = new Promise<number>((resolve, reject) => {
    resolveGateLocked = resolve;
    rejectGateLocked = reject;
  });

  let releaseGate!: () => void;
  const gateHold = new Promise<void>((resolve) => {
    releaseGate = resolve;
  });

  const gateTx = gateSql
    .begin(async (tx) => {
      const [pidRow] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wallets where id = ${options.walletId} for update`;
      resolveGateLocked(pidRow!.pid);
      await gateHold;
    })
    .catch((err) => {
      rejectGateLocked(err);
      throw err;
    });

  const gatePid = await gateLocked;
  let contendersPromise: Promise<T> | undefined;
  let capturedWaiters: LockWaiterSnapshot[] = [];

  try {
    contendersPromise = launchContenders();
    capturedWaiters = await pollUntil(
      async () => {
        const rows = await adminSql<Array<{ pid: number; application_name: string }>>`
          with recursive lock_tree as (
            select pid::int as pid, application_name
            from pg_stat_activity
            where datname = current_database()
              and pid <> ${gatePid}
              and ${gatePid} = any(pg_blocking_pids(pid))
            union
            select a.pid::int as pid, a.application_name
            from pg_stat_activity a
            join lock_tree lt on lt.pid = any(pg_blocking_pids(a.pid))
            where a.datname = current_database()
              and a.pid <> ${gatePid}
          )
          select pid, application_name
          from lock_tree
          order by pid asc
        `;
        const distinctApps = new Set(rows.map((r) => r.application_name).filter((n) => n.length > 0));
        if (rows.length >= options.expectedWaiters && distinctApps.size >= minInstances) {
          return rows.map((r) => ({ pid: r.pid, applicationName: r.application_name }));
        }
        return undefined;
      },
      {
        timeoutMs,
        intervalMs: 10,
        description: `${options.expectedWaiters} lock waiters across >= ${minInstances} instances on wallet ${options.walletId}`,
      },
    );
  } finally {
    releaseGate();
    await gateTx;
  }

  const result = await contendersPromise!;
  const distinctApplicationNames = [
    ...new Set(capturedWaiters.map((w) => w.applicationName).filter((n) => n.length > 0)),
  ].sort();

  return {
    result,
    gatePid,
    waiters: capturedWaiters,
    distinctApplicationNames,
  };
}

export interface QueueDepthSnapshot {
  readonly visible: number;
  readonly notVisible: number;
  readonly delayed: number;
}

export async function getQueueDepth(sqs: SQSClient, queueUrl: string): Promise<QueueDepthSnapshot> {
  const res = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: [
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesDelayed',
      ],
    }),
  );
  return {
    visible: Number(res.Attributes?.ApproximateNumberOfMessages ?? '0'),
    notVisible: Number(res.Attributes?.ApproximateNumberOfMessagesNotVisible ?? '0'),
    delayed: Number(res.Attributes?.ApproximateNumberOfMessagesDelayed ?? '0'),
  };
}

/**
 * Drains and deletes all currently visible messages from an SQS queue in deterministic batches,
 * preserving receive order per FIFO `MessageGroupId`.
 */
export async function drainQueueMessages(
  sqs: SQSClient,
  queueUrl: string,
  expectedMinimumCount = 0,
  timeoutMs = 8_000,
): Promise<Message[]> {
  const collected: Message[] = [];
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const res = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 0,
        AttributeNames: ['All'],
        MessageSystemAttributeNames: ['All'],
        MessageAttributeNames: ['All'],
      }),
    );
    const batch = res.Messages ?? [];
    if (batch.length === 0) {
      if (collected.length >= expectedMinimumCount) {
        break;
      }
      await Bun.sleep(25);
      continue;
    }
    collected.push(...batch);
    await sqs.send(
      new DeleteMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: batch.map((msg, idx) => ({
          Id: String(idx),
          ReceiptHandle: msg.ReceiptHandle!,
        })),
      }),
    );
  }

  if (
    collected.length > 1 &&
    collected.every(
      (msg) => msg.Attributes?.SequenceNumber !== undefined && /^\d+$/.test(msg.Attributes.SequenceNumber),
    )
  ) {
    // Order by SQS FIFO's authoritative monotonic broker `SequenceNumber` (assigned on SendMessage receipt),
    // avoiding false inversions from LocalStack's wall-clock `SentTimestamp` heap if VM `CLOCK_REALTIME` slews.
    collected.sort((a, b) => {
      const seqA = BigInt(a.Attributes!.SequenceNumber!);
      const seqB = BigInt(b.Attributes!.SequenceNumber!);
      return seqA < seqB ? -1 : seqA > seqB ? 1 : 0;
    });
  }

  return collected;
}

