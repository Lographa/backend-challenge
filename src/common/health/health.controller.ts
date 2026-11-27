import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { Controller, Get, Logger, Optional, ServiceUnavailableException } from '@nestjs/common';
import { Public } from '../auth/auth.guard';
import { AppConfig } from '../config/env';
import { ShutdownService } from '../http/shutdown.service';

export type DependencyStatus = 'up' | 'down' | 'unknown';

export interface ReadinessReport {
  status: 'ok' | 'unavailable';
  shuttingDown?: true;
  checks: { postgres: DependencyStatus; sqs: DependencyStatus };
}

class ProbeBudgetExhaustedError extends Error {
  constructor(budgetMs: number) {
    super(`timed out after ${budgetMs}ms`);
    this.name = 'ProbeBudgetExhaustedError';
  }
}

@Public()
@Controller('health')
export class HealthController {
  private readonly logger = new Logger(HealthController.name);

  constructor(
    private readonly orm: MikroORM,
    private readonly sqs: SQSClient,
    private readonly config: AppConfig,
    @Optional() private readonly shutdown?: ShutdownService,
  ) {}

  @Get('live')
  live(): { status: 'ok' } {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready(): Promise<ReadinessReport> {
    const shuttingDown = this.shutdown?.isShuttingDown() === true;
    const drainCompleted = this.shutdown?.isHttpDrainCompleted() === true;
    const fullTimeoutMs = this.config.healthCheckTimeoutMs;
    const budgetMs = shuttingDown
      ? Math.min(fullTimeoutMs, this.shutdown!.remainingCooperativeMs())
      : fullTimeoutMs;

    if (drainCompleted || (shuttingDown && budgetMs <= 0)) {
      const exhaustedReport: ReadinessReport = {
        status: 'unavailable',
        shuttingDown: true,
        checks: { postgres: 'unknown', sqs: 'unknown' },
      };
      throw new ServiceUnavailableException(exhaustedReport);
    }

    const budgetShortened = shuttingDown && budgetMs < fullTimeoutMs;
    const [postgres, sqs] = await Promise.all([
      this.check('postgres', () => this.pingPostgres(), budgetMs, budgetShortened),
      this.check('sqs', (signal) => this.pingSqs(signal), budgetMs, budgetShortened),
    ]);
    const currentlyShuttingDown = shuttingDown || this.shutdown?.isShuttingDown() === true;
    const report: ReadinessReport = {
      status: !currentlyShuttingDown && postgres === 'up' && sqs === 'up' ? 'ok' : 'unavailable',
      ...(currentlyShuttingDown ? { shuttingDown: true } : {}),
      checks: { postgres, sqs },
    };
    if (report.status !== 'ok') {
      throw new ServiceUnavailableException(report);
    }
    return report;
  }

  private pingPostgres(): Promise<void> {
    // MikroORM 7 connects lazily, so checkConnection() reports "not established" until a real query.
    // Note on Kysely 0.29.9 (`query-executor-base.js:68-82`): passing `{ signal }` to `execute` races
    // `abortPromise` and rejects BEFORE `queryPromise` finishes on PostgreSQL and BEFORE `release()`
    // returns the pooled `pg` client (while detaching `pg_cancel_backend` on a second connection).
    // Calling `execute('select 1')` without Kysely's detached signal ensures the tracked Promise
    // settles strictly AFTER PostgreSQL finishes and `DefaultConnectionProvider` releases the client.
    const executeProbe = async (): Promise<void> => {
      await this.orm.em.getConnection().execute('select 1');
    };

    if (this.shutdown !== undefined) {
      return this.shutdown.trackCriticalOperation(
        {
          kind: 'health_probe',
          name: 'HealthController.pingPostgres',
          resources: ['postgres'],
          context: { dependency: 'postgres' },
        },
        executeProbe,
      );
    }
    return executeProbe();
  }

  private pingSqs(signal: AbortSignal): Promise<void> {
    const queues = [
      this.config.wagerTransactionsQueue,
      this.config.wagerTransactionsDlq,
      this.config.integrationEventsQueue,
    ];
    const executeProbe = async (): Promise<void> => {
      await Promise.all(
        queues.map((QueueName) =>
          this.sqs.send(new GetQueueUrlCommand({ QueueName }), {
            abortSignal: signal,
          }),
        ),
      );
    };

    if (this.shutdown !== undefined) {
      return this.shutdown.trackCriticalOperation(
        {
          kind: 'health_probe',
          name: 'HealthController.pingSqs',
          resources: ['sqs'],
          context: { dependency: 'sqs' },
          signal,
        },
        executeProbe,
      );
    }
    return executeProbe();
  }

  private async check(
    dependency: string,
    probe: (signal: AbortSignal) => Promise<void>,
    budgetMs: number,
    budgetShortenedByShutdown: boolean,
  ): Promise<DependencyStatus> {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let budgetExpired = false;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        budgetExpired = true;
        const err = new ProbeBudgetExhaustedError(budgetMs);
        controller.abort(err);
        reject(err);
      }, budgetMs);
    });

    const probePromise = probe(controller.signal);
    // Prevent late unhandled rejection if `timeout` wins `Promise.race` while `probePromise` settles later.
    probePromise.catch(() => undefined);

    try {
      await Promise.race([probePromise, timeout]);
      return 'up';
    } catch (error) {
      if ((budgetExpired || error instanceof ProbeBudgetExhaustedError) && budgetShortenedByShutdown) {
        return 'unknown';
      }
      this.logger.warn({
        message: 'Readiness dependency check failed',
        dependency,
        reason: error instanceof Error ? error.message : String(error),
      });
      return 'down';
    } finally {
      clearTimeout(timer);
      if (!controller.signal.aborted) {
        controller.abort();
      }
    }
  }
}
