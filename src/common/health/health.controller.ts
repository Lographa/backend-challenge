import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { Controller, Get, Logger, ServiceUnavailableException } from '@nestjs/common';
import { Public } from '../auth/auth.guard';
import { AppConfig } from '../config/env';

type DependencyStatus = 'up' | 'down';

export interface ReadinessReport {
  status: 'ok' | 'unavailable';
  checks: { postgres: DependencyStatus; sqs: DependencyStatus };
}

@Public()
@Controller('health')
export class HealthController {
  private readonly logger = new Logger(HealthController.name);

  constructor(
    private readonly orm: MikroORM,
    private readonly sqs: SQSClient,
    private readonly config: AppConfig,
  ) {}

  @Get('live')
  live(): { status: 'ok' } {
    return { status: 'ok' };
  }

  @Get('ready')
  async ready(): Promise<ReadinessReport> {
    const [postgres, sqs] = await Promise.all([
      this.check('postgres', () => this.pingPostgres()),
      this.check('sqs', () => this.pingSqs()),
    ]);
    const report: ReadinessReport = {
      status: postgres === 'up' && sqs === 'up' ? 'ok' : 'unavailable',
      checks: { postgres, sqs },
    };
    if (report.status !== 'ok') {
      throw new ServiceUnavailableException(report);
    }
    return report;
  }

  private async pingPostgres(): Promise<void> {
    // MikroORM 7 connects lazily, so checkConnection() reports "not established"
    // until an explicit connect(); a real round-trip is the reliable probe.
    await this.orm.em.getConnection().execute('select 1');
  }

  private async pingSqs(): Promise<void> {
    const queues = [
      this.config.wagerTransactionsQueue,
      this.config.wagerTransactionsDlq,
      this.config.integrationEventsQueue,
    ];
    await Promise.all(
      queues.map((QueueName) =>
        this.sqs.send(new GetQueueUrlCommand({ QueueName }), {
          abortSignal: AbortSignal.timeout(this.config.healthCheckTimeoutMs),
        }),
      ),
    );
  }

  private async check(dependency: string, probe: () => Promise<void>): Promise<DependencyStatus> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`timed out after ${this.config.healthCheckTimeoutMs}ms`)),
        this.config.healthCheckTimeoutMs,
      );
    });
    try {
      await Promise.race([probe(), timeout]);
      return 'up';
    } catch (error) {
      this.logger.warn({
        message: 'Readiness dependency check failed',
        dependency,
        reason: error instanceof Error ? error.message : String(error),
      });
      return 'down';
    } finally {
      clearTimeout(timer);
    }
  }
}
