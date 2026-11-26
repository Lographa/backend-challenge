import type { LogLevel } from '@nestjs/common';

const LOG_LEVELS = ['fatal', 'error', 'warn', 'log', 'debug', 'verbose'] as const;
const LOG_LEVEL_ALIASES: Record<string, LogLevel> = { info: 'log' };

export class InvalidEnvironmentError extends Error {
  constructor(readonly issues: string[]) {
    super(`Invalid environment: ${issues.join('; ')}`);
    this.name = 'InvalidEnvironmentError';
  }
}

export class AppConfig {
  private constructor(
    readonly port: number,
    readonly logLevel: LogLevel,
    readonly databaseUrl: string,
    readonly databasePoolMax: number,
    readonly awsRegion: string,
    readonly awsEndpointUrl: string | undefined,
    readonly wagerTransactionsQueue: string,
    readonly wagerTransactionsDlq: string,
    readonly integrationEventsQueue: string,
    readonly healthCheckTimeoutMs: number,
    readonly shutdownTimeoutMs: number,
    readonly workersEnabled: boolean,
    readonly sqsWaitTimeSeconds: number,
    readonly sqsCallTimeoutMs: number,
    readonly outboxPollIntervalMs: number,
    readonly pendingReferencePollIntervalMs: number,
    readonly pendingReferenceTtlMs: number,
    readonly pendingReferenceMaxAttempts: number,
  ) {}

  static load(source: Record<string, string | undefined>): AppConfig {
    const issues: string[] = [];
    const read = new EnvReader(source, issues);

    const config = new AppConfig(
      read.integer('PORT', { default: 3000, min: 1, max: 65535 }),
      read.logLevel('LOG_LEVEL', 'log'),
      read.url('DATABASE_URL', ['postgres:', 'postgresql:']),
      read.integer('DATABASE_POOL_MAX', { default: 10, min: 1, max: 100 }),
      read.string('AWS_REGION'),
      read.optionalUrl('AWS_ENDPOINT_URL', ['http:', 'https:']),
      read.fifoQueueName('SQS_WAGER_TRANSACTIONS_QUEUE', 'wager-transactions.fifo'),
      read.fifoQueueName('SQS_WAGER_TRANSACTIONS_DLQ', 'wager-transactions-dlq.fifo'),
      read.fifoQueueName('SQS_INTEGRATION_EVENTS_QUEUE', 'wager-integration-events.fifo'),
      read.integer('HEALTH_CHECK_TIMEOUT_MS', { default: 2000, min: 100, max: 30000 }),
      read.integer('SHUTDOWN_TIMEOUT_MS', { default: 10000, min: 1000, max: 120000 }),
      read.boolean('WORKERS_ENABLED', source.NODE_ENV?.trim() !== 'test'),
      read.integer('SQS_WAIT_TIME_SECONDS', { default: 10, min: 0, max: 20 }),
      read.integer('SQS_CALL_TIMEOUT_MS', { default: 5000, min: 100, max: 60000 }),
      read.integer('OUTBOX_POLL_INTERVAL_MS', { default: 200, min: 10, max: 60000 }),
      read.integer('PENDING_REFERENCE_POLL_INTERVAL_MS', { default: 250, min: 10, max: 60000 }),
      read.integer('PENDING_REFERENCE_TTL_MS', { default: 60000, min: 100, max: 86400000 }),
      read.integer('PENDING_REFERENCE_MAX_ATTEMPTS', { default: 10, min: 1, max: 100 }),
    );

    if (issues.length > 0) {
      throw new InvalidEnvironmentError(issues);
    }
    return config;
  }
}

class EnvReader {
  constructor(
    private readonly source: Record<string, string | undefined>,
    private readonly issues: string[],
  ) {}

  string(name: string, fallback?: string): string {
    const value = this.raw(name) ?? fallback;
    if (value === undefined) {
      this.issues.push(`${name} is required`);
      return '';
    }
    return value;
  }

  integer(name: string, rule: { default: number; min: number; max: number }): number {
    const value = this.raw(name);
    if (value === undefined) {
      return rule.default;
    }
    if (!/^\d+$/.test(value)) {
      this.issues.push(`${name} must be an integer`);
      return rule.default;
    }
    const parsed = Number(value);
    if (parsed < rule.min || parsed > rule.max) {
      this.issues.push(`${name} must be between ${rule.min} and ${rule.max}`);
    }
    return parsed;
  }

  url(name: string, protocols: string[]): string {
    const value = this.string(name);
    if (value !== '') {
      this.checkUrl(name, value, protocols);
    }
    return value;
  }

  optionalUrl(name: string, protocols: string[]): string | undefined {
    const value = this.raw(name);
    if (value !== undefined) {
      this.checkUrl(name, value, protocols);
    }
    return value;
  }

  logLevel(name: string, fallback: LogLevel): LogLevel {
    const value = this.raw(name)?.toLowerCase();
    if (value === undefined) {
      return fallback;
    }
    const level = LOG_LEVEL_ALIASES[value] ?? LOG_LEVELS.find((candidate) => candidate === value);
    if (level === undefined) {
      this.issues.push(`${name} must be one of info, ${LOG_LEVELS.join(', ')}`);
      return fallback;
    }
    return level;
  }

  fifoQueueName(name: string, fallback: string): string {
    const value = this.string(name, fallback);
    if (!/^[A-Za-z0-9_-]{1,75}\.fifo$/.test(value)) {
      this.issues.push(`${name} must be a valid FIFO queue name ending in .fifo`);
    }
    return value;
  }

  boolean(name: string, fallback: boolean): boolean {
    const value = this.raw(name)?.toLowerCase();
    if (value === undefined) {
      return fallback;
    }
    if (value === 'true') {
      return true;
    }
    if (value === 'false') {
      return false;
    }
    this.issues.push(`${name} must be true or false`);
    return fallback;
  }

  private checkUrl(name: string, value: string, protocols: string[]): void {
    if (!URL.canParse(value) || !protocols.includes(new URL(value).protocol)) {
      this.issues.push(`${name} must be a URL with protocol ${protocols.join(' or ')}`);
    }
  }

  private raw(name: string): string | undefined {
    const value = this.source[name]?.trim();
    return value === undefined || value === '' ? undefined : value;
  }
}
