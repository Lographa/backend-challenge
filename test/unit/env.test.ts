import { describe, expect, test } from 'bun:test';
import { AppConfig, InvalidEnvironmentError } from '../../src/common/config/env';

const valid = {
  DATABASE_URL: 'postgresql://user:pass@localhost:5433/db',
  AWS_REGION: 'us-east-1',
};

function issuesOf(source: Record<string, string | undefined>): string[] {
  try {
    AppConfig.load(source);
  } catch (error) {
    if (error instanceof InvalidEnvironmentError) {
      return error.issues;
    }
    throw error;
  }
  return [];
}

describe('AppConfig.load', () => {
  test('applies defaults when only required variables are set', () => {
    const config = AppConfig.load(valid);

    expect(config.port).toBe(3000);
    expect(config.logLevel).toBe('log');
    expect(config.databasePoolMax).toBe(10);
    expect(config.awsEndpointUrl).toBeUndefined();
    expect(config.wagerTransactionsQueue).toBe('wager-transactions.fifo');
    expect(config.wagerTransactionsDlq).toBe('wager-transactions-dlq.fifo');
    expect(config.integrationEventsQueue).toBe('wager-integration-events.fifo');
  });

  test('reports every missing required variable at once', () => {
    expect(issuesOf({})).toEqual(['DATABASE_URL is required', 'AWS_REGION is required']);
  });

  test('treats blank values as missing', () => {
    expect(issuesOf({ ...valid, AWS_REGION: '   ' })).toEqual(['AWS_REGION is required']);
  });

  test('rejects a database URL with a non-postgres protocol', () => {
    expect(issuesOf({ ...valid, DATABASE_URL: 'mysql://localhost/db' })).toEqual([
      'DATABASE_URL must be a URL with protocol postgres: or postgresql:',
    ]);
  });

  test('rejects non-integer and out-of-range numbers', () => {
    expect(issuesOf({ ...valid, PORT: '3000.5', DATABASE_POOL_MAX: '0' })).toEqual([
      'PORT must be an integer',
      'DATABASE_POOL_MAX must be between 1 and 100',
    ]);
  });

  test('maps LOG_LEVEL=info to the Nest "log" level and rejects unknown levels', () => {
    expect(AppConfig.load({ ...valid, LOG_LEVEL: 'INFO' }).logLevel).toBe('log');
    expect(issuesOf({ ...valid, LOG_LEVEL: 'trace' })).toHaveLength(1);
  });

  test('requires queue names to be FIFO', () => {
    expect(issuesOf({ ...valid, SQS_WAGER_TRANSACTIONS_QUEUE: 'wager-transactions' })).toEqual([
      'SQS_WAGER_TRANSACTIONS_QUEUE must be a valid FIFO queue name ending in .fifo',
    ]);
  });

  test('validates the optional AWS endpoint only when present', () => {
    expect(AppConfig.load({ ...valid, AWS_ENDPOINT_URL: 'http://localhost:4566' }).awsEndpointUrl).toBe(
      'http://localhost:4566',
    );
    expect(issuesOf({ ...valid, AWS_ENDPOINT_URL: 'localhost:4566' })).toHaveLength(1);
  });
});
