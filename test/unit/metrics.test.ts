import { Logger } from '@nestjs/common';
import { describe, expect, test } from 'bun:test';
import { createLogger } from '../../src/common/observability/logger';
import { MetricsRegistry, PROCESSING_DURATION_BUCKETS_MS } from '../../src/common/observability/metrics';
import type { CounterSample, HistogramSample, MetricsSnapshot } from '../../src/common/observability/metrics';

function sample(snapshot: MetricsSnapshot, name: string, labels: Record<string, string> = {}): number {
  const series = snapshot.counters[name] ?? [];
  const found = series.find((item) => Object.entries(labels).every(([key, value]) => item[key] === value));
  return typeof found?.value === 'number' ? found.value : 0;
}

describe('MetricsRegistry', () => {
  test('keeps closed series per process and rejects unknown names, labels and values', () => {
    const left = new MetricsRegistry('node-a');
    const right = new MetricsRegistry('node-b');
    const warnings: string[] = [];
    (left as unknown as { logger: { warn: (message: string) => void; log: () => void } }).logger = {
      warn: (message) => warnings.push(message),
      log: () => undefined,
    };

    left.increment('wager_duplicates_total', { cause: 'inbox' });
    left.increment('wager_duplicates_total', { cause: 'inbox' });
    right.increment('wager_duplicates_total', { cause: 'idempotency_replay' });
    left.increment('not-a-metric' as 'wager_duplicates_total');
    left.increment('not-a-metric' as 'wager_duplicates_total');
    left.increment('wager_duplicates_total', { cause: 'wallet-1' });
    left.increment('wager_lock_conflicts_total', { sqlstate: '40001' });
    left.increment('wager_retries_total', { channel: 'sqs', cause: 'business', walletId: 'wallet-1' });
    left.observe('wager_processing_duration_ms', Number.NaN, { channel: 'http', outcome: 'completed' });
    left.observe('wager_processing_duration_ms', -1, { channel: 'http', outcome: 'completed' });
    left.set('wager_outbox_lag_ms', Number.POSITIVE_INFINITY);

    const snap = left.snapshot();
    expect(snap.scope).toBe('process');
    expect(snap.instanceId).toBe('node-a');
    expect(sample(snap, 'wager_duplicates_total', { cause: 'inbox' })).toBe(2);
    expect(sample(snap, 'wager_duplicates_total', { cause: 'idempotency_replay' })).toBe(0);
    expect(sample(right.snapshot(), 'wager_duplicates_total', { cause: 'idempotency_replay' })).toBe(1);
    expect(snap.counters.wager_lock_conflicts_total).toBeUndefined();
    expect(snap.histograms.wager_processing_duration_ms).toBeUndefined();
    expect(snap.gauges.wager_outbox_lag_ms).toBeUndefined();
    expect(snap.instrumentationFailures).toBe(8);
    expect(warnings).toEqual([
      'Metrics instrumentation failed',
      'Metrics instrumentation failed',
      'Metrics instrumentation failed',
    ]);
    expect(JSON.stringify(snap)).not.toContain('wallet-1');
  });

  test('a throwing logger does not escape and is warned once per reason', () => {
    const metrics = new MetricsRegistry('node-a');
    let warnings = 0;
    (metrics as unknown as { logger: { warn: () => void; log: () => void } }).logger = {
      warn: () => {
        warnings += 1;
        throw new Error('logger down');
      },
      log: () => {
        throw new Error('logger down');
      },
    };

    expect(() => metrics.increment('missing' as 'wager_duplicates_total')).not.toThrow();
    expect(() => metrics.increment('missing' as 'wager_duplicates_total')).not.toThrow();
    expect(warnings).toBe(1);
    expect(metrics.snapshot().instrumentationFailures).toBe(2);
    expect(() => metrics.emitFinalSnapshot()).not.toThrow();
  });

  test('histogram buckets are exclusive and values above 5000 go to overflow', () => {
    const metrics = new MetricsRegistry('node-a');
    metrics.observe('wager_processing_duration_ms', 0, { channel: 'http', outcome: 'completed' });
    metrics.observe('wager_processing_duration_ms', 5, { channel: 'http', outcome: 'completed' });
    metrics.observe('wager_processing_duration_ms', 5.1, { channel: 'http', outcome: 'completed' });
    metrics.observe('wager_processing_duration_ms', 5000, { channel: 'http', outcome: 'completed' });
    metrics.observe('wager_processing_duration_ms', 5000.1, { channel: 'http', outcome: 'completed' });
    metrics.observe('wager_processing_duration_ms', 8, { channel: 'sqs', outcome: 'error' });

    const snap = metrics.snapshot();
    const http = snap.histograms.wager_processing_duration_ms?.find((item) => item.channel === 'http');
    const sqs = snap.histograms.wager_processing_duration_ms?.find((item) => item.channel === 'sqs');
    expect(http).toBeDefined();
    expect(sqs).toBeDefined();
    const httpSample = http as HistogramSample;
    expect(httpSample.bucketsMs).toEqual([...PROCESSING_DURATION_BUCKETS_MS]);
    expect(httpSample.bucketCounts[0]).toBe(2);
    expect(httpSample.bucketCounts[1]).toBe(1);
    expect(httpSample.bucketCounts[9]).toBe(1);
    expect(httpSample.overflowCount).toBe(1);
    expect(httpSample.bucketCounts.reduce((sum, count) => sum + count, 0) + httpSample.overflowCount).toBe(httpSample.count);
    expect(sqs?.count).toBe(1);
    expect(sqs?.outcome).toBe('error');
  });

  test('stopPeriodicSnapshot keeps counters and emits one final snapshot', () => {
    const metrics = new MetricsRegistry('node-a');
    const emissions: Array<Record<string, unknown>> = [];
    (metrics as unknown as { logger: { log: (message: string, fields: Record<string, unknown>) => void; warn: () => void } }).logger =
      {
        log: (_message, fields) => emissions.push(fields),
        warn: () => undefined,
      };

    metrics.increment('wager_dlq_forwards_completed_total');
    metrics.startPeriodicSnapshot(60_000);
    const timer = (metrics as unknown as { timer?: { hasRef?: () => boolean } }).timer;
    expect(timer?.hasRef?.()).toBe(false);
    metrics.stopPeriodicSnapshot();
    expect((metrics as unknown as { timer?: unknown }).timer).toBeUndefined();
    metrics.increment('wager_dlq_forwards_completed_total');
    metrics.set('wager_outbox_lag_ms', 0);

    metrics.emitFinalSnapshot();
    metrics.emitFinalSnapshot();
    expect(emissions).toHaveLength(1);
    expect(emissions[0]?.emission).toBe('shutdown');
    expect(emissions[0]?.scope).toBe('process');
    expect(emissions[0]?.instanceId).toBe('node-a');
    const counters = emissions[0]?.counters as Record<string, CounterSample[]>;
    expect(counters.wager_dlq_forwards_completed_total?.[0]?.value).toBe(2);
    expect(emissions[0]?.gauges).toEqual({ wager_outbox_lag_ms: { value: 0, sharedSample: true } });
    expect(metrics.snapshot().instrumentationFailures).toBe(0);
  });

  test('an injected registry failure does not throw', () => {
    const metrics = new MetricsRegistry('node-a');
    metrics.breakNextMutationForTesting();
    expect(() => metrics.increment('wallet_reconciliation_mismatch_total')).not.toThrow();
    expect(sample(metrics.snapshot(), 'wallet_reconciliation_mismatch_total')).toBe(0);
    expect(metrics.snapshot().instrumentationFailures).toBeGreaterThanOrEqual(1);
    metrics.increment('wallet_reconciliation_mismatch_total');
    expect(sample(metrics.snapshot(), 'wallet_reconciliation_mismatch_total')).toBe(1);
  });

  test('the shutdown snapshot log is one JSON object with flattened fields', () => {
    const captured: string[] = [];
    const original = process.stdout.write.bind(process.stdout);
    process.stdout.write = ((chunk: string | Uint8Array): boolean => {
      captured.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
      return true;
    }) as typeof process.stdout.write;

    try {
      Logger.overrideLogger(createLogger('log', 'node-json'));
      const metrics = new MetricsRegistry('node-json');
      metrics.increment('wager_duplicates_total', { cause: 'inbox' });
      metrics.set('wager_outbox_lag_ms', 12);
      metrics.observe('wager_processing_duration_ms', 5001, { channel: 'http', outcome: 'completed' });
      metrics.emitFinalSnapshot();
    } finally {
      process.stdout.write = original;
      Logger.overrideLogger(true);
    }

    const line = captured.map((chunk) => chunk.trim()).find((chunk) => chunk.includes('metrics_snapshot'));
    expect(line).toBeDefined();
    const parsed = JSON.parse(line as string) as Record<string, unknown>;
    expect(parsed.message).toBe('metrics_snapshot');
    expect(parsed.level).toBe('log');
    expect(parsed.instanceId).toBe('node-json');
    expect(parsed.scope).toBe('process');
    expect(parsed.emission).toBe('shutdown');
    expect(parsed.pid).toBe(process.pid);
    const gauges = parsed.gauges as { wager_outbox_lag_ms: { value: number; sharedSample: boolean } };
    expect(gauges.wager_outbox_lag_ms).toEqual({ value: 12, sharedSample: true });
    const histograms = parsed.histograms as { wager_processing_duration_ms: HistogramSample[] };
    const http = histograms.wager_processing_duration_ms[0];
    expect(http?.overflowCount).toBe(1);
    expect(http?.bucketCounts.reduce((sum, count) => sum + count, 0)).toBe(0);
    expect(JSON.stringify(parsed)).not.toContain('walletId');
  });
});
