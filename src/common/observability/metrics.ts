import { Logger } from '@nestjs/common';

/** Exclusive upper bounds, in milliseconds. A value lands in the first bucket whose bound it does not exceed. */
export const PROCESSING_DURATION_BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000] as const;

const PERIODIC_SNAPSHOT_MS = 60_000;

const COUNTER_LABELS = {
  wager_transaction_status_transitions_total: { status: ['PENDING_REFERENCE', 'PROCESSED', 'REJECTED'] },
  wager_duplicates_total: { cause: ['inbox', 'idempotency_replay'] },
  wager_hash_conflicts_total: { cause: ['inbox_payload', 'idempotency'] },
  wager_retries_total: {
    channel: ['sqs', 'outbox', 'pending_reference'],
    cause: ['business', 'infrastructure', 'unexpected'],
  },
  wager_dlq_forwards_completed_total: {},
  wager_lock_conflicts_total: { sqlstate: ['40P01', '55P03'] },
  wallet_reconciliation_mismatch_total: {},
} as const;

const HISTOGRAM_LABELS = {
  wager_processing_duration_ms: {
    channel: ['http', 'sqs', 'pending_reference'],
    outcome: ['completed', 'error'],
  },
} as const;

const GAUGE_NAME = 'wager_outbox_lag_ms';

type CounterName = keyof typeof COUNTER_LABELS;
type HistogramName = keyof typeof HISTOGRAM_LABELS;
type LabelSpec = Readonly<Record<string, readonly string[]>>;
type Labels = Readonly<Record<string, string>>;

export interface CounterSample extends Record<string, string | number> {
  value: number;
}

export interface HistogramSample {
  channel: string;
  outcome: string;
  count: number;
  sumMs: number;
  bucketsMs: readonly number[];
  bucketCounts: readonly number[];
  overflowCount: number;
}

export interface GaugeSample {
  value: number;
  sharedSample: true;
}

export interface MetricsSnapshot {
  instanceId: string;
  scope: 'process';
  counters: Record<string, CounterSample[]>;
  gauges: Record<string, GaugeSample>;
  histograms: Record<string, HistogramSample[]>;
  instrumentationFailures: number;
}

interface CounterSeries {
  labels: Record<string, string>;
  value: number;
}

interface HistogramSeries {
  labels: Record<string, string>;
  count: number;
  sumMs: number;
  bucketCounts: number[];
  overflowCount: number;
}

type FailureReason = 'unknown_metric' | 'rejected_label' | 'invalid_value' | 'internal';

/**
 * Process-local counters, one gauge and one histogram. Closed names and labels.
 * Every public method swallows its own failures. Counters are not reset on shutdown.
 */
export class MetricsRegistry {
  private readonly logger = new Logger(MetricsRegistry.name);
  private readonly counters = new Map<string, CounterSeries>();
  private readonly histograms = new Map<string, HistogramSeries>();
  private readonly warnedReasons = new Set<FailureReason>();
  private gauge: number | undefined;
  private instrumentationFailures = 0;
  private timer: ReturnType<typeof setInterval> | undefined;
  private finalSnapshotEmitted = false;
  private failNextMutation = false;

  constructor(private readonly instanceId: string) {}

  /** Test seam: the next mutation throws inside the registry and is recorded, not propagated. */
  breakNextMutationForTesting(): void {
    this.failNextMutation = true;
  }

  increment(name: CounterName, labels?: Labels): void {
    this.guard(() => {
      const spec = COUNTER_LABELS[name] as LabelSpec | undefined;
      if (spec === undefined) {
        this.noteFailure('unknown_metric');
        return;
      }
      const accepted = acceptLabels(spec, labels);
      if (accepted === undefined) {
        this.noteFailure('rejected_label');
        return;
      }
      const key = seriesKey(name, accepted);
      const existing = this.counters.get(key);
      if (existing === undefined) {
        this.counters.set(key, { labels: accepted, value: 1 });
        return;
      }
      existing.value += 1;
    });
  }

  set(name: typeof GAUGE_NAME, value: number): void {
    this.guard(() => {
      if (name !== GAUGE_NAME) {
        this.noteFailure('unknown_metric');
        return;
      }
      if (!Number.isFinite(value) || value < 0) {
        this.noteFailure('invalid_value');
        return;
      }
      this.gauge = value;
    });
  }

  observe(name: HistogramName, value: number, labels?: Labels): void {
    this.guard(() => {
      const spec = HISTOGRAM_LABELS[name] as LabelSpec | undefined;
      if (spec === undefined) {
        this.noteFailure('unknown_metric');
        return;
      }
      if (!Number.isFinite(value) || value < 0) {
        this.noteFailure('invalid_value');
        return;
      }
      const accepted = acceptLabels(spec, labels);
      if (accepted === undefined) {
        this.noteFailure('rejected_label');
        return;
      }
      const key = seriesKey(name, accepted);
      let series = this.histograms.get(key);
      if (series === undefined) {
        series = {
          labels: accepted,
          count: 0,
          sumMs: 0,
          bucketCounts: PROCESSING_DURATION_BUCKETS_MS.map(() => 0),
          overflowCount: 0,
        };
        this.histograms.set(key, series);
      }
      series.count += 1;
      series.sumMs += value;
      const slot = histogramSlot(value);
      if (slot === 'overflow') {
        series.overflowCount += 1;
      } else {
        series.bucketCounts[slot] = (series.bucketCounts[slot] ?? 0) + 1;
      }
    });
  }

  snapshot(): MetricsSnapshot {
    try {
      this.throwIfBroken();
      return this.buildSnapshot();
    } catch {
      this.noteFailure('internal');
      return {
        instanceId: this.instanceId,
        scope: 'process',
        counters: {},
        gauges: {},
        histograms: {},
        instrumentationFailures: this.instrumentationFailures,
      };
    }
  }

  startPeriodicSnapshot(intervalMs = PERIODIC_SNAPSHOT_MS): void {
    if (this.timer !== undefined) {
      return;
    }
    const timer = setInterval(() => {
      this.emit('periodic');
    }, intervalMs);
    timer.unref?.();
    this.timer = timer;
  }

  /** Stops the periodic log. In-flight counters, gauges and histograms stay. */
  stopPeriodicSnapshot(): void {
    if (this.timer === undefined) {
      return;
    }
    clearInterval(this.timer);
    this.timer = undefined;
  }

  emitFinalSnapshot(): void {
    if (this.finalSnapshotEmitted) {
      return;
    }
    this.finalSnapshotEmitted = true;
    this.emit('shutdown');
  }

  private emit(emission: 'periodic' | 'shutdown'): void {
    let snap: MetricsSnapshot;
    try {
      snap = this.buildSnapshot();
    } catch {
      this.noteFailure('internal');
      return;
    }
    try {
      this.logger.log('metrics_snapshot', { ...snap, emission });
    } catch {
      this.noteFailure('internal');
    }
  }

  private buildSnapshot(): MetricsSnapshot {
    const counters: Record<string, CounterSample[]> = {};
    for (const [key, series] of [...this.counters.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      const name = key.slice(0, key.indexOf('\u0000'));
      const sample: CounterSample = { ...series.labels, value: series.value };
      const list = counters[name] ?? [];
      list.push(sample);
      counters[name] = list;
    }

    const histograms: Record<string, HistogramSample[]> = {};
    for (const [key, series] of [...this.histograms.entries()].sort(([left], [right]) => left.localeCompare(right))) {
      const name = key.slice(0, key.indexOf('\u0000'));
      const sample: HistogramSample = {
        channel: series.labels.channel ?? '',
        outcome: series.labels.outcome ?? '',
        count: series.count,
        sumMs: series.sumMs,
        bucketsMs: PROCESSING_DURATION_BUCKETS_MS,
        bucketCounts: [...series.bucketCounts],
        overflowCount: series.overflowCount,
      };
      const list = histograms[name] ?? [];
      list.push(sample);
      histograms[name] = list;
    }

    const gauges: Record<string, GaugeSample> = {};
    if (this.gauge !== undefined) {
      gauges[GAUGE_NAME] = { value: this.gauge, sharedSample: true };
    }

    return {
      instanceId: this.instanceId,
      scope: 'process',
      counters,
      gauges,
      histograms,
      instrumentationFailures: this.instrumentationFailures,
    };
  }

  private guard(work: () => void): void {
    try {
      this.throwIfBroken();
      work();
    } catch {
      this.noteFailure('internal');
    }
  }

  private throwIfBroken(): void {
    if (!this.failNextMutation) {
      return;
    }
    this.failNextMutation = false;
    throw new Error('injected instrumentation failure');
  }

  private noteFailure(reason: FailureReason): void {
    this.instrumentationFailures += 1;
    if (this.warnedReasons.has(reason)) {
      return;
    }
    this.warnedReasons.add(reason);
    try {
      this.logger.warn('Metrics instrumentation failed', { reason });
    } catch {
      // A failing logger must not escape into financial or messaging code.
    }
  }
}

const RECORDED_STATUSES = new Set(['PENDING_REFERENCE', 'PROCESSED', 'REJECTED']);

/** One committed status transition. PENDING and FAILED are not series. */
export function recordStatusTransition(metrics: MetricsRegistry | undefined, status: string): void {
  if (!RECORDED_STATUSES.has(status)) {
    return;
  }
  metrics?.increment('wager_transaction_status_transitions_total', {
    status: status as 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED',
  });
}

/** Runs instrumentation and discards any throw so the caller keeps its committed result. */
export function guardMetrics(work: () => void): void {
  try {
    work();
  } catch {
    // Instrumentation must not change a committed financial or messaging result.
  }
}

function acceptLabels(spec: LabelSpec, labels: Labels | undefined): Record<string, string> | undefined {
  const provided = labels ?? {};
  const expected = Object.keys(spec);
  const keys = Object.keys(provided);
  if (keys.length !== expected.length) {
    return undefined;
  }
  const accepted: Record<string, string> = {};
  for (const key of expected) {
    const value = provided[key];
    const allowed = spec[key];
    if (value === undefined || allowed === undefined || !allowed.includes(value)) {
      return undefined;
    }
    accepted[key] = value;
  }
  for (const key of keys) {
    if (!expected.includes(key)) {
      return undefined;
    }
  }
  return accepted;
}

function seriesKey(name: string, labels: Record<string, string>): string {
  const encoded = Object.keys(labels)
    .sort()
    .map((key) => `${key}=${labels[key]}`)
    .join('|');
  return `${name}\u0000${encoded}`;
}

function histogramSlot(value: number): number | 'overflow' {
  for (let index = 0; index < PROCESSING_DURATION_BUCKETS_MS.length; index += 1) {
    const upper = PROCESSING_DURATION_BUCKETS_MS[index];
    if (upper !== undefined && value <= upper) {
      return index;
    }
  }
  return 'overflow';
}
