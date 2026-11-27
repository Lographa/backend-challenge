import 'reflect-metadata';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { newId } from '../../src/common/ids';
import { createLogger } from '../../src/common/observability/logger';
import { MetricsRegistry } from '../../src/common/observability/metrics';
import type { MetricsSnapshot } from '../../src/common/observability/metrics';
import { createTestOrm, createTestSql, resetSchema, testDatabaseUrl } from '../support/database';

function databaseName(role: 'app' | 'migrator'): string {
  return decodeURIComponent(new URL(testDatabaseUrl(role)).pathname.replace(/^\//, ''));
}

function mismatchCount(snapshot: MetricsSnapshot): number {
  return snapshot.counters.wallet_reconciliation_mismatch_total?.[0]?.value ?? 0;
}

function statusCount(snapshot: MetricsSnapshot, status: string): number {
  const found = snapshot.counters.wager_transaction_status_transitions_total?.find((item) => item.status === status);
  return typeof found?.value === 'number' ? found.value : 0;
}

async function attemptCleanup(work: () => Promise<unknown>): Promise<void> {
  try {
    await work();
  } catch {
    // One cleanup step must not skip the remaining ones.
  }
}

/** Re-enables the wallet guards even when the compensating balance update throws. */
export async function restoreDivergenceGuards(admin: SQL, walletId: string, diverged: boolean): Promise<void> {
  try {
    if (diverged) {
      await attemptCleanup(() => admin`alter table wallets disable trigger wallets_guard`);
      await attemptCleanup(() => admin`alter table wallets disable trigger wallets_balance_matches_ledger`);
      await attemptCleanup(
        () => admin`update wallets set balance_amount = balance_amount - 1.00 where id = ${walletId}`,
      );
    }
  } finally {
    await attemptCleanup(() => admin`alter table wallets enable trigger wallets_balance_matches_ledger`);
    await attemptCleanup(() => admin`alter table wallets enable trigger wallets_guard`);
  }
}

describe('divergence guard restoration', () => {
  test('re-enables both wallet triggers when the compensating balance update fails', async () => {
    const calls: string[] = [];
    const admin = (async (strings: TemplateStringsArray) => {
      const statement = strings.join(' ');
      calls.push(statement);
      if (statement.includes('update wallets')) {
        throw new Error('compensate failed');
      }
    }) as unknown as SQL;

    await restoreDivergenceGuards(admin, 'wallet-1', true);

    expect(calls.some((statement) => statement.includes('disable trigger wallets_guard'))).toBe(true);
    expect(calls.some((statement) => statement.includes('disable trigger wallets_balance_matches_ledger'))).toBe(true);
    expect(calls.some((statement) => statement.includes('update wallets'))).toBe(true);
    expect(calls.filter((statement) => statement.includes('enable trigger wallets_balance_matches_ledger'))).toHaveLength(1);
    expect(calls.filter((statement) => statement.includes('enable trigger wallets_guard'))).toHaveLength(1);
    expect(calls.at(-2)).toContain('enable trigger wallets_balance_matches_ledger');
    expect(calls.at(-1)).toContain('enable trigger wallets_guard');
  });
});

describe('wallet reconciliation mismatch metric', () => {
  const appDatabase = databaseName('app');
  const migratorDatabase = databaseName('migrator');
  if (appDatabase !== 'wagering_test' || migratorDatabase !== 'wagering_test') {
    throw new Error(`Refusing to run reconciliation metrics against database "${appDatabase}"`);
  }

  let app: INestApplication;
  let baseUrl: string;
  let db: SQL;
  let admin: SQL;

  beforeAll(async () => {
    const migrator = await createTestOrm('migrator');
    try {
      const [identity] = await migrator.em.execute<{ current_database: string }[]>(
        'select current_database() as current_database',
      );
      if (identity?.current_database !== 'wagering_test') {
        throw new Error(`Refusing destructive schema reset on database "${identity?.current_database}"`);
      }
      await resetSchema(migrator);
    } finally {
      await migrator.close();
    }

    const config = AppConfig.load({
      ...process.env,
      DATABASE_URL: testDatabaseUrl('app'),
      WORKERS_ENABLED: 'false',
      INSTANCE_ID: 'recon-metrics',
    });
    app = await NestFactory.create(AppModule.forRoot(config), { logger: createLogger('log', 'recon-metrics') });
    await app.listen(0);
    baseUrl = await app.getUrl();
    db = createTestSql('app');
    admin = createTestSql('migrator');
    const [connected] = await db`select current_database() as current_database`;
    expect(connected.current_database).toBe('wagering_test');
  });

  afterAll(async () => {
    await app?.close();
    await db?.close();
    await admin?.close();
  });

  test('counts each divergent read without changing the balance or the ledger', async () => {
    const metrics = app.get(MetricsRegistry);
    const zero = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-correlation-id': 'corr-zero' },
      body: JSON.stringify({ playerId: newId(), initialBalance: { amount: '0.00', currency: 'BRL' } }),
    });
    expect(zero.status).toBe(201);
    expect(statusCount(metrics.snapshot(), 'PROCESSED')).toBe(0);

    const opened = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-correlation-id': 'corr-open' },
      body: JSON.stringify({ playerId: newId(), initialBalance: { amount: '10.00', currency: 'BRL' } }),
    });
    expect(opened.status).toBe(201);
    const wallet = (await opened.json()) as { id: string };
    expect(statusCount(metrics.snapshot(), 'PROCESSED')).toBe(1);

    const [beforeWallet] = await db`select balance_amount::text as balance, version from wallets where id = ${wallet.id}`;
    const [beforeLedger] = await db`
      select count(*)::int as entries, coalesce(max(balance_after)::text, '0.00') as head
      from wallet_ledger_entries
      where wallet_id = ${wallet.id}
    `;
    expect(beforeWallet.balance).toBe('10.00');
    expect(beforeLedger.entries).toBe(1);
    expect(beforeLedger.head).toBe('10.00');

    let diverged = false;
    try {
      await admin`alter table wallets disable trigger wallets_guard`;
      await admin`alter table wallets disable trigger wallets_balance_matches_ledger`;
      await admin`update wallets set balance_amount = balance_amount + 1.00 where id = ${wallet.id}`;
      diverged = true;
      await admin`alter table wallets enable trigger wallets_balance_matches_ledger`;
      await admin`alter table wallets enable trigger wallets_guard`;

      const captured: string[] = [];
      const original = process.stdout.write.bind(process.stdout);
      process.stdout.write = ((chunk: string | Uint8Array): boolean => {
        captured.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
        return true;
      }) as typeof process.stdout.write;
      let response: Response;
      try {
        response = await fetch(`${baseUrl}/wallets/${wallet.id}/reconciliation`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'x-correlation-id': 'corr-div' },
        });
      } finally {
        process.stdout.write = original;
      }

      expect(response.status).toBe(200);
      const body = (await response.json()) as { consistent: boolean; difference: { amount: string }; checkedEntries: number };
      expect(body.consistent).toBe(false);
      expect(body.difference.amount).toBe('1.00');
      expect(body.checkedEntries).toBe(1);
      expect(mismatchCount(metrics.snapshot())).toBe(1);

      const line = captured.map((chunk) => chunk.trim()).find((chunk) => chunk.includes('Wallet balance diverges'));
      expect(line).toBeDefined();
      const parsed = JSON.parse(line as string) as Record<string, unknown>;
      expect(parsed.message).toBe('Wallet balance diverges from its ledger');
      expect(parsed.correlationId).toBe('corr-div');
      expect(parsed.walletId).toBe(wallet.id);
      expect(parsed.checkedEntries).toBe(1);
      expect(parsed.instanceId).toBe('recon-metrics');
      expect(parsed.difference).toBeUndefined();
      expect(parsed.storedBalance).toBeUndefined();
      expect(parsed.calculatedBalance).toBeUndefined();
      const logged = JSON.stringify(parsed);
      expect(logged).not.toContain('password');
      expect(logged).not.toMatch(/\d+\.\d{2}/);
      expect(logged.toLowerCase()).not.toContain('brl');
      expect(logged).not.toContain('10.00');
      expect(logged).not.toContain('11.00');

      const again = await fetch(`${baseUrl}/wallets/${wallet.id}/reconciliation`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'x-correlation-id': 'corr-div-2' },
      });
      expect(again.status).toBe(200);
      expect(mismatchCount(metrics.snapshot())).toBe(2);

      const [afterWallet] = await db`select balance_amount::text as balance, version from wallets where id = ${wallet.id}`;
      const [afterLedger] = await db`
        select count(*)::int as entries, coalesce(max(balance_after)::text, '0.00') as head
        from wallet_ledger_entries
        where wallet_id = ${wallet.id}
      `;
      expect(afterWallet.balance).toBe('11.00');
      expect(afterWallet.version).toBe(beforeWallet.version);
      expect(afterLedger.entries).toBe(beforeLedger.entries);
      expect(afterLedger.head).toBe(beforeLedger.head);

      const calm = await fetch(`${baseUrl}/wallets`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ playerId: newId(), initialBalance: { amount: '0.00', currency: 'BRL' } }),
      });
      const calmWallet = (await calm.json()) as { id: string };
      const consistent = await fetch(`${baseUrl}/wallets/${calmWallet.id}/reconciliation`, { method: 'POST' });
      expect(consistent.status).toBe(200);
      expect(((await consistent.json()) as { consistent: boolean }).consistent).toBe(true);
      expect(mismatchCount(metrics.snapshot())).toBe(2);
    } finally {
      await restoreDivergenceGuards(admin, wallet.id, diverged);
    }
  });
});
