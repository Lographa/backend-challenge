import { createConnection, createServer as createNetServer, type Socket } from 'node:net';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import type { SQL } from 'bun';
import Decimal from 'decimal.js';
import { FailureCode } from '../../src/common/errors/failure-code';
import { newId } from '../../src/common/ids';
import { INSTANCE_ID_HEADER } from '../../src/common/instance/instance-identity';
import {
  toWagerTransactionSendInput,
  WAGER_REQUESTED_MESSAGE_TYPE,
  type WagerTransactionRequestedEnvelope,
} from '../../src/messaging/wager-message';
import { WagerTransactionKind, WagerTransactionStatus } from '../../src/wagering/wager-transaction';
import type { SubmitWagerResponseView } from '../../src/wagering/wagering.controller';
import type { WalletReconciliation } from '../../src/wallets/wallet-reconciliation';
import type { WalletView } from '../../src/wallets/wallets.controller';
import { createTestOrm, createTestSql, resetSchema, testRoleName } from '../support/database';
import {
  type GlobalLedgerAuditReport,
  runIndependentGlobalLedgerAudit,
} from '../support/global-ledger-audit';
import {
  createClusterQueues,
  createTestSqsClient,
  drainQueueMessages,
  type GracefulShutdownEvidence,
  getQueueDepth,
  type IsolatedClusterQueues,
  isOsProcessAlive,
  MultiProcessCluster,
  pollUntil,
  type RunningServiceInstance,
} from '../support/multi-process';

class GateRollbackSentinel extends Error {
  constructor() {
    super('Intentional rollback of test barrier transaction');
  }
}

interface HttpJsonResult<T = unknown> {
  readonly status: number;
  readonly instanceIdHeader: string | null;
  readonly body: T;
}

type OperationAccountingCategory =
  | 'committed'
  | 'rejectedBeforeFinancialOp'
  | 'idempotentReplay'
  | 'knownRecoverableFailure';

interface AccountedOperation {
  readonly opId: string;
  readonly idempotencyKey: string;
  readonly kind: WagerTransactionKind;
  readonly category: OperationAccountingCategory;
  readonly httpStatus: number | null;
  readonly detail: string;
}

class OperationLedgerAccountant {
  private readonly records: AccountedOperation[] = [];

  record(entry: AccountedOperation): void {
    this.records.push(entry);
  }

  summary() {
    let committed = 0;
    let rejectedBeforeFinancialOp = 0;
    let idempotentReplay = 0;
    let knownRecoverableFailure = 0;

    for (const r of this.records) {
      switch (r.category) {
        case 'committed':
          committed += 1;
          break;
        case 'rejectedBeforeFinancialOp':
          rejectedBeforeFinancialOp += 1;
          break;
        case 'idempotentReplay':
          idempotentReplay += 1;
          break;
        case 'knownRecoverableFailure':
          knownRecoverableFailure += 1;
          break;
      }
    }

    return {
      totalSubmitted: this.records.length,
      committed,
      rejectedBeforeFinancialOp,
      idempotentReplay,
      knownRecoverableFailure,
      unaccounted:
        this.records.length -
        (committed + rejectedBeforeFinancialOp + idempotentReplay + knownRecoverableFailure),
      entries: [...this.records],
    };
  }
}

function classifyHttpOutcome(
  opId: string,
  idempotencyKey: string,
  kind: WagerTransactionKind,
  outcome: { ok: true; res: HttpJsonResult<Record<string, unknown>> } | { ok: false; error: unknown },
): AccountedOperation {
  if (!outcome.ok) {
    const reason = outcome.error instanceof Error ? outcome.error.message : String(outcome.error);
    return {
      opId,
      idempotencyKey,
      kind,
      category: 'knownRecoverableFailure',
      httpStatus: null,
      detail: `Transport error during shutdown transition: ${reason}`,
    };
  }

  const { status, body } = outcome.res;
  if (status === 503) {
    return {
      opId,
      idempotencyKey,
      kind,
      category: 'rejectedBeforeFinancialOp',
      httpStatus: status,
      detail: String(body?.code ?? 'SERVICE_UNAVAILABLE'),
    };
  }
  if (status === 400 || status === 404 || status === 409) {
    return {
      opId,
      idempotencyKey,
      kind,
      category: 'rejectedBeforeFinancialOp',
      httpStatus: status,
      detail: String(body?.code ?? `HTTP_${status}`),
    };
  }
  if (status === 200 || status === 202 || status === 422) {
    if (body?.idempotentReplay === true) {
      return {
        opId,
        idempotencyKey,
        kind,
        category: 'idempotentReplay',
        httpStatus: status,
        detail: `Replay status=${String(body?.status)}`,
      };
    }
    return {
      opId,
      idempotencyKey,
      kind,
      category: 'committed',
      httpStatus: status,
      detail: `Persisted status=${String(body?.status)}`,
    };
  }

  return {
    opId,
    idempotencyKey,
    kind,
    category: 'knownRecoverableFailure',
    httpStatus: status,
    detail: `Unexpected transient status=${status}`,
  };
}

describe('Shutdown under load, cold restart & independent global ledger audit (Stage 7.5 — wagering_test + LocalStack + >=3 OS processes)', () => {
  let migratorOrm: MikroORM;
  let adminSql: SQL;
  let appSql: SQL;
  let gateSql: SQL;
  let sqs: SQSClient;
  let cluster: MultiProcessCluster;
  let activeQueues: IsolatedClusterQueues | undefined;
  const activeCleanupHooks: Array<() => Promise<void> | void> = [];

  beforeAll(async () => {
    migratorOrm = await createTestOrm('migrator');
    adminSql = createTestSql('admin');
    appSql = createTestSql('app');
    gateSql = createTestSql('app');
    sqs = createTestSqsClient();
    cluster = new MultiProcessCluster();

    // Explicit target database & role verification before any test executes:
    const [appIdentity] = await appSql<Array<{ db: string; usr: string }>>`
      select current_database() as db, current_user as usr
    `;
    const [adminIdentity] = await adminSql<Array<{ db: string; usr: string }>>`
      select current_database() as db, current_user as usr
    `;
    expect(appIdentity?.db).toBe('wagering_test');
    expect(appIdentity?.usr).toBe(testRoleName('app'));
    expect(adminIdentity?.db).toBe('wagering_test');
  });

  beforeEach(async () => {
    await resetSchema(migratorOrm);
  });

  afterEach(async () => {
    while (activeCleanupHooks.length > 0) {
      const fn = activeCleanupHooks.pop();
      await fn?.();
    }

    await cluster.stopAllGracefully().catch(async (err) => {
      await cluster.forceKillAll();
      throw err;
    });

    if (activeQueues !== undefined) {
      await activeQueues.destroy();
      activeQueues = undefined;
    }

    const lingering = await pollUntil(
      async () => {
        const rows = await adminSql<Array<{ pid: number; application_name: string }>>`
          select pid::int as pid, application_name
          from pg_stat_activity
          where datname = current_database()
            and application_name like 'wagering:%'
        `;
        return rows.length === 0 ? rows : undefined;
      },
      { timeoutMs: 5_000, intervalMs: 25, description: 'zero lingering wagering:* PostgreSQL sessions' },
    );
    expect([...lingering]).toEqual([]);
  });

  afterAll(async () => {
    while (activeCleanupHooks.length > 0) {
      await activeCleanupHooks.pop()?.();
    }
    await cluster.forceKillAll();
    if (activeQueues !== undefined) {
      await activeQueues.destroy();
    }
    sqs.destroy();
    await gateSql.close();
    await appSql.close();
    await adminSql.close();
    await migratorOrm.close(true);
  });

  async function spawnClusterNodes(
    prefix: string,
    count: number,
    queues: IsolatedClusterQueues,
    overrides: Partial<{
      workersEnabled: boolean;
      sqsWaitTimeSeconds: number;
      outboxPollIntervalMs: number;
      pendingReferencePollIntervalMs: number;
      shutdownTimeoutMs: number;
      envOverrides: Readonly<Record<string, string>>;
    }> = {},
  ): Promise<RunningServiceInstance[]> {
    const nodes = await Promise.all(
      Array.from({ length: count }, (_unused, idx) =>
        cluster.spawn({
          instanceId: `${prefix}-${idx + 1}`,
          port: 0,
          queues,
          workersEnabled: overrides.workersEnabled ?? true,
          sqsWaitTimeSeconds: overrides.sqsWaitTimeSeconds ?? 1,
          outboxPollIntervalMs: overrides.outboxPollIntervalMs ?? 50,
          pendingReferencePollIntervalMs: overrides.pendingReferencePollIntervalMs ?? 50,
          shutdownTimeoutMs: overrides.shutdownTimeoutMs ?? 6_000,
          ...(overrides.envOverrides !== undefined ? { envOverrides: overrides.envOverrides } : {}),
        }),
      ),
    );

    expect(new Set(nodes.map((n) => n.pid)).size).toBe(count);
    expect(new Set(nodes.map((n) => n.port)).size).toBe(count);
    expect(new Set(nodes.map((n) => n.instanceId)).size).toBe(count);
    return nodes;
  }

  async function openWallet(
    instance: RunningServiceInstance,
    amount = '200.00',
    currency = 'BRL',
  ): Promise<WalletView> {
    const res = await instance.request('/wallets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId: newId(),
        initialBalance: { amount, currency },
      }),
    });
    expect(res.status).toBe(201);
    return (await res.json()) as WalletView;
  }

  async function safeSubmitWager(
    instance: RunningServiceInstance,
    payload: Record<string, unknown>,
    idempotencyKey: string,
  ): Promise<{ ok: true; res: HttpJsonResult<Record<string, unknown>> } | { ok: false; error: unknown }> {
    try {
      const res = await instance.request('/wagering/transactions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': idempotencyKey,
        },
        body: JSON.stringify(payload),
      });
      const body = (await res.json()) as Record<string, unknown>;
      return {
        ok: true,
        res: {
          status: res.status,
          instanceIdHeader: res.headers.get(INSTANCE_ID_HEADER),
          body,
        },
      };
    } catch (error) {
      return { ok: false, error };
    }
  }

  function buildWagerPayload(
    wallet: WalletView,
    overrides: Partial<{
      providerId: string;
      externalTransactionId: string;
      roundId: string;
      gameId: string;
      kind: WagerTransactionKind;
      amount: string;
      currency: string;
      referenceExternalTransactionId: string;
    }> = {},
  ): Record<string, unknown> {
    return {
      providerId: overrides.providerId ?? 'prov-stage75',
      externalTransactionId: overrides.externalTransactionId ?? `ext-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: overrides.roundId ?? `round-${newId().slice(0, 8)}`,
      gameId: overrides.gameId ?? 'crash-75',
      kind: overrides.kind ?? WagerTransactionKind.Bet,
      money: {
        amount: overrides.amount ?? '10.00',
        currency: overrides.currency ?? wallet.balance.currency,
      },
      ...(overrides.referenceExternalTransactionId !== undefined
        ? { referenceExternalTransactionId: overrides.referenceExternalTransactionId }
        : {}),
    };
  }

  function buildSqsEnvelope(
    wallet: WalletView,
    overrides: Partial<{
      messageId: string;
      providerId: string;
      externalTransactionId: string;
      idempotencyKey: string;
      roundId: string;
      gameId: string;
      kind: WagerTransactionKind;
      amount: string;
      currency: string;
      referenceExternalTransactionId: string;
    }> = {},
  ): WagerTransactionRequestedEnvelope {
    const extId = overrides.externalTransactionId ?? `sqs-ext-${newId()}`;
    return {
      messageId: overrides.messageId ?? `msg-${newId()}`,
      type: WAGER_REQUESTED_MESSAGE_TYPE,
      occurredAt: new Date().toISOString(),
      correlationId: `corr-${newId().slice(0, 12)}`,
      data: {
        providerId: overrides.providerId ?? 'prov-stage75',
        externalTransactionId: extId,
        idempotencyKey: overrides.idempotencyKey ?? `idem-${extId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: overrides.roundId ?? `round-sqs-${newId().slice(0, 8)}`,
        gameId: overrides.gameId ?? 'roulette-75',
        kind: overrides.kind ?? WagerTransactionKind.Bet,
        money: {
          amount: overrides.amount ?? '10.00',
          currency: overrides.currency ?? wallet.balance.currency,
        },
        ...(overrides.referenceExternalTransactionId !== undefined
          ? { referenceExternalTransactionId: overrides.referenceExternalTransactionId }
          : {}),
      },
    };
  }

  async function holdWalletRowLock(walletId: string): Promise<{
    readonly gatePid: number;
    readonly release: () => Promise<void>;
  }> {
    let resolveGateReady!: (pid: number) => void;
    const gateReady = new Promise<number>((resolve) => {
      resolveGateReady = resolve;
    });

    let releaseLock!: () => void;
    const releasePromise = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });

    const txDone = gateSql.begin(async (tx) => {
      const [pidRow] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wallets where id = ${walletId} for update`;
      resolveGateReady(Number(pidRow!.pid));
      await releasePromise;
    });

    const gatePid = await gateReady;
    let released = false;
    const release = async (): Promise<void> => {
      if (released) {
        return;
      }
      released = true;
      releaseLock();
      await txDone;
    };
    activeCleanupHooks.push(release);
    return { gatePid, release };
  }

  async function waitForBlockedBackendCount(
    gatePid: number,
    expectedCount: number,
    applicationNameFilter?: string,
  ): Promise<Array<{ pid: number; application_name: string }>> {
    return pollUntil(
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
        const filtered =
          applicationNameFilter !== undefined
            ? rows.filter((r) => r.application_name === applicationNameFilter)
            : rows;
        return filtered.length >= expectedCount ? filtered : undefined;
      },
      {
        timeoutMs: 8_000,
        intervalMs: 15,
        description: `${expectedCount} backend(s) blocked on gatePid=${gatePid}`,
      },
    );
  }

  function assertValidGracefulEvidence(ev: GracefulShutdownEvidence, expectedExitCode = 0): void {
    expect(ev.exitCode).toBe(expectedExitCode);
    expect(ev.osProcessAliveAfterExit).toBe(false);
    expect(isOsProcessAlive(ev.pid)).toBe(false);
    expect(ev.startedAt).not.toBeNull();
    expect(ev.completedAt).not.toBeNull();
  }

  // ===========================================================================
  // CENÁRIO A — Shutdown gracioso sob carga HTTP real (repetido nas 3 instâncias)
  // ===========================================================================
  test('Scenario A: rolling graceful shutdown across all 3 instances under live concurrent HTTP financial load', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 10 });
    const nodes = await spawnClusterNodes('scen-a', 3, activeQueues);

    // Confirm initial readiness of all 3 instances:
    for (const node of nodes) {
      const ready = await node.request('/health/ready');
      expect(ready.status).toBe(200);
    }

    const hotWallet = await openWallet(nodes[0]!, '500.00', 'BRL');
    const indWallets = await Promise.all([
      openWallet(nodes[0]!, '300.00', 'BRL'),
      openWallet(nodes[1]!, '300.00', 'BRL'),
      openWallet(nodes[2]!, '300.00', 'BRL'),
    ]);

    const accountant = new OperationLedgerAccountant();
    const shutdownEvidences: GracefulShutdownEvidence[] = [];

    // Seed 1 committed BET per independent wallet so WIN/LOSS/REFUND/ROLLBACK can also run across rounds:
    const seedBets: Array<{ wallet: WalletView; extId: string; roundId: string; idemKey: string; payload: Record<string, unknown> }> = [];
    for (let i = 0; i < indWallets.length; i += 1) {
      const w = indWallets[i]!;
      const roundId = `seed-round-${i}-${newId().slice(0, 6)}`;
      const extId = `seed-bet-${i}-${newId()}`;
      const idemKey = `idem-seed-${i}-${newId()}`;
      const payload = buildWagerPayload(w, {
        externalTransactionId: extId,
        roundId,
        kind: WagerTransactionKind.Bet,
        amount: '20.00',
      });
      const res = await safeSubmitWager(nodes[i]!, payload, idemKey);
      accountant.record(classifyHttpOutcome(`seed-${i}`, idemKey, WagerTransactionKind.Bet, res));
      expect(res.ok && res.res.status).toBe(200);
      seedBets.push({ wallet: w, extId, roundId, idemKey, payload });
    }

    // Repeat graceful shutdown under real load across ALL 3 initial instances sequentially,
    // spawning a replacement node before shutting down the last one so active survivors ALWAYS exist:
    const activePool = [...nodes];
    for (let targetIdx = 0; targetIdx < nodes.length; targetIdx += 1) {
      const target = nodes[targetIdx]!;
      const poolIdx = activePool.indexOf(target);
      if (poolIdx >= 0) {
        activePool.splice(poolIdx, 1);
      }
      if (activePool.length === 0) {
        const replacement = await cluster.spawn({
          instanceId: 'scen-a-standby-4',
          port: 0,
          queues: activeQueues,
        });
        expect((await replacement.request('/health/ready')).status).toBe(200);
        activePool.push(replacement);
      }
      const survivors = [...activePool];

      // Hold hotWallet lock so 2 conflicting operations admitted on `target` stay genuinely in-flight when SIGTERM arrives:
      const lockGate = await holdWalletRowLock(hotWallet.id);

      const hotBetKey = `idem-hot-bet-${targetIdx}-${newId()}`;
      const hotBetPayload = buildWagerPayload(hotWallet, {
        externalTransactionId: `hot-bet-${targetIdx}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '15.00',
      });
      const hotRejectKey = `idem-hot-over-${targetIdx}-${newId()}`;
      const hotRejectPayload = buildWagerPayload(hotWallet, {
        externalTransactionId: `hot-over-${targetIdx}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '99999.00',
      });

      const inFlightHot1 = safeSubmitWager(target, hotBetPayload, hotBetKey);
      const inFlightHot2 = safeSubmitWager(target, hotRejectPayload, hotRejectKey);

      // Deterministically wait until PostgreSQL confirms both requests are admitted & blocked on hotWallet inside `target`:
      await waitForBlockedBackendCount(lockGate.gatePid, 2, target.pgApplicationName);

      // Fire concurrent independent wallet operations + idempotent replays + payload conflict across target & survivors:
      const seedRef = seedBets[targetIdx]!;
      const settlementKind =
        targetIdx === 0
          ? WagerTransactionKind.Win
          : targetIdx === 1
            ? WagerTransactionKind.Refund
            : WagerTransactionKind.Rollback;
      const settlementAmount = targetIdx === 0 ? '35.00' : '20.00';
      const indSettleKey = `idem-ind-settle-${targetIdx}-${newId()}`;
      const indSettlePayload = buildWagerPayload(seedRef.wallet, {
        externalTransactionId: `ind-settle-${targetIdx}-${newId()}`,
        roundId: seedRef.roundId,
        kind: settlementKind,
        amount: settlementAmount,
        referenceExternalTransactionId: seedRef.extId,
      });

      const survivorTarget = survivors[0] ?? target;
      const liveSettlePromise = safeSubmitWager(survivorTarget, indSettlePayload, indSettleKey);
      const validReplayPromise = safeSubmitWager(survivorTarget, seedRef.payload, seedRef.idemKey);
      const conflictPayload = buildWagerPayload(seedRef.wallet, {
        externalTransactionId: `conflict-ext-${targetIdx}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '99.00',
      });
      const conflictPromise = safeSubmitWager(survivorTarget, conflictPayload, seedRef.idemKey);

      // Initiate graceful shutdown on `target` WHILE in-flight operations are held!
      target.sendSignal('SIGTERM');

      // Poll until `target` enters shutdown state and verify `/health/ready` returns 503 with `shuttingDown: true`:
      const readyDuringShutdown = await pollUntil(
        async () => {
          const r = await target.request('/health/ready');
          if (r.status === 503) {
            return { status: r.status, body: (await r.json()) as Record<string, unknown> };
          }
          return undefined;
        },
        { timeoutMs: 3_000, intervalMs: 10, description: `${target.instanceId} /health/ready -> 503` },
      );
      expect(readyDuringShutdown.status).toBe(503);
      expect(readyDuringShutdown.body.shuttingDown).toBe(true);

      // Submit NEW financial operations to `target` while it is shutting down -> MUST be rejected with 503 before touching SQL:
      const postShutdownRejectedKey = `idem-rejected-503-${targetIdx}-${newId()}`;
      const postShutdownPayload = buildWagerPayload(indWallets[targetIdx]!, {
        externalTransactionId: `rejected-503-${targetIdx}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '12.50',
      });
      const postShutdownRes = await safeSubmitWager(target, postShutdownPayload, postShutdownRejectedKey);
      expect(postShutdownRes.ok).toBe(true);
      if (postShutdownRes.ok) {
        expect(postShutdownRes.res.status).toBe(503);
        expect(postShutdownRes.res.body.code).toBe('SERVICE_UNAVAILABLE');
      }
      accountant.record(
        classifyHttpOutcome(`post-sigterm-503-${targetIdx}`, postShutdownRejectedKey, WagerTransactionKind.Bet, postShutdownRes),
      );

      // Confirm zero rows were persisted for the 503-rejected idempotency key:
      const [rejectedRowCheck] = await appSql<Array<{ cnt: number }>>`
        select count(*)::int as cnt from wager_transactions where idempotency_key = ${postShutdownRejectedKey}
      `;
      expect(rejectedRowCheck?.cnt).toBe(0);

      // Release hotWallet lock within cooperative budget so admitted operations drain cleanly:
      await lockGate.release();

      const [hot1Out, hot2Out, settleOut, replayOut, conflictOut] = await Promise.all([
        inFlightHot1,
        inFlightHot2,
        liveSettlePromise,
        validReplayPromise,
        conflictPromise,
      ]);

      expect(hot1Out.ok && hot1Out.res.status).toBe(200);
      expect(hot2Out.ok && hot2Out.res.status).toBe(422);
      expect(settleOut.ok && settleOut.res.status).toBe(200);
      expect(replayOut.ok && replayOut.res.status).toBe(200);
      if (replayOut.ok) {
        expect(replayOut.res.body.idempotentReplay).toBe(true);
      }
      expect(conflictOut.ok && conflictOut.res.status).toBe(409);

      accountant.record(classifyHttpOutcome(`hot-bet-${targetIdx}`, hotBetKey, WagerTransactionKind.Bet, hot1Out));
      accountant.record(classifyHttpOutcome(`hot-over-${targetIdx}`, hotRejectKey, WagerTransactionKind.Bet, hot2Out));
      accountant.record(classifyHttpOutcome(`settle-${targetIdx}`, indSettleKey, settlementKind, settleOut));
      accountant.record(classifyHttpOutcome(`replay-${targetIdx}`, seedRef.idemKey, WagerTransactionKind.Bet, replayOut));
      accountant.record(classifyHttpOutcome(`conflict-${targetIdx}`, seedRef.idemKey, WagerTransactionKind.Bet, conflictOut));

      // Wait for `target` to complete graceful shutdown ONLY after draining admitted work:
      const evidence = await target.stopGracefullyWithEvidence('SIGTERM', 8_000);
      assertValidGracefulEvidence(evidence, 0);
      expect(evidence.drainedHttpRequests).toBeGreaterThanOrEqual(2);
      expect(evidence.rejectedHttpRequests).toBeGreaterThanOrEqual(1);
      shutdownEvidences.push(evidence);

      // Confirm `target`'s PostgreSQL pool sessions are completely closed:
      await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ cnt: number }>>`
            select count(*)::int as cnt from pg_stat_activity
            where datname = current_database() and application_name = ${target.pgApplicationName}
          `;
          return rows[0]?.cnt === 0 ? true : undefined;
        },
        { timeoutMs: 3_000, intervalMs: 20, description: `${target.pgApplicationName} pool closed` },
      );

      // Confirm surviving instances remain healthy and continue serving financial traffic (including retrying the 503 key!):
      if (survivors.length > 0) {
        for (const surv of survivors) {
          const survReady = await surv.request('/health/ready');
          expect(survReady.status).toBe(200);
        }
        const retry503OnSurvivor = await safeSubmitWager(survivors[0]!, postShutdownPayload, postShutdownRejectedKey);
        expect(retry503OnSurvivor.ok && retry503OnSurvivor.res.status).toBe(200);
        accountant.record(
          classifyHttpOutcome(`retry-503-survivor-${targetIdx}`, postShutdownRejectedKey, WagerTransactionKind.Bet, retry503OnSurvivor),
        );
      }
    }

    expect(shutdownEvidences.length).toBe(3);

    const acc = accountant.summary();
    expect(acc.unaccounted).toBe(0);
    expect(acc.committed).toBeGreaterThanOrEqual(11);
    expect(acc.rejectedBeforeFinancialOp).toBeGreaterThanOrEqual(6);
    expect(acc.idempotentReplay).toBe(3);

    const audit = await runIndependentGlobalLedgerAudit(appSql);
    expect(audit.totalFinancialViolations).toBe(0);
    expect(audit.totalWalletsAudited).toBe(4);
  }, 45_000);

  // ===========================================================================
  // CENÁRIO B — Shutdown simultâneo de todo o cluster sob carga
  // ===========================================================================
  test('Scenario B: simultaneous graceful shutdown of all 3 instances under combined HTTP, SQS FIFO, and Outbox load', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 8 });
    const nodes = await spawnClusterNodes('scen-b', 3, activeQueues, {
      shutdownTimeoutMs: 6_000,
    });

    const wallets = await Promise.all(
      Array.from({ length: 6 }, (_u, idx) => openWallet(nodes[idx % 3]!, '300.00', 'BRL')),
    );

    const accountant = new OperationLedgerAccountant();

    // Enqueue 18 SQS FIFO messages across all 6 wallets (3 per wallet):
    const sqsEnvelopes: WagerTransactionRequestedEnvelope[] = [];
    for (const w of wallets) {
      for (let seq = 0; seq < 3; seq += 1) {
        const env = buildSqsEnvelope(w, {
          kind: WagerTransactionKind.Bet,
          amount: '10.00',
        });
        sqsEnvelopes.push(env);
        await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, env)));
      }
    }

    // Hold lock on wallets[0] briefly so HTTP financial requests across all 3 nodes are guaranteed in-flight at simultaneous SIGTERM:
    const lockGate = await holdWalletRowLock(wallets[0]!.id);

    const preShutdownPromises = nodes.map((node, idx) => {
      const idemKey = `idem-sim-inflight-${idx}-${newId()}`;
      const payload = buildWagerPayload(wallets[0]!, {
        externalTransactionId: `sim-inflight-${idx}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '15.00',
      });
      return safeSubmitWager(node, payload, idemKey).then((res) => {
        accountant.record(classifyHttpOutcome(`sim-inflight-${idx}`, idemKey, WagerTransactionKind.Bet, res));
        return res;
      });
    });

    await waitForBlockedBackendCount(lockGate.gatePid, 3);

    // Also launch concurrent HTTP operations on wallets[1..5] right as simultaneous SIGTERM fires:
    const concurrentWavePromises = Array.from({ length: 12 }, (_u, idx) => {
      const targetNode = nodes[idx % 3]!;
      const targetWallet = wallets[(idx % 5) + 1]!;
      const idemKey = `idem-sim-wave-${idx}-${newId()}`;
      const payload = buildWagerPayload(targetWallet, {
        externalTransactionId: `sim-wave-${idx}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '5.00',
      });
      return safeSubmitWager(targetNode, payload, idemKey).then((res) => {
        accountant.record(classifyHttpOutcome(`sim-wave-${idx}`, idemKey, WagerTransactionKind.Bet, res));
        return res;
      });
    });

    // Trigger coordinated simultaneous SIGTERM across ALL 3 processes:
    for (const node of nodes) {
      node.sendSignal('SIGTERM');
    }

    // Continue submitting new operations AFTER SIGTERM is dispatched across all 3 instances:
    const duringShutdownPromises = nodes.map(async (node, idx) => {
      // Wait until the node flips readiness to 503 (or closes socket):
      await pollUntil(
        async () => {
          try {
            const r = await node.request('/health/ready');
            return r.status === 503 ? true : undefined;
          } catch {
            return true;
          }
        },
        { timeoutMs: 2_500, intervalMs: 10, description: `${node.instanceId} entering shutdown` },
      );

      const idemKey = `idem-during-shutdown-${idx}-${newId()}`;
      const payload = buildWagerPayload(wallets[idx + 1]!, {
        externalTransactionId: `during-shutdown-${idx}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '7.00',
      });
      const res = await safeSubmitWager(node, payload, idemKey);
      accountant.record(classifyHttpOutcome(`during-shutdown-${idx}`, idemKey, WagerTransactionKind.Bet, res));
      return res;
    });

    // Release wallets[0] gate well inside the cooperative deadline so admitted operations drain cleanly:
    await lockGate.release();

    const [inFlightResults, _waveResults, duringResults] = await Promise.all([
      Promise.all(preShutdownPromises),
      Promise.all(concurrentWavePromises),
      Promise.all(duringShutdownPromises),
    ]);

    // All 3 pre-shutdown admitted requests MUST have drained to 200 OK:
    for (const r of inFlightResults) {
      expect(r.ok && r.res.status).toBe(200);
    }
    // Requests sent after readiness flipped to 503 MUST be rejected (503 or closed socket), never committed:
    for (const r of duringResults) {
      if (r.ok) {
        expect(r.res.status).toBe(503);
      }
    }

    // Wait for all 3 OS processes to exit cleanly within shutdownTimeoutMs:
    const evidences = await cluster.stopAllGracefullyWithEvidence(8_000);
    expect(evidences.length).toBe(3);
    for (const ev of evidences) {
      assertValidGracefulEvidence(ev, 0);
      expect(ev.elapsedMs).toBeLessThan(6_000);
    }

    // Confirm zero lingering wagering:* PG backends:
    const activePg = await adminSql<Array<{ pid: number }>>`
      select pid::int as pid from pg_stat_activity
      where datname = current_database() and application_name like 'wagering:%'
    `;
    expect([...activePg]).toEqual([]);

    // Confirm zero pre-commit SQS deletions: inbox_messages + remaining SQS messages === 18!
    const [inboxCountRow] = await appSql<Array<{ cnt: number }>>`
      select count(*)::int as cnt from inbox_messages
    `;
    const queueDepthAfterStop = await getQueueDepth(sqs, activeQueues.txQueueUrl);
    const totalQueueRemaining = queueDepthAfterStop.visible + queueDepthAfterStop.notVisible;
    expect((inboxCountRow?.cnt ?? 0) + totalQueueRemaining).toBe(sqsEnvelopes.length);

    const acc = accountant.summary();
    expect(acc.unaccounted).toBe(0);

    // Audit global ledger directly in PostgreSQL while cluster is completely down:
    const auditWhileStopped = await runIndependentGlobalLedgerAudit(appSql);
    expect(auditWhileStopped.totalFinancialViolations).toBe(0);
  }, 45_000);

  // ===========================================================================
  // CENÁRIO C — Shutdown com operações financeiras em voo (todos os 9 pontos)
  // ===========================================================================
  test('Scenario C: deterministic barrier testing across all 9 financial & messaging lifecycle points during shutdown', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 3 });

    // Local programmable HTTP SQS proxy in front of LocalStack for deterministic C6, C7, C8 barriers:
    const upstreamEndpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';
    let sqsProxyInterceptor:
      | ((
          action: string,
          rawBody: string,
          reqUrl: string,
          reqHeaders: Headers,
        ) => Promise<'passthrough' | { status: number; body: string; contentType?: string }>)
      | undefined;

    const sqsProxy = Bun.serve({
      port: 0,
      async fetch(req) {
        const rawBody = await req.text();
        const amzTarget = req.headers.get('x-amz-target') ?? '';
        const actionMatch = /Action=([A-Za-z0-9]+)/.exec(rawBody);
        const action = amzTarget.includes('.')
          ? amzTarget.split('.').pop()!
          : (actionMatch?.[1] ?? rawBody);

        if (sqsProxyInterceptor !== undefined) {
          const decision = await sqsProxyInterceptor(action, rawBody, req.url, req.headers);
          if (decision !== 'passthrough') {
            return new Response(decision.body, {
              status: decision.status,
              headers: { 'Content-Type': decision.contentType ?? 'application/x-amz-json-1.0' },
            });
          }
        }

        const targetUrl = new URL(req.url);
        const upstreamUrl = `${upstreamEndpoint}${targetUrl.pathname}${targetUrl.search}`;
        const forwardHeaders = new Headers(req.headers);
        forwardHeaders.delete('host');
        return fetch(upstreamUrl, {
          method: req.method,
          headers: forwardHeaders,
          body: rawBody.length > 0 ? rawBody : undefined,
        });
      },
    });
    activeCleanupHooks.push(() => sqsProxy.stop(true));
    const proxyEndpointUrl = `http://127.0.0.1:${sqsProxy.port}`;

    // Spawn 1 survivor node that stays alive to verify recovery across subcases:
    const survivor = await cluster.spawn({
      instanceId: 'scen-c-survivor',
      port: 0,
      queues: activeQueues,
      workersEnabled: false,
      shutdownTimeoutMs: 6_000,
    });

    // -------------------------------------------------------------------------
    // POINT C1: HTTP request received (headers accepted), BEFORE SQL TX starts
    // -------------------------------------------------------------------------
    const walletC1 = await openWallet(survivor, '100.00', 'BRL');

    // C1a — Completes within cooperative budget:
    {
      const instC1a = await cluster.spawn({
        instanceId: 'scen-c1a',
        port: 0,
        queues: activeQueues,
        workersEnabled: false,
        shutdownTimeoutMs: 4_000,
      });

      const c1aKey = `idem-c1a-${newId()}`;
      const c1aBody = JSON.stringify(
        buildWagerPayload(walletC1, {
          externalTransactionId: `c1a-${newId()}`,
          kind: WagerTransactionKind.Bet,
          amount: '10.00',
        }),
      );
      const half1 = c1aBody.slice(0, Math.floor(c1aBody.length / 2));
      const half2 = c1aBody.slice(Math.floor(c1aBody.length / 2));

      let resolveFirstChunkFlushed!: () => void;
      const firstChunkFlushed = new Promise<void>((r) => {
        resolveFirstChunkFlushed = r;
      });

      const rawResponsePromise = new Promise<{ statusCode: number; raw: string }>((resolve, reject) => {
        const sock: Socket = createConnection({ host: '127.0.0.1', port: instC1a.port }, () => {
          sock.write(
            `POST /wagering/transactions HTTP/1.1\r\n` +
              `Host: 127.0.0.1:${instC1a.port}\r\n` +
              `Content-Type: application/json\r\n` +
              `Idempotency-Key: ${c1aKey}\r\n` +
              `Transfer-Encoding: chunked\r\n\r\n` +
              `${Buffer.byteLength(half1).toString(16)}\r\n${half1}\r\n`,
            () => resolveFirstChunkFlushed(),
          );
        });
        let buf = '';
        sock.setEncoding('utf8');
        sock.on('data', (chunk) => {
          buf += chunk;
        });
        sock.on('end', () => {
          const statusMatch = /^HTTP\/1\.1 (\d{3})/.exec(buf);
          resolve({ statusCode: Number(statusMatch?.[1] ?? 0), raw: buf });
        });
        sock.on('error', reject);

        // Wait until first chunk is flushed over loopback and accepted by Express onRequestHook, verify 0 SQL queries active, send SIGTERM, then finish chunked upload:
        void (async () => {
          await firstChunkFlushed;
          // Round-trip a /health/live GET on the same loopback server to guarantee prior socket data has already processed through the event loop:
          expect((await instC1a.request('/health/live')).status).toBe(200);

          const [activeSql] = await adminSql<Array<{ cnt: number }>>`
            select count(*)::int as cnt from pg_stat_activity
            where datname = current_database()
              and application_name = ${instC1a.pgApplicationName}
              and state = 'active'
          `;
          expect(activeSql?.cnt ?? 0).toBe(0);

          instC1a.sendSignal('SIGTERM');
          await pollUntil(
            async () => ((await instC1a.request('/health/ready')).status === 503 ? true : undefined),
            { timeoutMs: 2_000, intervalMs: 10 },
          );

          sock.write(`${Buffer.byteLength(half2).toString(16)}\r\n${half2}\r\n0\r\n\r\n`);
        })();
      });

      const c1aHttp = await rawResponsePromise;
      expect(c1aHttp.statusCode).toBe(200);
      const evC1a = await instC1a.stopGracefullyWithEvidence('SIGTERM', 5_000);
      assertValidGracefulEvidence(evC1a, 0);
    }

    // C1b — Body upload stalled past shutdown deadline -> aborted cleanly without partial state:
    {
      const instC1b = await cluster.spawn({
        instanceId: 'scen-c1b',
        port: 0,
        queues: activeQueues,
        workersEnabled: false,
        shutdownTimeoutMs: 1_200,
      });

      const c1bKey = `idem-c1b-${newId()}`;
      const c1bPayload = buildWagerPayload(walletC1, {
        externalTransactionId: `c1b-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '11.00',
      });
      const half1 = JSON.stringify(c1bPayload).slice(0, 24);

      let resolveC1bFirstChunk!: () => void;
      const c1bFirstChunk = new Promise<void>((r) => {
        resolveC1bFirstChunk = r;
      });

      let c1bSock: Socket | undefined;
      const sockClosed = new Promise<void>((resolve) => {
        const sock = createConnection({ host: '127.0.0.1', port: instC1b.port }, () => {
          sock.write(
            `POST /wagering/transactions HTTP/1.1\r\n` +
              `Host: 127.0.0.1:${instC1b.port}\r\n` +
              `Content-Type: application/json\r\n` +
              `Idempotency-Key: ${c1bKey}\r\n` +
              `Transfer-Encoding: chunked\r\n\r\n` +
              `${Buffer.byteLength(half1).toString(16)}\r\n${half1}\r\n`,
            () => resolveC1bFirstChunk(),
          );
        });
        c1bSock = sock;
        sock.resume();
        sock.on('close', () => resolve());
        sock.on('error', () => resolve());
      });
      activeCleanupHooks.push(() => {
        c1bSock?.destroy();
      });

      await c1bFirstChunk;
      expect((await instC1b.request('/health/live')).status).toBe(200);
      instC1b.sendSignal('SIGTERM');

      const evC1b = await instC1b.stopGracefullyWithEvidence('SIGTERM', 4_000);
      c1bSock?.destroy();
      await sockClosed;
      // HTTP drain exceeded budget -> exits with code 1 and ZERO rows written in PostgreSQL:
      expect(evC1b.exitCode).toBe(1);
      expect(evC1b.osProcessAliveAfterExit).toBe(false);
      const [c1bRow] = await appSql<Array<{ cnt: number }>>`
        select count(*)::int as cnt from wager_transactions where idempotency_key = ${c1bKey}
      `;
      expect(c1bRow?.cnt).toBe(0);

      // Recoverable on survivor:
      const recC1b = await safeSubmitWager(survivor, c1bPayload, c1bKey);
      expect(recC1b.ok && recC1b.res.status).toBe(200);
    }

    // -------------------------------------------------------------------------
    // POINT C2: SQL TX started, BEFORE acquiring `wallets FOR UPDATE`
    // -------------------------------------------------------------------------
    const walletC2 = await openWallet(survivor, '100.00', 'BRL');
    {
      // C2a (drains before deadline) + C2b (aborts on statement_timeout rollback):
      const instC2 = await cluster.spawn({
        instanceId: 'scen-c2',
        port: 0,
        queues: activeQueues,
        workersEnabled: false,
        shutdownTimeoutMs: 5_000,
      });

      const gate2a = await holdWalletRowLock(walletC2.id);
      const c2aKey = `idem-c2a-${newId()}`;
      const c2aPayload = buildWagerPayload(walletC2, {
        externalTransactionId: `c2a-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '15.00',
      });
      const p2a = safeSubmitWager(instC2, c2aPayload, c2aKey);
      await waitForBlockedBackendCount(gate2a.gatePid, 1, instC2.pgApplicationName);

      instC2.sendSignal('SIGTERM');
      await gate2a.release();

      const r2a = await p2a;
      expect(r2a.ok && r2a.res.status).toBe(200);
      const evC2a = await instC2.stopGracefullyWithEvidence('SIGTERM', 6_000);
      assertValidGracefulEvidence(evC2a, 0);

      // C2b: SQL TX aborts with atomic ROLLBACK when blocked past hard shutdown deadline (1_200ms):
      const instC2b = await cluster.spawn({
        instanceId: 'scen-c2b',
        port: 0,
        queues: activeQueues,
        workersEnabled: false,
        shutdownTimeoutMs: 1_200,
      });
      const gate2b = await holdWalletRowLock(walletC2.id);
      const c2bKey = `idem-c2b-${newId()}`;
      const c2bPayload = buildWagerPayload(walletC2, {
        externalTransactionId: `c2b-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '12.00',
      });
      const p2b = safeSubmitWager(instC2b, c2bPayload, c2bKey);
      await waitForBlockedBackendCount(gate2b.gatePid, 1, instC2b.pgApplicationName);
      instC2b.sendSignal('SIGTERM');

      const r2b = await p2b;
      expect(!r2b.ok || r2b.res.status === 503).toBe(true);
      const evC2b = await instC2b.stopGracefullyWithEvidence('SIGTERM', 4_000);
      expect(evC2b.exitCode).toBe(1);
      expect(evC2b.osProcessAliveAfterExit).toBe(false);
      await gate2b.release();

      // Wait until PostgreSQL finishes rolling back instC2b's closed backend (<250ms):
      await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ cnt: number }>>`
            select count(*)::int as cnt from pg_stat_activity
            where datname = current_database() and application_name = ${instC2b.pgApplicationName}
          `;
          return rows[0]?.cnt === 0 ? true : undefined;
        },
        { timeoutMs: 3_000, intervalMs: 20, description: 'C2b backend rolled back and closed' },
      );

      const [c2bCheck] = await appSql<Array<{ cnt: number }>>`
        select count(*)::int as cnt from wager_transactions where idempotency_key = ${c2bKey}
      `;
      expect(c2bCheck?.cnt).toBe(0);
      const recC2b = await safeSubmitWager(survivor, c2bPayload, c2bKey);
      expect(recC2b.ok && recC2b.res.status).toBe(200);
    }

    // -------------------------------------------------------------------------
    // POINT C3: `wallets FOR UPDATE` acquired, BEFORE financial mutations
    // -------------------------------------------------------------------------
    const walletC3 = await openWallet(survivor, '100.00', 'BRL');
    const helperWalletC3 = await openWallet(survivor, '50.00', 'BRL');
    {
      const instC3 = await cluster.spawn({
        instanceId: 'scen-c3',
        port: 0,
        queues: activeQueues,
        workersEnabled: false,
        shutdownTimeoutMs: 5_000,
      });

      const c3Key = `idem-c3-${newId()}`;
      let resolveHelperReady!: (pid: number) => void;
      const helperReady = new Promise<number>((r) => {
        resolveHelperReady = r;
      });
      let rollbackHelper!: () => void;
      const helperWait = new Promise<void>((r) => {
        rollbackHelper = r;
      });

      const helperTx = gateSql
        .begin(async (tx) => {
          const [p] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
          await tx`
            insert into wager_transactions (
              id, provider_id, external_transaction_id, idempotency_key, payload_hash,
              wallet_id, player_id, currency, round_id, game_id, kind, amount, status,
              observed_balance_amount, attempt_count, processed_at, created_at
            ) values (
              ${newId()}, 'prov-helper', ${`ext-h-${newId()}`}, ${c3Key}, ${'a'.repeat(64)},
              ${helperWalletC3.id}, ${helperWalletC3.playerId}, 'BRL', 'r-h', 'g-h', 'LOSS', 1.00, 'PROCESSED',
              50.00, 0, clock_timestamp(), clock_timestamp()
            )
          `;
          resolveHelperReady(Number(p!.pid));
          await helperWait;
          throw new GateRollbackSentinel();
        })
        .catch((err) => {
          if (!(err instanceof GateRollbackSentinel)) throw err;
        });

      const helperGatePid = await helperReady;
      let helperDone = false;
      const finishHelper = async () => {
        if (helperDone) return;
        helperDone = true;
        rollbackHelper();
        await helperTx;
      };
      activeCleanupHooks.push(finishHelper);
      const c3Payload = buildWagerPayload(walletC3, {
        externalTransactionId: `c3-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '20.00',
      });
      const p3 = safeSubmitWager(instC3, c3Payload, c3Key);

      // Confirm instC3 is blocked on helperGatePid AFTER acquiring `wallets FOR UPDATE` on walletC3:
      const [blockedC3] = await waitForBlockedBackendCount(helperGatePid, 1, instC3.pgApplicationName);
      const walletLocksHeld = await adminSql<Array<{ cnt: number }>>`
        select count(*)::int as cnt
        from pg_locks l
        join pg_class c on c.oid = l.relation
        where l.pid = ${blockedC3!.pid}
          and c.relname = 'wallets'
          and l.granted = true
      `;
      expect((walletLocksHeld[0]?.cnt ?? 0)).toBeGreaterThanOrEqual(1);

      instC3.sendSignal('SIGTERM');
      await finishHelper();

      const r3 = await p3;
      expect(r3.ok && r3.res.status).toBe(200);
      const evC3 = await instC3.stopGracefullyWithEvidence('SIGTERM', 6_000);
      assertValidGracefulEvidence(evC3, 0);
    }

    // -------------------------------------------------------------------------
    // POINT C4: Financial mutations executed (wallets + tx + ledger), BEFORE COMMIT
    // -------------------------------------------------------------------------
    const walletC4 = await openWallet(survivor, '100.00', 'BRL');
    {
      // C4a: Drains to atomic COMMIT when outbox_messages lock releases within budget:
      const instC4a = await cluster.spawn({
        instanceId: 'scen-c4a',
        port: 0,
        queues: activeQueues,
        workersEnabled: false,
        shutdownTimeoutMs: 5_000,
      });

      let resolveOutboxLockReady!: (pid: number) => void;
      const outboxLockReady = new Promise<number>((r) => {
        resolveOutboxLockReady = r;
      });
      let releaseOutboxLock!: () => void;
      const outboxLockWait = new Promise<void>((r) => {
        releaseOutboxLock = r;
      });

      const outboxLockTx = adminSql.begin(async (tx) => {
        const [p] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
        await tx`lock table outbox_messages in share row exclusive mode`;
        resolveOutboxLockReady(Number(p!.pid));
        await outboxLockWait;
      });

      const outboxGatePid = await outboxLockReady;
      let outboxReleased1 = false;
      const finishOutboxLock1 = async () => {
        if (outboxReleased1) return;
        outboxReleased1 = true;
        releaseOutboxLock();
        await outboxLockTx;
      };
      activeCleanupHooks.push(finishOutboxLock1);
      const c4aKey = `idem-c4a-${newId()}`;
      const c4aPayload = buildWagerPayload(walletC4, {
        externalTransactionId: `c4a-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '25.00',
      });
      const p4a = safeSubmitWager(instC4a, c4aPayload, c4aKey);

      // Wait until instC4a blocks on `insert into outbox_messages` (AFTER updating wallets & inserting ledger!):
      await waitForBlockedBackendCount(outboxGatePid, 1, instC4a.pgApplicationName);

      instC4a.sendSignal('SIGTERM');
      await finishOutboxLock1();

      const r4a = await p4a;
      expect(r4a.ok && r4a.res.status).toBe(200);
      const evC4a = await instC4a.stopGracefullyWithEvidence('SIGTERM', 6_000);
      assertValidGracefulEvidence(evC4a, 0);

      // C4b: Rolls back ALL financial mutations atomically when aborted before COMMIT past hard deadline (1_200ms):
      const instC4b = await cluster.spawn({
        instanceId: 'scen-c4b',
        port: 0,
        queues: activeQueues,
        workersEnabled: false,
        shutdownTimeoutMs: 1_200,
      });

      let resolveOutboxLock2!: (pid: number) => void;
      const outboxLockReady2 = new Promise<number>((r) => {
        resolveOutboxLock2 = r;
      });
      let releaseOutboxLock2!: () => void;
      const outboxLockWait2 = new Promise<void>((r) => {
        releaseOutboxLock2 = r;
      });
      const outboxLockTx2 = adminSql.begin(async (tx) => {
        const [p] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
        await tx`lock table outbox_messages in share row exclusive mode`;
        resolveOutboxLock2(Number(p!.pid));
        await outboxLockWait2;
      });
      const outboxGatePid2 = await outboxLockReady2;
      let outboxReleased2 = false;
      const finishOutboxLock2 = async () => {
        if (outboxReleased2) return;
        outboxReleased2 = true;
        releaseOutboxLock2();
        await outboxLockTx2;
      };
      activeCleanupHooks.push(finishOutboxLock2);

      const c4bKey = `idem-c4b-${newId()}`;
      const c4bPayload = buildWagerPayload(walletC4, {
        externalTransactionId: `c4b-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '30.00',
      });
      const p4b = safeSubmitWager(instC4b, c4bPayload, c4bKey);
      await waitForBlockedBackendCount(outboxGatePid2, 1, instC4b.pgApplicationName);
      instC4b.sendSignal('SIGTERM');

      const r4b = await p4b;
      expect(!r4b.ok || r4b.res.status === 503).toBe(true);
      const evC4b = await instC4b.stopGracefullyWithEvidence('SIGTERM', 4_000);
      expect(evC4b.exitCode).toBe(1);
      expect(evC4b.osProcessAliveAfterExit).toBe(false);
      await finishOutboxLock2();

      await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ cnt: number }>>`
            select count(*)::int as cnt from pg_stat_activity
            where datname = current_database() and application_name = ${instC4b.pgApplicationName}
          `;
          return rows[0]?.cnt === 0 ? true : undefined;
        },
        { timeoutMs: 3_000, intervalMs: 20, description: 'C4b backend rolled back and closed' },
      );

      // Verify walletC4 balance is STILL 75.00 (100 - 25 from C4a; C4b's 30.00 rolled back completely!):
      const [w4AfterRollback] = await appSql<Array<{ balance_amount: string }>>`
        select balance_amount::numeric(19,2)::text as balance_amount from wallets where id = ${walletC4.id}
      `;
      expect(w4AfterRollback?.balance_amount).toBe('75.00');
    }

    // -------------------------------------------------------------------------
    // POINT C5: SQL COMMIT completed, BEFORE HTTP response reaches client
    // -------------------------------------------------------------------------
    const walletC5 = await openWallet(survivor, '100.00', 'BRL');
    {
      const instC5 = await cluster.spawn({
        instanceId: 'scen-c5',
        port: 0,
        queues: activeQueues,
        workersEnabled: false,
        shutdownTimeoutMs: 5_000,
      });

      // Local TCP proxy that intercepts HTTP response bytes from instC5 AFTER PG COMMIT, triggers SIGTERM, and drops client leg:
      const tcpProxy = createNetServer((clientSock) => {
        const upstreamSock = createConnection({ host: '127.0.0.1', port: instC5.port });
        clientSock.pipe(upstreamSock);
        upstreamSock.once('data', () => {
          // Response bytes arrived from instC5 -> SQL COMMIT is already durable!
          instC5.sendSignal('SIGTERM');
          clientSock.destroy();
          upstreamSock.resume();
        });
      });
      await new Promise<void>((r) => tcpProxy.listen(0, '127.0.0.1', () => r()));
      activeCleanupHooks.push(() => new Promise<void>((r) => tcpProxy.close(() => r())));
      const tcpProxyPort = (tcpProxy.address() as { port: number }).port;

      const c5Key = `idem-c5-${newId()}`;
      const c5Payload = buildWagerPayload(walletC5, {
        externalTransactionId: `c5-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '40.00',
      });

      let clientResetObserved = false;
      try {
        await fetch(`http://127.0.0.1:${tcpProxyPort}/wagering/transactions`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Idempotency-Key': c5Key },
          body: JSON.stringify(c5Payload),
        });
      } catch {
        clientResetObserved = true;
      }
      expect(clientResetObserved).toBe(true);

      const evC5 = await instC5.stopGracefullyWithEvidence('SIGTERM', 6_000);
      assertValidGracefulEvidence(evC5, 0);

      // PostgreSQL ALREADY committed C5 -> replay on survivor returns 200 OK with idempotentReplay: true!
      const replayC5 = await safeSubmitWager(survivor, c5Payload, c5Key);
      expect(replayC5.ok && replayC5.res.status).toBe(200);
      if (replayC5.ok) {
        expect(replayC5.res.body.idempotentReplay).toBe(true);
        expect((replayC5.res.body.balance as { amount: string }).amount).toBe('60.00');
      }
    }

    // -------------------------------------------------------------------------
    // POINT C6: SQS message received, BEFORE SQL TX starts
    // -------------------------------------------------------------------------
    const walletC6 = await openWallet(survivor, '100.00', 'BRL');
    {
      let instC6!: RunningServiceInstance;
      let interceptedReceive = false;

      sqsProxyInterceptor = async (action, _rawBody, _reqUrl) => {
        if (!interceptedReceive && action === 'GetQueueAttributes' && instC6 !== undefined) {
          interceptedReceive = true;
          // At GetQueueAttributes, ReceiveMessage has ALREADY returned envC6 (with ReceiptHandle) into memory,
          // and ZERO SQL transactions have started yet (before `for (let index = 0 ...)`):
          instC6.sendSignal('SIGTERM');
          await pollUntil(
            async () => ((await instC6.request('/health/ready')).status === 503 ? true : undefined),
            { timeoutMs: 2_000, intervalMs: 10 },
          );
        }
        return 'passthrough';
      };

      const envC6 = buildSqsEnvelope(walletC6, { kind: WagerTransactionKind.Bet, amount: '15.00' });
      instC6 = await cluster.spawn({
        instanceId: 'scen-c6',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
        envOverrides: { AWS_ENDPOINT_URL: proxyEndpointUrl },
      });

      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envC6)));
      await pollUntil(async () => (interceptedReceive ? true : undefined), {
        timeoutMs: 5_000,
        intervalMs: 15,
        description: 'C6 ReceiveMessage intercepted',
      });

      const evC6 = await instC6.stopGracefullyWithEvidence('SIGTERM', 6_000);
      assertValidGracefulEvidence(evC6, 0);
      expect(evC6.sqsVisibilityReleased).toBeGreaterThanOrEqual(1);
      sqsProxyInterceptor = undefined;

      // Confirm instC6 did NOT start the SQL TX (0 inbox rows for envC6):
      const [c6InboxBefore] = await appSql<Array<{ cnt: number }>>`
        select count(*)::int as cnt from inbox_messages where message_id = ${envC6.messageId}
      `;
      expect(c6InboxBefore?.cnt).toBe(0);

      // Message was immediately released (VisibilityTimeout=0) -> recovery worker consumes it right away!
      const recWorkerC6 = await cluster.spawn({
        instanceId: 'scen-c6-rec',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
      });
      await pollUntil(
        async () => {
          const [r] = await appSql<Array<{ cnt: number }>>`
            select count(*)::int as cnt from inbox_messages where message_id = ${envC6.messageId}
          `;
          return r?.cnt === 1 ? true : undefined;
        },
        { timeoutMs: 4_000, intervalMs: 25, description: 'C6 message recovered immediately' },
      );
      await recWorkerC6.stopGracefullyWithEvidence('SIGTERM', 6_000);
    }

    // -------------------------------------------------------------------------
    // POINT C7: SQS message committed in SQL, BEFORE SQS DeleteMessage ACK
    // -------------------------------------------------------------------------
    const walletC7 = await openWallet(survivor, '100.00', 'BRL');
    {
      let instC7!: RunningServiceInstance;
      let deleteBlocked = false;

      sqsProxyInterceptor = async (action) => {
        if (!deleteBlocked && action === 'DeleteMessage' && instC7 !== undefined) {
          deleteBlocked = true;
          instC7.sendSignal('SIGTERM');
          return {
            status: 500,
            body: JSON.stringify({ __type: 'InternalFailure', message: 'Injected pre-ACK failure during shutdown' }),
          };
        }
        return 'passthrough';
      };

      const envC7 = buildSqsEnvelope(walletC7, { kind: WagerTransactionKind.Bet, amount: '22.00' });
      instC7 = await cluster.spawn({
        instanceId: 'scen-c7',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
        envOverrides: { AWS_ENDPOINT_URL: proxyEndpointUrl },
      });

      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envC7)));
      await pollUntil(async () => (deleteBlocked ? true : undefined), {
        timeoutMs: 5_000,
        intervalMs: 15,
        description: 'C7 DeleteMessage intercepted after SQL COMMIT',
      });

      const evC7 = await instC7.stopGracefullyWithEvidence('SIGTERM', 6_000);
      assertValidGracefulEvidence(evC7, 0);
      sqsProxyInterceptor = undefined;

      // SQL TX already committed (balance = 78.00), while SQS message redelivers after 3s VisibilityTimeout and deduplicates via Inbox!
      const recWorkerC7 = await cluster.spawn({
        instanceId: 'scen-c7-rec',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
      });
      await pollUntil(
        async () => {
          const depth = await getQueueDepth(sqs, activeQueues!.txQueueUrl);
          return depth.visible === 0 && depth.notVisible === 0 ? true : undefined;
        },
        { timeoutMs: 7_000, intervalMs: 50, description: 'C7 redelivered message deduplicated & ACKed' },
      );
      await recWorkerC7.stopGracefullyWithEvidence('SIGTERM', 6_000);

      const [w7After] = await appSql<Array<{ balance_amount: string }>>`
        select balance_amount::numeric(19,2)::text as balance_amount from wallets where id = ${walletC7.id}
      `;
      expect(w7After?.balance_amount).toBe('78.00');
    }

    // -------------------------------------------------------------------------
    // POINT C8: Outbox publication in progress during shutdown
    // -------------------------------------------------------------------------
    const walletC8 = await openWallet(survivor, '100.00', 'BRL');
    {
      // Create unpublished Outbox events via survivor (workersEnabled: false):
      const c8BetRes = await safeSubmitWager(
        survivor,
        buildWagerPayload(walletC8, { externalTransactionId: `c8-${newId()}`, kind: WagerTransactionKind.Bet, amount: '18.00' }),
        `idem-c8-${newId()}`,
      );
      expect(c8BetRes.ok && c8BetRes.res.status).toBe(200);

      let instC8!: RunningServiceInstance;
      let interceptedSendBatch = false;

      sqsProxyInterceptor = async (action) => {
        if (!interceptedSendBatch && action === 'SendMessageBatch' && instC8 !== undefined) {
          interceptedSendBatch = true;
          // Trigger SIGTERM while SendMessageBatch is actively in flight!
          instC8.sendSignal('SIGTERM');
        }
        return 'passthrough';
      };

      instC8 = await cluster.spawn({
        instanceId: 'scen-c8',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        outboxPollIntervalMs: 30,
        envOverrides: { AWS_ENDPOINT_URL: proxyEndpointUrl },
      });

      await pollUntil(async () => (interceptedSendBatch ? true : undefined), {
        timeoutMs: 5_000,
        intervalMs: 15,
        description: 'C8 SendMessageBatch intercepted',
      });

      const evC8 = await instC8.stopGracefullyWithEvidence('SIGTERM', 6_000);
      assertValidGracefulEvidence(evC8, 0);
      sqsProxyInterceptor = undefined;

      // Verify any remaining Outbox events for walletC8 are cleanly recoverable (zero lost events):
      const recOutbox = await cluster.spawn({
        instanceId: 'scen-c8-rec',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        outboxPollIntervalMs: 30,
      });
      await pollUntil(
        async () => {
          const [row] = await appSql<Array<{ cnt: number }>>`
            select count(*)::int as cnt from outbox_messages
            where aggregate_id = ${walletC8.id} and published_at is null
          `;
          return row?.cnt === 0 ? true : undefined;
        },
        { timeoutMs: 5_000, intervalMs: 25, description: 'C8 outbox fully published' },
      );
      await recOutbox.stopGracefullyWithEvidence('SIGTERM', 6_000);
    }

    // -------------------------------------------------------------------------
    // POINT C9: `PENDING_REFERENCE` resolution in progress during shutdown
    // -------------------------------------------------------------------------
    const walletC9a = await openWallet(survivor, '100.00', 'BRL');
    const walletC9b = await openWallet(survivor, '100.00', 'BRL');
    {
      const roundA = `r-c9a-${newId().slice(0, 6)}`;
      const betExtA = `bet-c9a-${newId()}`;
      const roundB = `r-c9b-${newId().slice(0, 6)}`;
      const betExtB = `bet-c9b-${newId()}`;

      // Park WIN on walletC9a and WIN on walletC9b BEFORE their BETs arrive (202 PENDING_REFERENCE):
      const winA = await safeSubmitWager(
        survivor,
        buildWagerPayload(walletC9a, {
          externalTransactionId: `win-c9a-${newId()}`,
          roundId: roundA,
          kind: WagerTransactionKind.Win,
          amount: '30.00',
          referenceExternalTransactionId: betExtA,
        }),
        `idem-win-c9a-${newId()}`,
      );
      const winB = await safeSubmitWager(
        survivor,
        buildWagerPayload(walletC9b, {
          externalTransactionId: `win-c9b-${newId()}`,
          roundId: roundB,
          kind: WagerTransactionKind.Win,
          amount: '40.00',
          referenceExternalTransactionId: betExtB,
        }),
        `idem-win-c9b-${newId()}`,
      );
      expect(winA.ok && winA.res.status).toBe(202);
      expect(winB.ok && winB.res.status).toBe(202);

      // Submit both referenced BETs so both WINs become resolvable once due:
      await safeSubmitWager(
        survivor,
        buildWagerPayload(walletC9a, { externalTransactionId: betExtA, roundId: roundA, kind: WagerTransactionKind.Bet, amount: '10.00' }),
        `idem-bet-c9a-${newId()}`,
      );
      await safeSubmitWager(
        survivor,
        buildWagerPayload(walletC9b, { externalTransactionId: betExtB, roundId: roundB, kind: WagerTransactionKind.Bet, amount: '10.00' }),
        `idem-bet-c9b-${newId()}`,
      );

      // Make both PENDING_REFERENCE rows immediately due (winA ordered ahead of winB):
      await appSql`
        update wager_transactions
        set next_attempt_at = clock_timestamp() - interval '5 seconds'
        where wallet_id = ${walletC9a.id} and status = 'PENDING_REFERENCE'
      `;
      await appSql`
        update wager_transactions
        set next_attempt_at = clock_timestamp() - interval '2 seconds'
        where wallet_id = ${walletC9b.id} and status = 'PENDING_REFERENCE'
      `;

      // Hold walletC9a row lock so PendingReferenceWorker blocks mid-resolution on winA!
      const gate9 = await holdWalletRowLock(walletC9a.id);
      const instC9 = await cluster.spawn({
        instanceId: 'scen-c9',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        pendingReferencePollIntervalMs: 30,
      });

      await waitForBlockedBackendCount(gate9.gatePid, 1, instC9.pgApplicationName);

      // Send SIGTERM while PendingReferenceWorker is mid-resolution on walletC9a, then release gate9:
      instC9.sendSignal('SIGTERM');
      await gate9.release();

      const evC9 = await instC9.stopGracefullyWithEvidence('SIGTERM', 6_000);
      assertValidGracefulEvidence(evC9, 0);

      // winA drained to PROCESSED (100 - 10 + 30 = 120.00), while winB Stopped at loop boundary and recovers on next worker!
      const [w9aRow] = await appSql<Array<{ balance_amount: string }>>`
        select balance_amount::numeric(19,2)::text as balance_amount from wallets where id = ${walletC9a.id}
      `;
      expect(w9aRow?.balance_amount).toBe('120.00');

      const recC9 = await cluster.spawn({
        instanceId: 'scen-c9-rec',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        pendingReferencePollIntervalMs: 30,
      });
      await pollUntil(
        async () => {
          const [w9bRow] = await appSql<Array<{ balance_amount: string }>>`
            select balance_amount::numeric(19,2)::text as balance_amount from wallets where id = ${walletC9b.id}
          `;
          return w9bRow?.balance_amount === '130.00' ? true : undefined;
        },
        { timeoutMs: 5_000, intervalMs: 25, description: 'C9 remaining pending reference recovered' },
      );
      await recC9.stopGracefullyWithEvidence('SIGTERM', 6_000);
    }

    await survivor.stopGracefullyWithEvidence('SIGTERM', 6_000);

    const audit = await runIndependentGlobalLedgerAudit(appSql);
    expect(audit.totalFinancialViolations).toBe(0);
  }, 60_000);

  // ===========================================================================
  // CENÁRIO D — Drenagem de SQS e Outbox durante shutdown
  // ===========================================================================
  test('Scenario D: SQS FIFO multi-group visibility leases, cooperative release, causal order, and Outbox persistence during shutdown', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 4 });
    const nodes = await spawnClusterNodes('scen-d', 3, activeQueues, {
      sqsWaitTimeSeconds: 1,
      outboxPollIntervalMs: 40,
    });

    // 5 wallets -> 5 distinct MessageGroupIds, each receiving an ordered causal 4-step sequence:
    // Step 1: BET 30.00 (r1) -> Step 2: WIN 45.00 (r1) -> Step 3: BET 20.00 (r2) -> Step 4: REFUND 20.00 (r2)
    // Net expected final balance per wallet = 200 - 30 + 45 - 20 + 20 = 215.00, final wallet_version = 5!
    const wallets = await Promise.all(
      Array.from({ length: 5 }, (_u, idx) => openWallet(nodes[idx % 3]!, '200.00', 'BRL')),
    );

    const allEnvelopes: WagerTransactionRequestedEnvelope[] = [];
    for (const w of wallets) {
      const r1 = `rd1-${newId().slice(0, 6)}`;
      const bet1Ext = `d-bet1-${newId()}`;
      const r2 = `rd2-${newId().slice(0, 6)}`;
      const bet2Ext = `d-bet2-${newId()}`;

      const seqMsgs = [
        buildSqsEnvelope(w, { externalTransactionId: bet1Ext, roundId: r1, kind: WagerTransactionKind.Bet, amount: '30.00' }),
        buildSqsEnvelope(w, {
          externalTransactionId: `d-win1-${newId()}`,
          roundId: r1,
          kind: WagerTransactionKind.Win,
          amount: '45.00',
          referenceExternalTransactionId: bet1Ext,
        }),
        buildSqsEnvelope(w, { externalTransactionId: bet2Ext, roundId: r2, kind: WagerTransactionKind.Bet, amount: '20.00' }),
        buildSqsEnvelope(w, {
          externalTransactionId: `d-ref2-${newId()}`,
          roundId: r2,
          kind: WagerTransactionKind.Refund,
          amount: '20.00',
          referenceExternalTransactionId: bet2Ext,
        }),
      ];
      for (const m of seqMsgs) {
        allEnvelopes.push(m);
      }
    }

    // Hold lock on wallets[0] so the consumer owning wallets[0] holds an active lease + queued tail when SIGTERM fires:
    const gate = await holdWalletRowLock(wallets[0]!.id);

    for (const env of allEnvelopes) {
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, env)));
    }

    // Wait until wallets[0]'s first SQS message is actively blocked in PostgreSQL AND other groups have started processing:
    await waitForBlockedBackendCount(gate.gatePid, 1);
    await pollUntil(
      async () => {
        const [r] = await appSql<Array<{ cnt: number }>>`select count(*)::int as cnt from inbox_messages`;
        return (r?.cnt ?? 0) >= 4 ? true : undefined;
      },
      { timeoutMs: 5_000, intervalMs: 20, description: 'partial SQS batch progress across independent groups' },
    );

    // Initiate graceful shutdown across all 3 instances, then release gate within cooperative budget:
    for (const node of nodes) {
      node.sendSignal('SIGTERM');
    }
    await gate.release();

    const evidences = await cluster.stopAllGracefullyWithEvidence(8_000);
    for (const ev of evidences) {
      assertValidGracefulEvidence(ev, 0);
    }

    // Verify invariant: zero pre-commit ACKs (`inbox_messages` + remaining SQS messages === 20):
    const [inboxMid] = await appSql<Array<{ cnt: number }>>`select count(*)::int as cnt from inbox_messages`;
    const midDepth = await getQueueDepth(sqs, activeQueues.txQueueUrl);
    expect((inboxMid?.cnt ?? 0) + midDepth.visible + midDepth.notVisible).toBe(allEnvelopes.length);

    // Spawn recovery nodes to drain remaining released/recoverable SQS messages + unpublished Outbox events:
    const recNodes = await spawnClusterNodes('scen-d-rec', 2, activeQueues, {
      sqsWaitTimeSeconds: 1,
      outboxPollIntervalMs: 30,
    });

    await pollUntil(
      async () => {
        const [ib] = await appSql<Array<{ cnt: number }>>`select count(*)::int as cnt from inbox_messages`;
        const [unpub] = await appSql<Array<{ cnt: number }>>`
          select count(*)::int as cnt from outbox_messages where published_at is null
        `;
        return ib?.cnt === allEnvelopes.length && unpub?.cnt === 0 ? true : undefined;
      },
      { timeoutMs: 12_000, intervalMs: 40, description: 'all 20 SQS FIFO messages and all Outbox events drained' },
    );

    await cluster.stopAllGracefullyWithEvidence(8_000);
    void recNodes;

    // Verify every single wallet ended at exact causal balance 215.00 and version 5 (zero out-of-order FIFO execution!):
    for (const w of wallets) {
      const [row] = await appSql<Array<{ balance_amount: string; version: number }>>`
        select balance_amount::numeric(19,2)::text as balance_amount, version from wallets where id = ${w.id}
      `;
      expect(row?.balance_amount).toBe('215.00');
      expect(row?.version).toBe(5);
    }

    // Drain integration events queue and verify causal sequence per MessageGroupId (walletId):
    const publishedEvents = await drainQueueMessages(sqs, activeQueues.eventsQueueUrl);
    expect(publishedEvents.length).toBeGreaterThanOrEqual(50); // 5 openings*2 + 20 txs*2 = 50 events

    const audit = await runIndependentGlobalLedgerAudit(appSql, {
      expectedInboxMessages: allEnvelopes.map((e) => ({
        messageId: e.messageId,
        idempotencyKey: e.data.idempotencyKey,
      })),
    });
    expect(audit.totalFinancialViolations).toBe(0);
  }, 45_000);

  // ===========================================================================
  // CENÁRIO E — Cold restart completo do cluster da aplicação
  // ===========================================================================
  test('Scenario E: complete cluster cold restart with brand-new INSTANCE_IDs and PIDs preserving PostgreSQL & LocalStack state', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 5 });

    // Generation 1 cluster (3 processes):
    const gen1Nodes = await spawnClusterNodes('cold-gen1', 3, activeQueues, {
      workersEnabled: true,
      outboxPollIntervalMs: 50,
      pendingReferencePollIntervalMs: 50,
    });
    const gen1Ids = gen1Nodes.map((n) => n.instanceId);
    const gen1Pids = gen1Nodes.map((n) => n.pid);

    const wallets = await Promise.all([
      openWallet(gen1Nodes[0]!, '300.00', 'BRL'),
      openWallet(gen1Nodes[1]!, '300.00', 'BRL'),
      openWallet(gen1Nodes[2]!, '300.00', 'BRL'),
    ]);

    // Execute live HTTP operations on Gen 1:
    const round0 = `cold-r0-${newId().slice(0, 6)}`;
    const bet0Ext = `cold-bet0-${newId()}`;
    const rBet0 = await safeSubmitWager(
      gen1Nodes[0]!,
      buildWagerPayload(wallets[0]!, { externalTransactionId: bet0Ext, roundId: round0, kind: WagerTransactionKind.Bet, amount: '40.00' }),
      `idem-cold-bet0-${newId()}`,
    );
    expect(rBet0.ok && rBet0.res.status).toBe(200);

    // Park 2 PENDING_REFERENCE transactions (WIN on wallets[1] and LOSS on wallets[2]) BEFORE their BETs exist:
    const round1 = `cold-r1-${newId().slice(0, 6)}`;
    const lateBet1Ext = `cold-late-bet1-${newId()}`;
    const round2 = `cold-r2-${newId().slice(0, 6)}`;
    const lateBet2Ext = `cold-late-bet2-${newId()}`;

    const parkedWin1 = await safeSubmitWager(
      gen1Nodes[1]!,
      buildWagerPayload(wallets[1]!, {
        externalTransactionId: `cold-win1-${newId()}`,
        roundId: round1,
        kind: WagerTransactionKind.Win,
        amount: '65.00',
        referenceExternalTransactionId: lateBet1Ext,
      }),
      `idem-cold-win1-${newId()}`,
    );
    const parkedLoss2 = await safeSubmitWager(
      gen1Nodes[2]!,
      buildWagerPayload(wallets[2]!, {
        externalTransactionId: `cold-loss2-${newId()}`,
        roundId: round2,
        kind: WagerTransactionKind.Loss,
        amount: '25.00',
        referenceExternalTransactionId: lateBet2Ext,
      }),
      `idem-cold-loss2-${newId()}`,
    );
    expect(parkedWin1.ok && parkedWin1.res.status).toBe(202);
    expect(parkedLoss2.ok && parkedLoss2.res.status).toBe(202);

    // Gracefully shut down ALL 3 Generation 1 instances:
    const gen1ShutdownEvidences = await cluster.stopAllGracefullyWithEvidence(8_000);
    expect(gen1ShutdownEvidences.length).toBe(3);
    for (const ev of gen1ShutdownEvidences) {
      assertValidGracefulEvidence(ev, 0);
    }

    // Confirm ZERO Gen 1 processes remain alive at OS level and ZERO `wagering:*` sessions remain in PostgreSQL:
    expect(cluster.activeInstances().length).toBe(0);
    for (const oldPid of gen1Pids) {
      expect(isOsProcessAlive(oldPid)).toBe(false);
    }
    const pgSessionsDuringColdGap = await adminSql<Array<{ pid: number; application_name: string }>>`
      select pid::int as pid, application_name
      from pg_stat_activity
      where datname = current_database() and application_name like 'wagering:%'
    `;
    expect([...pgSessionsDuringColdGap]).toEqual([]);

    // While ZERO application processes are running, enqueue SQS FIFO messages (including the missing late BETs!)
    // so PostgreSQL + LocalStack hold pending SQS messages, pending Outbox events, and PENDING_REFERENCE rows:
    const coldSqsMessages = [
      buildSqsEnvelope(wallets[1]!, { externalTransactionId: lateBet1Ext, roundId: round1, kind: WagerTransactionKind.Bet, amount: '25.00' }),
      buildSqsEnvelope(wallets[2]!, { externalTransactionId: lateBet2Ext, roundId: round2, kind: WagerTransactionKind.Bet, amount: '25.00' }),
      buildSqsEnvelope(wallets[0]!, { externalTransactionId: `cold-sqs-w0-${newId()}`, roundId: round0, kind: WagerTransactionKind.Win, amount: '90.00', referenceExternalTransactionId: bet0Ext }),
    ];
    for (const m of coldSqsMessages) {
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, m)));
    }

    // Snapshot persisted DB state right before Cold Restart:
    const [preRestartSnapshot] = await appSql<
      Array<{ wallets_cnt: number; tx_cnt: number; pending_ref_cnt: number }>
    >`
      select
        (select count(*)::int from wallets) as wallets_cnt,
        (select count(*)::int from wager_transactions) as tx_cnt,
        (select count(*)::int from wager_transactions where status = 'PENDING_REFERENCE') as pending_ref_cnt
    `;
    expect(preRestartSnapshot?.wallets_cnt).toBe(3);
    expect(preRestartSnapshot?.pending_ref_cnt).toBe(2);

    // Cold Restart: Spawn Generation 2 cluster (3 brand-new OS processes with new INSTANCE_IDs and new PIDs):
    const gen2Nodes = await spawnClusterNodes('cold-gen2', 3, activeQueues, {
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
      outboxPollIntervalMs: 40,
      pendingReferencePollIntervalMs: 40,
    });
    const gen2Ids = gen2Nodes.map((n) => n.instanceId);
    const gen2Pids = gen2Nodes.map((n) => n.pid);

    // Confirm strictly disjoint identities & OS PIDs between Gen 1 and Gen 2:
    for (const id of gen2Ids) {
      expect(gen1Ids).not.toContain(id);
    }
    for (const pid of gen2Pids) {
      expect(gen1Pids).not.toContain(pid);
    }

    // Confirm real readiness of all 3 Gen 2 instances:
    for (const node of gen2Nodes) {
      const ready = await node.request('/health/ready');
      expect(ready.status).toBe(200);
    }

    // Wait for Gen 2 to autonomously recover & drain SQS messages, resolve both PENDING_REFERENCE rows, and publish all Outbox events:
    await pollUntil(
      async () => {
        const [state] = await appSql<
          Array<{ inbox_cnt: number; pending_ref_cnt: number; unpub_outbox_cnt: number }>
        >`
          select
            (select count(*)::int from inbox_messages) as inbox_cnt,
            (select count(*)::int from wager_transactions where status = 'PENDING_REFERENCE') as pending_ref_cnt,
            (select count(*)::int from outbox_messages where published_at is null) as unpub_outbox_cnt
        `;
        if (
          state?.inbox_cnt === coldSqsMessages.length &&
          state?.pending_ref_cnt === 0 &&
          state?.unpub_outbox_cnt === 0
        ) {
          return true;
        }
        return undefined;
      },
      { timeoutMs: 12_000, intervalMs: 50, description: 'Gen 2 cold restart full recovery' },
    );

    // Execute HTTP financial reconciliation across all 3 wallets on Gen 2:
    const expectedFinalBalances = ['350.00', '340.00', '275.00'];
    for (let i = 0; i < wallets.length; i += 1) {
      const recRes = await gen2Nodes[i]!.request(`/wallets/${wallets[i]!.id}/reconciliation`, {
        method: 'POST',
      });
      expect(recRes.status).toBe(200);
      const report = (await recRes.json()) as WalletReconciliation;
      expect(report.consistent).toBe(true);
      expect(report.difference.amount).toBe('0.00');
      expect(report.storedBalance.amount).toBe(expectedFinalBalances[i]!);
      expect(report.calculatedBalance.amount).toBe(expectedFinalBalances[i]!);
    }

    const audit = await runIndependentGlobalLedgerAudit(appSql);
    expect(audit.totalFinancialViolations).toBe(0);
    expect(audit.pendingReferenceTransactionCount).toBe(0);
    expect(audit.pendingOutboxEventCount).toBe(0);
  }, 45_000);

  // ===========================================================================
  // CENÁRIO F — Auditoria global independente do ledger em wagering_test
  // ===========================================================================
  test('Scenario F: independent SQL numeric(19,2) global ledger audit across zero/positive openings, all 5 kinds, non-balance states, Inbox, and Outbox', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 10 });
    const nodes = await spawnClusterNodes('scen-f', 3, activeQueues, {
      workersEnabled: true,
      outboxPollIntervalMs: 40,
      pendingReferencePollIntervalMs: 500,
    });

    // 1. Wallet opened with 0.00 that stays untouched (0 ledger entries, version=1):
    const wZeroIdle = await openWallet(nodes[0]!, '0.00', 'BRL');

    // 2. Wallet opened with 0.00 that receives external movements (first ledger entry starts at wallet_version=2!):
    const wZeroActive = await openWallet(nodes[1]!, '0.00', 'USD');
    // Fund via an external WIN referencing a 0-risk or park? No: WIN requires processed BET!
    // Wait: how can a 0.00 wallet fail a BET? A BET of 10.00 on 0.00 wallet records REJECTED (0 ledger entries, version still 1)!
    const rejZero = await safeSubmitWager(
      nodes[1]!,
      buildWagerPayload(wZeroActive, { externalTransactionId: `f-zero-rej-${newId()}`, kind: WagerTransactionKind.Bet, amount: '10.00', currency: 'USD' }),
      `idem-f-zero-rej-${newId()}`,
    );
    expect(rejZero.ok && rejZero.res.status).toBe(422);

    // 3. Wallet with positive opening + decimal precision edge values (0.10 + 0.20, 0.01) + BET, WIN, LOSS, REFUND, ROLLBACK(BET), ROLLBACK(WIN):
    const wRich = await openWallet(nodes[2]!, '250.75', 'BRL');

    // Round 1: BET 0.10 -> WIN 0.20 -> LOSS 0.00? (LOSS amount must be positive, e.g. 0.10, does not move balance!)
    const r1 = `f-r1-${newId().slice(0, 6)}`;
    const bet1Ext = `f-bet1-${newId()}`;
    expect(
      (await safeSubmitWager(nodes[0]!, buildWagerPayload(wRich, { externalTransactionId: bet1Ext, roundId: r1, kind: WagerTransactionKind.Bet, amount: '0.10' }), `idem-f1-${newId()}`)).ok,
    ).toBe(true);
    expect(
      (await safeSubmitWager(nodes[1]!, buildWagerPayload(wRich, { externalTransactionId: `f-win1-${newId()}`, roundId: r1, kind: WagerTransactionKind.Win, amount: '0.20', referenceExternalTransactionId: bet1Ext }), `idem-f2-${newId()}`)).ok,
    ).toBe(true);
    const lossRes = await safeSubmitWager(
      nodes[2]!,
      buildWagerPayload(wRich, { externalTransactionId: `f-loss1-${newId()}`, roundId: r1, kind: WagerTransactionKind.Loss, amount: '0.10', referenceExternalTransactionId: bet1Ext }),
      `idem-f3-${newId()}`,
    );
    expect(lossRes.ok && lossRes.res.status).toBe(200);

    // Round 2: BET 15.01 -> REFUND 15.01 -> duplicate second REFUND (422 REFERENCE_ALREADY_REVERSED):
    const r2 = `f-r2-${newId().slice(0, 6)}`;
    const bet2Ext = `f-bet2-${newId()}`;
    await safeSubmitWager(nodes[0]!, buildWagerPayload(wRich, { externalTransactionId: bet2Ext, roundId: r2, kind: WagerTransactionKind.Bet, amount: '15.01' }), `idem-f4-${newId()}`);
    await safeSubmitWager(nodes[1]!, buildWagerPayload(wRich, { externalTransactionId: `f-ref2-${newId()}`, roundId: r2, kind: WagerTransactionKind.Refund, amount: '15.01', referenceExternalTransactionId: bet2Ext }), `idem-f5-${newId()}`);
    const dupRefund = await safeSubmitWager(
      nodes[2]!,
      buildWagerPayload(wRich, { externalTransactionId: `f-ref2-dup-${newId()}`, roundId: r2, kind: WagerTransactionKind.Refund, amount: '15.01', referenceExternalTransactionId: bet2Ext }),
      `idem-f6-${newId()}`,
    );
    expect(dupRefund.ok && dupRefund.res.status).toBe(422);

    // Round 3: BET 20.00 -> ROLLBACK of BET 20.00 (CREDIT) AND Round 4: BET 10.00 -> WIN 30.00 -> ROLLBACK of WIN 30.00 (DEBIT):
    const r3 = `f-r3-${newId().slice(0, 6)}`;
    const bet3Ext = `f-bet3-${newId()}`;
    await safeSubmitWager(nodes[0]!, buildWagerPayload(wRich, { externalTransactionId: bet3Ext, roundId: r3, kind: WagerTransactionKind.Bet, amount: '20.00' }), `idem-f7-${newId()}`);
    await safeSubmitWager(nodes[1]!, buildWagerPayload(wRich, { externalTransactionId: `f-rb3-${newId()}`, roundId: r3, kind: WagerTransactionKind.Rollback, amount: '20.00', referenceExternalTransactionId: bet3Ext }), `idem-f8-${newId()}`);

    const r4 = `f-r4-${newId().slice(0, 6)}`;
    const bet4Ext = `f-bet4-${newId()}`;
    const win4Ext = `f-win4-${newId()}`;
    await safeSubmitWager(nodes[2]!, buildWagerPayload(wRich, { externalTransactionId: bet4Ext, roundId: r4, kind: WagerTransactionKind.Bet, amount: '10.00' }), `idem-f9-${newId()}`);
    await safeSubmitWager(nodes[0]!, buildWagerPayload(wRich, { externalTransactionId: win4Ext, roundId: r4, kind: WagerTransactionKind.Win, amount: '30.00', referenceExternalTransactionId: bet4Ext }), `idem-f10-${newId()}`);
    await safeSubmitWager(nodes[1]!, buildWagerPayload(wRich, { externalTransactionId: `f-rb-win4-${newId()}`, roundId: r4, kind: WagerTransactionKind.Rollback, amount: '30.00', referenceExternalTransactionId: win4Ext }), `idem-f11-${newId()}`);

    // 4. Wallet with active recoverable PENDING_REFERENCE (missing BET):
    const wPending = await openWallet(nodes[0]!, '80.00', 'BRL');
    const pendRes = await safeSubmitWager(
      nodes[1]!,
      buildWagerPayload(wPending, {
        externalTransactionId: `f-pend-win-${newId()}`,
        roundId: `f-rp-${newId().slice(0, 6)}`,
        kind: WagerTransactionKind.Win,
        amount: '50.00',
        referenceExternalTransactionId: `missing-bet-${newId()}`,
      }),
      `idem-f-pend-${newId()}`,
    );
    expect(pendRes.ok && pendRes.res.status).toBe(202);

    // 5. SQS FIFO valid message + business_conflict message (CURRENCY_MISMATCH):
    const wSqs = await openWallet(nodes[2]!, '120.00', 'BRL');
    const validEnv = buildSqsEnvelope(wSqs, { kind: WagerTransactionKind.Bet, amount: '19.99' });
    const conflictEnv = buildSqsEnvelope(wSqs, { kind: WagerTransactionKind.Bet, amount: '5.00', currency: 'EUR' });

    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, validEnv)));
    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, conflictEnv)));

    await pollUntil(
      async () => {
        const [ib] = await appSql<Array<{ cnt: number }>>`select count(*)::int as cnt from inbox_messages`;
        const [unpub] = await appSql<Array<{ cnt: number }>>`select count(*)::int as cnt from outbox_messages where published_at is null`;
        return ib?.cnt === 2 && unpub?.cnt === 0 ? true : undefined;
      },
      { timeoutMs: 8_000, intervalMs: 30, description: 'Scenario F SQS & Outbox settled' },
    );

    void wZeroIdle;

    // Run independent SQL-first Global Ledger Audit directly on wagering_test:
    const report: GlobalLedgerAuditReport = await runIndependentGlobalLedgerAudit(appSql, {
      expectedInboxMessages: [
        { messageId: validEnv.messageId, idempotencyKey: validEnv.data.idempotencyKey },
        { messageId: conflictEnv.messageId, idempotencyKey: conflictEnv.data.idempotencyKey, expectBusinessConflict: true },
      ],
    });

    expect(report.databaseName).toBe('wagering_test');
    expect(report.totalWalletsAudited).toBe(5);
    expect(report.totalTransactionsExamined).toBeGreaterThanOrEqual(15);
    expect(report.totalLedgerEntriesExamined).toBeGreaterThanOrEqual(10);
    expect(report.totalInboxRecordsExamined).toBe(2);
    expect(report.totalOutboxEventsExamined).toBeGreaterThanOrEqual(25);
    expect(report.pendingReferenceTransactionCount).toBe(1);
    expect(report.balanceDivergenceCount).toBe(0);
    expect(report.versionGapOrSequenceViolationCount).toBe(0);
    expect(report.duplicateViolationCount).toBe(0);
    expect(report.referentialInconsistencyCount).toBe(0);
    expect(report.payloadHashMismatchCount).toBe(0);
    expect(report.inboxViolationCount).toBe(0);
    expect(report.outboxViolationCount).toBe(0);
    expect(report.totalFinancialViolations).toBe(0);

    // Verify wRich exact decimal arithmetic: 250.75 - 0.10 + 0.20 - 15.01 + 15.01 - 20.00 + 20.00 - 10.00 + 30.00 - 30.00 = 240.85!
    const [wRichDb] = await appSql<Array<{ balance_amount: string }>>`
      select balance_amount::numeric(19,2)::text as balance_amount from wallets where id = ${wRich.id}
    `;
    expect(wRichDb?.balance_amount).toBe(new Decimal('250.75').minus('0.10').plus('0.20').minus('10.00').toFixed(2));
  }, 40_000);

  // ===========================================================================
  // CENÁRIO G — Carga concorrente, shutdown, cold restart e recuperação E2E
  // ===========================================================================
  test('Scenario G: end-to-end concurrent mixed workload, lock contention, graceful shutdown, cold restart, recovery, and global ledger audit', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 5 });

    // Generation 1 cluster (3 real OS processes):
    const gen1 = await spawnClusterNodes('e2e-gen1', 3, activeQueues, {
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
      outboxPollIntervalMs: 40,
      pendingReferencePollIntervalMs: 40,
      shutdownTimeoutMs: 6_000,
    });
    const gen1Ids = gen1.map((n) => n.instanceId);
    const gen1Pids = gen1.map((n) => n.pid);

    const wallets = await Promise.all([
      openWallet(gen1[0]!, '500.00', 'BRL'),
      openWallet(gen1[1]!, '500.00', 'BRL'),
      openWallet(gen1[2]!, '500.00', 'USD'),
      openWallet(gen1[0]!, '400.00', 'BRL'),
      openWallet(gen1[1]!, '400.00', 'USD'),
      openWallet(gen1[2]!, '0.00', 'BRL'),
    ]);

    const accountant = new OperationLedgerAccountant();
    const retryableHttpSpecs: Array<{ payload: Record<string, unknown>; idemKey: string; kind: WagerTransactionKind; opId: string }> = [];

    // 1. Seed base rounds across wallets[0..4] (BETs) so WIN, LOSS, REFUND, ROLLBACK can run concurrently:
    const baseRounds = await Promise.all(
      wallets.slice(0, 5).map(async (w, idx) => {
        const roundId = `e2e-r-${idx}-${newId().slice(0, 6)}`;
        const betExt = `e2e-base-bet-${idx}-${newId()}`;
        const idemKey = `idem-e2e-base-${idx}-${newId()}`;
        const payload = buildWagerPayload(w, {
          externalTransactionId: betExt,
          roundId,
          kind: WagerTransactionKind.Bet,
          amount: '30.00',
        });
        const res = await safeSubmitWager(gen1[idx % 3]!, payload, idemKey);
        expect(res.ok && res.res.status).toBe(200);
        accountant.record(classifyHttpOutcome(`base-bet-${idx}`, idemKey, WagerTransactionKind.Bet, res));
        return { wallet: w, roundId, betExt, idemKey, payload };
      }),
    );

    // 2. Park 2 out-of-order PENDING_REFERENCE transactions (WIN & REFUND whose BETs will arrive via SQS!):
    const oooRound1 = `e2e-ooo1-${newId().slice(0, 6)}`;
    const oooBet1Ext = `e2e-ooo-bet1-${newId()}`;
    const oooRound2 = `e2e-ooo2-${newId().slice(0, 6)}`;
    const oooBet2Ext = `e2e-ooo-bet2-${newId()}`;

    const oooWinKey = `idem-e2e-ooo-win-${newId()}`;
    const oooWinRes = await safeSubmitWager(
      gen1[0]!,
      buildWagerPayload(wallets[0]!, {
        externalTransactionId: `e2e-ooo-win-${newId()}`,
        roundId: oooRound1,
        kind: WagerTransactionKind.Win,
        amount: '55.00',
        referenceExternalTransactionId: oooBet1Ext,
      }),
      oooWinKey,
    );
    expect(oooWinRes.ok && oooWinRes.res.status).toBe(202);
    accountant.record(classifyHttpOutcome('ooo-win', oooWinKey, WagerTransactionKind.Win, oooWinRes));

    const oooRefKey = `idem-e2e-ooo-ref-${newId()}`;
    const oooRefRes = await safeSubmitWager(
      gen1[1]!,
      buildWagerPayload(wallets[1]!, {
        externalTransactionId: `e2e-ooo-ref-${newId()}`,
        roundId: oooRound2,
        kind: WagerTransactionKind.Refund,
        amount: '25.00',
        referenceExternalTransactionId: oooBet2Ext,
      }),
      oooRefKey,
    );
    expect(oooRefRes.ok && oooRefRes.res.status).toBe(202);
    accountant.record(classifyHttpOutcome('ooo-ref', oooRefKey, WagerTransactionKind.Refund, oooRefRes));

    // 3. Enqueue 12 SQS FIFO messages (including the 2 missing BETs for PENDING_REFERENCE + multi-wallet BET/LOSS/WIN):
    const sqsMessages: WagerTransactionRequestedEnvelope[] = [
      buildSqsEnvelope(wallets[0]!, { externalTransactionId: oooBet1Ext, roundId: oooRound1, kind: WagerTransactionKind.Bet, amount: '20.00' }),
      buildSqsEnvelope(wallets[1]!, { externalTransactionId: oooBet2Ext, roundId: oooRound2, kind: WagerTransactionKind.Bet, amount: '25.00' }),
    ];
    for (let i = 0; i < 10; i += 1) {
      const targetW = wallets[i % 5]!;
      sqsMessages.push(
        buildSqsEnvelope(targetW, {
          externalTransactionId: `e2e-sqs-${i}-${newId()}`,
          kind: WagerTransactionKind.Bet,
          amount: '5.00',
        }),
      );
    }

    // 4. Create real lock contention on wallets[0] while firing mixed HTTP wave + SQS messages + graceful cluster shutdown:
    const contentionGate = await holdWalletRowLock(wallets[0]!.id);

    for (const msg of sqsMessages) {
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, msg)));
    }

    // Launch mixed HTTP wave (WIN, LOSS, REFUND, ROLLBACK, BETs on contended wallets[0], replays, conflicts):
    const mixedHttpTasks: Array<Promise<void>> = [];

    const settlements: Array<{ baseIdx: number; kind: WagerTransactionKind; amount: string }> = [
      { baseIdx: 0, kind: WagerTransactionKind.Win, amount: '45.00' },
      { baseIdx: 1, kind: WagerTransactionKind.Loss, amount: '30.00' },
      { baseIdx: 2, kind: WagerTransactionKind.Refund, amount: '30.00' },
      { baseIdx: 3, kind: WagerTransactionKind.Rollback, amount: '30.00' },
    ];

    for (const s of settlements) {
      const base = baseRounds[s.baseIdx]!;
      const idemKey = `idem-e2e-settle-${s.baseIdx}-${newId()}`;
      const payload = buildWagerPayload(base.wallet, {
        externalTransactionId: `e2e-settle-${s.baseIdx}-${newId()}`,
        roundId: base.roundId,
        kind: s.kind,
        amount: s.amount,
        referenceExternalTransactionId: base.betExt,
      });
      const opId = `settle-${s.baseIdx}`;
      mixedHttpTasks.push(
        safeSubmitWager(gen1[s.baseIdx % 3]!, payload, idemKey).then((res) => {
          const classified = classifyHttpOutcome(opId, idemKey, s.kind, res);
          accountant.record(classified);
          if (classified.category === 'rejectedBeforeFinancialOp' && classified.httpStatus === 503) {
            retryableHttpSpecs.push({ payload, idemKey, kind: s.kind, opId });
          } else if (classified.category === 'knownRecoverableFailure') {
            retryableHttpSpecs.push({ payload, idemKey, kind: s.kind, opId });
          }
        }),
      );
    }

    // Valid idempotent replays & payload conflicts during active load:
    for (let i = 0; i < 3; i += 1) {
      const base = baseRounds[i]!;
      mixedHttpTasks.push(
        safeSubmitWager(gen1[i]!, base.payload, base.idemKey).then((res) => {
          accountant.record(classifyHttpOutcome(`e2e-replay-${i}`, base.idemKey, WagerTransactionKind.Bet, res));
        }),
      );
      const badConflictPayload = buildWagerPayload(base.wallet, {
        externalTransactionId: `e2e-conflict-${i}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '77.00',
      });
      mixedHttpTasks.push(
        safeSubmitWager(gen1[(i + 1) % 3]!, badConflictPayload, base.idemKey).then((res) => {
          accountant.record(classifyHttpOutcome(`e2e-conflict-${i}`, base.idemKey, WagerTransactionKind.Bet, res));
        }),
      );
    }

    // Wait until contentionGate confirms real lock contention on wallets[0], then trigger graceful shutdown of Gen 1:
    await waitForBlockedBackendCount(contentionGate.gatePid, 1);

    for (const node of gen1) {
      node.sendSignal('SIGTERM');
    }

    // Also send 3 late HTTP requests during shutdown transition (to capture 503 -> recovered on Gen 2):
    for (let i = 0; i < 3; i += 1) {
      const node = gen1[i]!;
      const idemKey = `idem-e2e-late-${i}-${newId()}`;
      const payload = buildWagerPayload(wallets[4]!, {
        externalTransactionId: `e2e-late-${i}-${newId()}`,
        kind: WagerTransactionKind.Bet,
        amount: '6.00',
      });
      const opId = `late-${i}`;
      mixedHttpTasks.push(
        safeSubmitWager(node, payload, idemKey).then((res) => {
          const classified = classifyHttpOutcome(opId, idemKey, WagerTransactionKind.Bet, res);
          accountant.record(classified);
          if (classified.category !== 'committed' && classified.category !== 'idempotentReplay') {
            retryableHttpSpecs.push({ payload, idemKey, kind: WagerTransactionKind.Bet, opId });
          }
        }),
      );
    }

    // Release contention gate within cooperative budget so admitted contended operations drain cleanly:
    await contentionGate.release();
    await Promise.all(mixedHttpTasks);

    const gen1Evidences = await cluster.stopAllGracefullyWithEvidence(8_000);
    expect(gen1Evidences.length).toBe(3);
    for (const ev of gen1Evidences) {
      assertValidGracefulEvidence(ev, 0);
    }

    // Confirm total cold boundary: 0 Gen 1 OS processes alive, 0 `wagering:*` PostgreSQL sessions:
    for (const pid of gen1Pids) {
      expect(isOsProcessAlive(pid)).toBe(false);
    }
    const coldSessions = await adminSql<Array<{ pid: number }>>`
      select pid::int as pid from pg_stat_activity
      where datname = current_database() and application_name like 'wagering:%'
    `;
    expect([...coldSessions]).toEqual([]);

    // Cold Restart: Spawn Generation 2 cluster (3 brand-new processes):
    const gen2 = await spawnClusterNodes('e2e-gen2', 3, activeQueues, {
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
      outboxPollIntervalMs: 40,
      pendingReferencePollIntervalMs: 40,
      shutdownTimeoutMs: 6_000,
    });
    for (const n of gen2) {
      expect(gen1Ids).not.toContain(n.instanceId);
      expect(gen1Pids).not.toContain(n.pid);
    }

    // Replay any 503/interrupted HTTP operations on Generation 2:
    for (let i = 0; i < retryableHttpSpecs.length; i += 1) {
      const spec = retryableHttpSpecs[i]!;
      const retryRes = await safeSubmitWager(gen2[i % 3]!, spec.payload, spec.idemKey);
      expect(retryRes.ok && (retryRes.res.status === 200 || retryRes.res.status === 202)).toBe(true);
      accountant.record(classifyHttpOutcome(`recovered-${spec.opId}`, spec.idemKey, spec.kind, retryRes));
    }

    // Wait until ALL SQS messages, ALL PENDING_REFERENCE transactions, and ALL Outbox events settle:
    await pollUntil(
      async () => {
        const [st] = await appSql<
          Array<{ inbox_cnt: number; pending_ref_cnt: number; unpub_outbox_cnt: number }>
        >`
          select
            (select count(*)::int from inbox_messages) as inbox_cnt,
            (select count(*)::int from wager_transactions where status = 'PENDING_REFERENCE') as pending_ref_cnt,
            (select count(*)::int from outbox_messages where published_at is null) as unpub_outbox_cnt
        `;
        const qDepth = await getQueueDepth(sqs, activeQueues!.txQueueUrl);
        if (
          st?.inbox_cnt === sqsMessages.length &&
          st?.pending_ref_cnt === 0 &&
          st?.unpub_outbox_cnt === 0 &&
          qDepth.visible === 0 &&
          qDepth.notVisible === 0
        ) {
          return true;
        }
        return undefined;
      },
      { timeoutMs: 15_000, intervalMs: 50, description: 'E2E Scenario G full settlement after cold restart' },
    );

    // Reconcile every wallet via HTTP + run independent SQL Global Ledger Audit:
    for (let i = 0; i < wallets.length; i += 1) {
      const rec = await gen2[i % 3]!.request(`/wallets/${wallets[i]!.id}/reconciliation`, {
        method: 'POST',
      });
      expect(rec.status).toBe(200);
      const body = (await rec.json()) as WalletReconciliation;
      expect(body.consistent).toBe(true);
      expect(body.difference.amount).toBe('0.00');
    }

    const acc = accountant.summary();
    expect(acc.unaccounted).toBe(0);

    const finalAudit = await runIndependentGlobalLedgerAudit(appSql, {
      expectedInboxMessages: sqsMessages.map((m) => ({
        messageId: m.messageId,
        idempotencyKey: m.data.idempotencyKey,
      })),
    });
    expect(finalAudit.totalWalletsAudited).toBe(6);
    expect(finalAudit.pendingReferenceTransactionCount).toBe(0);
    expect(finalAudit.pendingOutboxEventCount).toBe(0);
    expect(finalAudit.totalFinancialViolations).toBe(0);

    const gen2Evidences = await cluster.stopAllGracefullyWithEvidence(8_000);
    expect(gen2Evidences.length).toBe(3);
    for (const ev of gen2Evidences) {
      assertValidGracefulEvidence(ev, 0);
    }
  }, 60_000);
});
