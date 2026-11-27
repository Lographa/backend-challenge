import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import {
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import type { SQL } from 'bun';
import Decimal from 'decimal.js';
import { newId } from '../../src/common/ids';
import { INSTANCE_ID_HEADER } from '../../src/common/instance/instance-identity';
import {
  toWagerTransactionSendInput,
  WAGER_CONSUMER_NAME,
  WAGER_REQUESTED_MESSAGE_TYPE,
  type WagerTransactionRequestedEnvelope,
} from '../../src/messaging/wager-message';
import { WagerTransactionKind, WagerTransactionStatus } from '../../src/wagering/wager-transaction';
import type {
  SubmitWagerResponseView,
  WagerTransactionView,
} from '../../src/wagering/wagering.controller';
import { LedgerDirection } from '../../src/wallets/wallet-ledger-entry';
import type { WalletReconciliation } from '../../src/wallets/wallet-reconciliation';
import type { WalletView } from '../../src/wallets/wallets.controller';
import { createTestOrm, createTestSql, resetSchema, testRoleName } from '../support/database';
import {
  type AbruptKillEvidence,
  createClusterQueues,
  createTestSqsClient,
  drainQueueMessages,
  getQueueDepth,
  type IsolatedClusterQueues,
  isOsProcessAlive,
  MultiProcessCluster,
  pollUntil,
  type RunningServiceInstance,
} from '../support/multi-process';

class GateRollbackSentinel extends Error {
  constructor() {
    super('Intentional rollback of test idempotency-key barrier');
  }
}

interface HttpJsonResult<T = SubmitWagerResponseView> {
  readonly status: number;
  readonly instanceIdHeader: string | null;
  readonly body: T;
}

interface DbWalletRow {
  id: string;
  balance_amount: string;
  currency: string;
  version: number;
}

interface DbLedgerRow {
  id: string;
  wallet_id: string;
  transaction_id: string;
  direction: string;
  amount: string;
  currency: string;
  balance_before: string;
  balance_after: string;
  wallet_version: number;
}

interface DbWagerTxRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  kind: string;
  amount: string;
  status: string;
  failure_code: string | null;
  observed_balance_amount: string | null;
  processed_at: Date | string | null;
  next_attempt_at: Date | string | null;
}

describe('Abrupt OS process crash & recovery (Stage 7.4 — wagering_test + LocalStack + real TerminateProcess/SIGKILL)', () => {
  let migratorOrm: MikroORM;
  let adminSql: SQL;
  let appSql: SQL;
  let gateSql: SQL;
  let sqs: SQSClient;
  let cluster: MultiProcessCluster;
  let activeQueues: IsolatedClusterQueues | undefined;
  const activeProxyStops: Array<() => void> = [];

  beforeAll(async () => {
    migratorOrm = await createTestOrm('migrator');
    adminSql = createTestSql('admin');
    appSql = createTestSql('app');
    gateSql = createTestSql('app');
    sqs = createTestSqsClient();
    cluster = new MultiProcessCluster();

    // Explicit target database and role confirmation before any destructive crash test:
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
    while (activeProxyStops.length > 0) {
      const stop = activeProxyStops.pop();
      stop?.();
    }

    await cluster.stopAllGracefully().catch(async (err) => {
      await cluster.forceKillAll();
      throw err;
    });

    if (activeQueues !== undefined) {
      await activeQueues.destroy();
      activeQueues = undefined;
    }

    // Confirm zero lingering PostgreSQL sessions from any crashed or surviving instance:
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
      { timeoutMs: 4_000, intervalMs: 25, description: 'zero lingering wagering:* PostgreSQL sessions' },
    );
    expect([...lingering]).toEqual([]);
  });

  afterAll(async () => {
    while (activeProxyStops.length > 0) {
      activeProxyStops.pop()?.();
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
        }),
      ),
    );

    const pids = new Set(nodes.map((n) => n.pid));
    const ports = new Set(nodes.map((n) => n.port));
    expect(pids.size).toBe(count);
    expect(ports.size).toBe(count);
    return nodes;
  }

  async function openWallet(
    instance: RunningServiceInstance,
    amount = '100.00',
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

  async function submitWagerHttp<T = SubmitWagerResponseView>(
    instance: RunningServiceInstance,
    payload: Record<string, unknown>,
    idempotencyKey: string,
  ): Promise<HttpJsonResult<T>> {
    const res = await instance.request('/wagering/transactions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      body: JSON.stringify(payload),
    });
    return {
      status: res.status,
      instanceIdHeader: res.headers.get(INSTANCE_ID_HEADER),
      body: (await res.json()) as T,
    };
  }

  function buildWagerPayload(
    wallet: WalletView,
    overrides: Partial<{
      providerId: string;
      externalTransactionId: string;
      kind: WagerTransactionKind;
      amount: string;
      currency: string;
      roundId: string;
      gameId: string;
      referenceExternalTransactionId: string;
    }> = {},
  ): Record<string, unknown> {
    return {
      providerId: overrides.providerId ?? 'provider-74',
      externalTransactionId: overrides.externalTransactionId ?? `ext-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: overrides.roundId ?? `round-${newId().slice(0, 8)}`,
      gameId: overrides.gameId ?? 'crash-slots',
      kind: overrides.kind ?? WagerTransactionKind.Bet,
      money: {
        amount: overrides.amount ?? '25.00',
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
      kind: WagerTransactionKind;
      amount: string;
      roundId: string;
      referenceExternalTransactionId: string;
    }> = {},
  ): WagerTransactionRequestedEnvelope {
    const extId = overrides.externalTransactionId ?? `ext-${newId()}`;
    return {
      messageId: overrides.messageId ?? `msg-${newId()}`,
      type: WAGER_REQUESTED_MESSAGE_TYPE,
      occurredAt: new Date().toISOString(),
      correlationId: `corr-${newId().slice(0, 12)}`,
      data: {
        providerId: overrides.providerId ?? 'provider-74',
        externalTransactionId: extId,
        idempotencyKey: overrides.idempotencyKey ?? `idem-${extId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: overrides.roundId ?? `round-${newId().slice(0, 8)}`,
        gameId: 'crash-slots',
        kind: overrides.kind ?? WagerTransactionKind.Bet,
        money: {
          amount: overrides.amount ?? '25.00',
          currency: wallet.balance.currency,
        },
        ...(overrides.referenceExternalTransactionId !== undefined
          ? { referenceExternalTransactionId: overrides.referenceExternalTransactionId }
          : {}),
      },
    };
  }

  /**
   * Holds an uncommitted `idempotency_key` index entry on `helperWallet` inside `gateSql`,
   * causing any process executing a wager with `targetIdempotencyKey` on `targetWallet` to:
   * 1. Enter `BEGIN`,
   * 2. Acquire `inbox_messages FOR UPDATE` (if SQS) + `wallets FOR UPDATE` on `targetWallet`,
   * 3. Deterministically pause on `wager_transactions_idempotency_key_uq` BEFORE `COMMIT`!
   * Calling `rollback()` aborts the helper insert cleanly without persisting anything.
   */
  async function acquireUncommittedIdempotencyBarrier(
    helperWallet: WalletView,
    targetIdempotencyKey: string,
  ): Promise<{ readonly gatePid: number; readonly rollback: () => Promise<void> }> {
    let resolveLocked!: (pid: number) => void;
    let rejectLocked!: (err: unknown) => void;
    const lockedPromise = new Promise<number>((resolve, reject) => {
      resolveLocked = resolve;
      rejectLocked = reject;
    });

    let triggerRollback!: () => void;
    const holdPromise = new Promise<void>((resolve) => {
      triggerRollback = resolve;
    });

    const txTask = gateSql
      .begin(async (tx) => {
        const [pidRow] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
        await tx`
          insert into wager_transactions (
            id, provider_id, external_transaction_id, idempotency_key, payload_hash,
            wallet_id, player_id, round_id, game_id, kind, amount, currency,
            reference_external_transaction_id, reference_transaction_id,
            status, failure_code, observed_balance_amount, attempt_count,
            next_attempt_at, processed_at, created_at
          ) values (
            ${newId()}, 'provider-gate', ${`gate-ext-${newId()}`}, ${targetIdempotencyKey}, ${'a'.repeat(64)},
            ${helperWallet.id}, ${helperWallet.playerId}, 'round-gate', 'game-gate', 'LOSS', '0.00', ${helperWallet.balance.currency},
            null, null,
            'PROCESSED', null, ${helperWallet.balance.amount}, 0,
            null, clock_timestamp(), clock_timestamp()
          )
        `;
        resolveLocked(pidRow!.pid);
        await holdPromise;
        throw new GateRollbackSentinel();
      })
      .catch((err) => {
        if (err instanceof GateRollbackSentinel) {
          return;
        }
        rejectLocked(err);
        throw err;
      });

    const gatePid = await lockedPromise;
    return {
      gatePid,
      rollback: async () => {
        triggerRollback();
        await txTask;
      },
    };
  }

  /**
   * Verifies real OS abrupt termination (`SIGKILL` / `TerminateProcess` without graceful hooks)
   * and confirms PostgreSQL closed the dead process's sessions and released all locks.
   */
  async function assertAbruptDeathAndPgCleanup(evidence: AbruptKillEvidence): Promise<void> {
    expect(evidence.pid).toBeGreaterThan(0);
    expect(evidence.hadGracefulShutdownLogs).toBe(false);
    expect(evidence.osProcessAliveAfterKill).toBe(false);
    expect(isOsProcessAlive(evidence.pid)).toBe(false);
    if (evidence.signalCode === null) {
      expect(evidence.exitCode).not.toBe(0);
    }

    await pollUntil(
      async () => {
        const [sessRows, lockRows] = await Promise.all([
          adminSql<Array<{ pid: number }>>`
            select pid::int as pid
            from pg_stat_activity
            where datname = current_database()
              and application_name = ${evidence.pgApplicationName}
          `,
          adminSql<Array<{ pid: number }>>`
            select l.pid::int as pid
            from pg_locks l
            join pg_stat_activity a on a.pid = l.pid
            where a.datname = current_database()
              and a.application_name = ${evidence.pgApplicationName}
          `,
        ]);
        return sessRows.length === 0 && lockRows.length === 0 ? true : undefined;
      },
      {
        timeoutMs: 3_000,
        intervalMs: 15,
        description: `PostgreSQL session & lock cleanup for abruptly killed ${evidence.pgApplicationName} (pid=${evidence.pid})`,
      },
    );
  }

  /**
   * Exhaustive post-recovery audit directly on `wagering_test` + LocalStack SQS FIFO:
   * - `balance >= 0` & `storedBalance === calculatedBalance` (SQL + `/reconciliation`)
   * - Contiguous `wallet_version` (`1..N`) and `balance_before`/`balance_after` chain
   * - 1:1 `transaction_id` mapping between balance-affecting `PROCESSED` rows and ledger entries
   * - Zero partially persisted transactions
   * - Outbox `published_at IS NOT NULL` & per-wallet causal `sequence_number` order in SQS
   * - Zero residual messages in `txQueueUrl` and `dlqUrl`
   * - Zero orphan PostgreSQL locks or blocked queries
   */
  async function assertFullPostCrashInvariants(options: {
    readonly walletIds: readonly string[];
    readonly survivingInstance: RunningServiceInstance;
    readonly queues: IsolatedClusterQueues;
    readonly allowAtLeastOnceDuplicateEventIds?: ReadonlySet<string>;
  }): Promise<void> {
    // Wait until all outbox_messages for inspected wallets are published by surviving publishers:
    await pollUntil(
      async () => {
        const [pending] = await appSql<Array<{ count: number }>>`
          select count(*)::int as count
          from outbox_messages
          where published_at is null
        `;
        return pending?.count === 0 ? true : undefined;
      },
      { timeoutMs: 8_000, intervalMs: 30, description: 'all outbox_messages published after crash recovery' },
    );

    for (const walletId of options.walletIds) {
      const [walletRow] = await appSql<DbWalletRow[]>`
        select id, balance_amount::text as balance_amount, currency, version
        from wallets
        where id = ${walletId}
      `;
      expect(walletRow).toBeDefined();

      const storedBalance = new Decimal(walletRow!.balance_amount);
      expect(storedBalance.greaterThanOrEqualTo(0)).toBe(true);

      const ledgerRows = await appSql<DbLedgerRow[]>`
        select id,
               wallet_id,
               transaction_id,
               direction,
               amount::text as amount,
               currency,
               balance_before::text as balance_before,
               balance_after::text as balance_after,
               wallet_version
        from wallet_ledger_entries
        where wallet_id = ${walletId}
        order by wallet_version asc
      `;

      let runningBalance = new Decimal(0);
      const ledgerTxIds = new Set<string>();
      for (let idx = 0; idx < ledgerRows.length; idx += 1) {
        const entry = ledgerRows[idx]!;
        expect(entry.wallet_version).toBe(idx + 1);
        expect(new Decimal(entry.balance_before).equals(runningBalance)).toBe(true);

        const amt = new Decimal(entry.amount);
        expect(amt.greaterThan(0)).toBe(true);
        runningBalance =
          entry.direction === LedgerDirection.Credit ? runningBalance.plus(amt) : runningBalance.minus(amt);
        expect(runningBalance.greaterThanOrEqualTo(0)).toBe(true);
        expect(new Decimal(entry.balance_after).equals(runningBalance)).toBe(true);
        expect(ledgerTxIds.has(entry.transaction_id)).toBe(false);
        ledgerTxIds.add(entry.transaction_id);
      }

      if (ledgerRows.length > 0) {
        expect(walletRow!.version).toBe(ledgerRows.length);
      }
      expect(storedBalance.equals(runningBalance)).toBe(true);

      // Snapshot reconciliation via surviving service process:
      const reconRes = await options.survivingInstance.request(`/wallets/${walletId}/reconciliation`, {
        method: 'POST',
      });
      expect(reconRes.status).toBe(200);
      const recon = (await reconRes.json()) as WalletReconciliation;
      expect(recon.consistent).toBe(true);
      expect(recon.storedBalance.amount).toBe(storedBalance.toFixed(2));
      expect(recon.calculatedBalance.amount).toBe(runningBalance.toFixed(2));
      expect(recon.difference.amount).toBe('0.00');

      // Verify zero partially persisted transactions and 1:1 ledger correspondence:
      const txRows = await appSql<DbWagerTxRow[]>`
        select id,
               provider_id,
               external_transaction_id,
               idempotency_key,
               payload_hash,
               wallet_id,
               kind,
               amount::text as amount,
               status,
               failure_code,
               observed_balance_amount::text as observed_balance_amount,
               processed_at,
               next_attempt_at
        from wager_transactions
        where wallet_id = ${walletId}
      `;

      for (const tx of txRows) {
        expect(tx.idempotency_key.length).toBeGreaterThan(0);
        expect(/^[0-9a-f]{64}$/.test(tx.payload_hash)).toBe(true);
        if (tx.status === WagerTransactionStatus.Processed) {
          expect(tx.processed_at).not.toBeNull();
          expect(tx.observed_balance_amount).not.toBeNull();
          expect(tx.failure_code).toBeNull();
        }
        const affectsBalance =
          tx.status === WagerTransactionStatus.Processed &&
          tx.kind !== WagerTransactionKind.Loss &&
          new Decimal(tx.amount).greaterThan(0);
        expect(ledgerTxIds.has(tx.id)).toBe(affectsBalance);
      }

      const uniqueIdemKeys = new Set(txRows.map((t) => t.idempotency_key));
      const uniqueExtPairs = new Set(txRows.map((t) => `${t.provider_id}:${t.external_transaction_id}`));
      expect(uniqueIdemKeys.size).toBe(txRows.length);
      expect(uniqueExtPairs.size).toBe(txRows.length);
    }

    // Verify Inbox deduplication table integrity (zero half-processed rows, valid SHA-256 payload_hash):
    const inboxRows = await appSql<
      Array<{
        consumer_name: string;
        message_id: string;
        payload_hash: string;
        received_at: Date | string;
        processed_at: Date | string | null;
      }>
    >`
      select consumer_name, message_id, payload_hash, received_at, processed_at
      from inbox_messages
    `;
    const uniqueInboxKeys = new Set<string>();
    for (const row of inboxRows) {
      const composite = `${row.consumer_name}:${row.message_id}`;
      expect(uniqueInboxKeys.has(composite)).toBe(false);
      uniqueInboxKeys.add(composite);
      expect(/^[0-9a-f]{64}$/.test(row.payload_hash)).toBe(true);
      expect(row.processed_at).not.toBeNull();
      expect(new Date(row.processed_at!).getTime()).toBeGreaterThanOrEqual(new Date(row.received_at).getTime());
    }

    // Drain integration events queue and verify zero loss + causal sequence_number order per wallet:
    const expectedOutbox = await appSql<
      Array<{ id: string; aggregate_id: string; sequence_number: string }>
    >`
      select id, aggregate_id, sequence_number::text as sequence_number
      from outbox_messages
      order by outbox_messages.sequence_number asc
    `;

    const sqsMessages = await drainQueueMessages(
      sqs,
      options.queues.eventsQueueUrl,
      expectedOutbox.length,
      8_000,
    );

    const receivedByWallet = new Map<string, Array<{ eventId: string; sqsSequenceNumber: bigint }>>();
    const distinctReceivedIds = new Set<string>();
    for (const msg of sqsMessages) {
      if (msg.Body === undefined) {
        continue;
      }
      const parsed = JSON.parse(msg.Body) as { eventId: string; aggregateId: string };
      const rawSqsSeq = msg.Attributes?.SequenceNumber;
      expect(rawSqsSeq !== undefined && /^\d+$/.test(rawSqsSeq)).toBe(true);
      if (distinctReceivedIds.has(parsed.eventId)) {
        expect(options.allowAtLeastOnceDuplicateEventIds?.has(parsed.eventId) === true).toBe(true);
        continue;
      }
      distinctReceivedIds.add(parsed.eventId);

      let list = receivedByWallet.get(parsed.aggregateId);
      if (list === undefined) {
        list = [];
        receivedByWallet.set(parsed.aggregateId, list);
      }
      list.push({ eventId: parsed.eventId, sqsSequenceNumber: BigInt(rawSqsSeq!) });
    }

    expect(distinctReceivedIds.size).toBe(expectedOutbox.length);

    const expectedByWallet = new Map<string, string[]>();
    for (const row of expectedOutbox) {
      let list = expectedByWallet.get(row.aggregate_id);
      if (list === undefined) {
        list = [];
        expectedByWallet.set(row.aggregate_id, list);
      }
      list.push(row.id);
    }
    for (const [wId, expectedIds] of expectedByWallet) {
      const gotEntries = receivedByWallet.get(wId) ?? [];
      expect(gotEntries.map((e) => e.eventId)).toEqual(expectedIds);
      for (let i = 1; i < gotEntries.length; i += 1) {
        expect(gotEntries[i]!.sqsSequenceNumber > gotEntries[i - 1]!.sqsSequenceNumber).toBe(true);
      }
    }

    // Confirm zero residual messages in txQueue and DLQ, and zero blocked PostgreSQL sessions:
    const [txDepth, dlqDepth, blockedPg] = await Promise.all([
      getQueueDepth(sqs, options.queues.txQueueUrl),
      getQueueDepth(sqs, options.queues.dlqUrl),
      adminSql<Array<{ pid: number }>>`
        select pid::int as pid
        from pg_stat_activity
        where datname = current_database()
          and cardinality(pg_blocking_pids(pid)) > 0
      `,
    ]);
    expect(txDepth.visible).toBe(0);
    expect(txDepth.notVisible).toBe(0);
    expect(dlqDepth.visible).toBe(0);
    expect(dlqDepth.notVisible).toBe(0);
    expect([...blockedPg]).toEqual([]);
  }

  test('Cenário A — Crash abrupto durante transação financeira aberta antes do commit (3 processos reais, rollback automático PostgreSQL, liberação de locks e reentrega SQS processada 1 única vez)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 2, maxReceiveCount: 5 });
    const nodes = await spawnClusterNodes('p74-scen-a', 3, activeQueues);

    const targetWallet = await openWallet(nodes[0]!, '100.00', 'BRL');
    const helperWallet = await openWallet(nodes[1]!, '100.00', 'BRL');

    const envA = buildSqsEnvelope(targetWallet, {
      externalTransactionId: `crash-a-bet-${newId().slice(0, 8)}`,
      idempotencyKey: `idem-crash-a-${newId().slice(0, 8)}`,
      kind: WagerTransactionKind.Bet,
      amount: '35.00',
    });

    // Hold uncommitted idempotency_key barrier on helperWallet so the consumer that picks up envA
    // enters its SQL transaction, acquires `inbox_messages FOR UPDATE` + `wallets FOR UPDATE` on targetWallet,
    // and pauses deterministically BEFORE COMMIT!
    const barrier = await acquireUncommittedIdempotencyBarrier(helperWallet, envA.data.idempotencyKey);

    try {
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envA)));

      // Deterministically wait until one of the 3 OS processes holds the open transaction + granted wallet lock:
      const activeConsumerSnapshot = await pollUntil(
        async () => {
          const rows = await adminSql<
            Array<{ pid: number; application_name: string; has_wallet_lock: boolean; has_inbox_lock: boolean }>
          >`
            select a.pid::int as pid,
                   a.application_name,
                   exists (
                     select 1
                     from pg_locks l
                     join pg_class c on c.oid = l.relation
                     where l.pid = a.pid and l.granted = true and c.relname = 'wallets'
                   ) as has_wallet_lock,
                   exists (
                     select 1
                     from pg_locks l
                     join pg_class c on c.oid = l.relation
                     where l.pid = a.pid and l.granted = true and c.relname = 'inbox_messages'
                   ) as has_inbox_lock
            from pg_stat_activity a
            where a.datname = current_database()
              and ${barrier.gatePid} = any(pg_blocking_pids(a.pid))
          `;
          const candidate = rows[0];
          if (candidate !== undefined && candidate.has_wallet_lock && candidate.has_inbox_lock) {
            return candidate;
          }
          return undefined;
        },
        { timeoutMs: 6_000, intervalMs: 15, description: 'consumer holding open TX + wallets & inbox locks before commit' },
      );

      // Prove targetWallet row is genuinely locked by the active consumer (`FOR UPDATE NOWAIT` fails):
      let nowaitBlocked = false;
      try {
        await appSql.begin(async (tx) => {
          await tx`select id from wallets where id = ${targetWallet.id} for update nowait`;
        });
      } catch {
        nowaitBlocked = true;
      }
      expect(nowaitBlocked).toBe(true);

      const victimNode = nodes.find((n) => n.pgApplicationName === activeConsumerSnapshot.application_name)!;
      expect(victimNode).toBeDefined();
      const survivors = nodes.filter((n) => n.instanceId !== victimNode.instanceId);
      expect(survivors).toHaveLength(2);

      // Abruptly kill the consumer process BEFORE COMMIT!
      const crashStartedMs = performance.now();
      const killEvidence = await victimNode.killAbruptly();
      await barrier.rollback();

      // Verify abrupt OS kill + immediate PostgreSQL session & lock release:
      await assertAbruptDeathAndPgCleanup(killEvidence);

      // Confirm `FOR UPDATE NOWAIT` on targetWallet now succeeds immediately (zero orphan lock!):
      await appSql.begin(async (tx) => {
        const [unlocked] = await tx<Array<{ id: string }>>`
          select id from wallets where id = ${targetWallet.id} for update nowait
        `;
        expect(unlocked?.id).toBe(targetWallet.id);
      });

      // Immediately after crash (BEFORE the 2s SQS VisibilityTimeout expires):
      // verify ZERO partial financial writes in wallets, ledger, wager_transactions, or inbox_messages!
      const [walletAfterCrash] = await appSql<DbWalletRow[]>`
        select id, balance_amount::text as balance_amount, currency, version
        from wallets
        where id = ${targetWallet.id}
      `;
      expect(walletAfterCrash?.balance_amount).toBe('100.00');
      expect(walletAfterCrash?.version).toBe(1);

      const [partialTxCount] = await appSql<Array<{ cnt: number }>>`
        select count(*)::int as cnt
        from wager_transactions
        where external_transaction_id = ${envA.data.externalTransactionId}
      `;
      const [partialInboxCount] = await appSql<Array<{ cnt: number }>>`
        select count(*)::int as cnt
        from inbox_messages
        where consumer_name = ${WAGER_CONSUMER_NAME}
          and message_id = ${envA.messageId}
      `;
      expect(partialTxCount?.cnt).toBe(0);
      expect(partialInboxCount?.cnt).toBe(0);

      // Wait for SQS VisibilityTimeout (2s) expiration and recovery by one of the 2 surviving processes:
      await pollUntil(
        async () => {
          const [txRow] = await appSql<Array<{ status: string; observed_balance_amount: string }>>`
            select status, observed_balance_amount::text as observed_balance_amount
            from wager_transactions
            where external_transaction_id = ${envA.data.externalTransactionId}
          `;
          return txRow?.status === WagerTransactionStatus.Processed ? txRow : undefined;
        },
        { timeoutMs: 8_000, intervalMs: 35, description: 'surviving consumer recovery of envA after 2s lease expiry' },
      );
      const recoveryMs = Math.round(performance.now() - crashStartedMs);

      console.log(
        JSON.stringify({
          scenario: '7.4-A',
          killedInstanceId: killEvidence.instanceId,
          killedPid: killEvidence.pid,
          mechanism: killEvidence.mechanism,
          exitCode: killEvidence.exitCode,
          signalCode: killEvidence.signalCode,
          survivors: survivors.map((s) => ({ instanceId: s.instanceId, pid: s.pid })),
          recoveryMs,
        }),
      );

      // Verify single execution (`100.00 - 35.00 = 65.00 BRL`, `version = 2`):
      const [finalWallet] = await appSql<DbWalletRow[]>`
        select id, balance_amount::text as balance_amount, currency, version
        from wallets
        where id = ${targetWallet.id}
      `;
      expect(finalWallet?.balance_amount).toBe('65.00');
      expect(finalWallet?.version).toBe(2);

      await assertFullPostCrashInvariants({
        walletIds: [targetWallet.id, helperWallet.id],
        survivingInstance: survivors[0]!,
        queues: activeQueues,
      });
    } finally {
      await barrier.rollback().catch(() => undefined);
    }
  }, 35_000);

  test('Cenário B — Crash abrupto imediatamente após COMMIT PostgreSQL e antes do DeleteMessage SQS (reentrega real deduplicada via Inbox sem duplo efeito financeiro)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 2, maxReceiveCount: 5 });
    const upstreamEndpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';

    let resolveBeforeDeleteAck!: () => void;
    const beforeDeleteAckReached = new Promise<void>((resolve) => {
      resolveBeforeDeleteAck = resolve;
    });
    let releaseBlockedDeleteAck!: () => void;
    const holdDeleteAck = new Promise<void>((resolve) => {
      releaseBlockedDeleteAck = resolve;
    });

    // Wire-level pass-through HTTP proxy to LocalStack for node1 that intercepts `AmazonSQS.DeleteMessage`
    // strictly AFTER PostgreSQL COMMIT and BEFORE LocalStack receives the ACK!
    const deleteBarrierProxy = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch: async (req) => {
        const bodyBuf = await req.arrayBuffer();
        const amzTarget = req.headers.get('x-amz-target') ?? '';
        const bodyText = new TextDecoder().decode(bodyBuf);

        if (amzTarget.includes('DeleteMessage') || bodyText.includes('Action=DeleteMessage')) {
          resolveBeforeDeleteAck();
          await holdDeleteAck;
          return new Response('aborted before reaching SQS', { status: 500 });
        }

        const targetUrl = new URL(req.url);
        const upstreamUrl = new URL(targetUrl.pathname + targetUrl.search, upstreamEndpoint);
        const headers = new Headers(req.headers);
        headers.set('host', upstreamUrl.host);
        return fetch(upstreamUrl.toString(), {
          method: req.method,
          headers,
          body: bodyBuf.byteLength > 0 ? bodyBuf : undefined,
        });
      },
    });
    activeProxyStops.push(() => {
      releaseBlockedDeleteAck();
      deleteBarrierProxy.stop(true);
    });

    // Spawn node1 (via deleteBarrierProxy) first so it receives envB, plus node2 & node3 right after:
    const node1 = await cluster.spawn({
      instanceId: 'p74-scen-b-1',
      port: 0,
      queues: activeQueues,
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
      envOverrides: { AWS_ENDPOINT_URL: `http://127.0.0.1:${deleteBarrierProxy.port}` },
    });

    const wallet = await openWallet(node1, '100.00', 'BRL');
    const envB = buildSqsEnvelope(wallet, {
      externalTransactionId: `crash-b-bet-${newId().slice(0, 8)}`,
      kind: WagerTransactionKind.Bet,
      amount: '40.00',
    });

    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envB)));

    // Deterministically pause right after PostgreSQL COMMIT and before SQS DeleteMessage:
    await pollUntil(
      async () => {
        let reached = false;
        await Promise.race([
          beforeDeleteAckReached.then(() => {
            reached = true;
          }),
          Bun.sleep(5),
        ]);
        return reached ? true : undefined;
      },
      { timeoutMs: 6_000, intervalMs: 10, description: 'post-COMMIT / pre-DeleteMessage wire barrier on node1' },
    );

    // Spawn surviving nodes 2 & 3 so 3 real OS processes exist before killing node1:
    const [node2, node3] = await Promise.all([
      cluster.spawn({
        instanceId: 'p74-scen-b-2',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
      }),
      cluster.spawn({
        instanceId: 'p74-scen-b-3',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
      }),
    ]);

    // Confirm financial effect IS ALREADY COMMITTED in PostgreSQL while SQS message is STILL in-flight (unacknowledged):
    const [walletPreKill] = await appSql<DbWalletRow[]>`
      select id, balance_amount::text as balance_amount, currency, version
      from wallets
      where id = ${wallet.id}
    `;
    const [inboxPreKill] = await appSql<Array<{ processed_at: Date | null }>>`
      select processed_at
      from inbox_messages
      where consumer_name = ${WAGER_CONSUMER_NAME}
        and message_id = ${envB.messageId}
    `;
    const depthPreKill = await getQueueDepth(sqs, activeQueues.txQueueUrl);
    expect(walletPreKill?.balance_amount).toBe('60.00');
    expect(walletPreKill?.version).toBe(2);
    expect(inboxPreKill?.processed_at).not.toBeNull();
    expect(depthPreKill.notVisible).toBe(1);

    // Abruptly kill node1 BEFORE DeleteMessage ever reaches LocalStack!
    const crashStartedMs = performance.now();
    const killEvidence = await node1.killAbruptly();
    releaseBlockedDeleteAck();
    await assertAbruptDeathAndPgCleanup(killEvidence);

    // Wait for the 2s VisibilityTimeout to expire, allowing node2 or node3 to receive the REDELIVERED message,
    // deduplicate via `inbox_messages`, and execute the real `DeleteMessage` so `txQueueUrl` drains to 0!
    await pollUntil(
      async () => {
        const depth = await getQueueDepth(sqs, activeQueues!.txQueueUrl);
        return depth.visible === 0 && depth.notVisible === 0 ? true : undefined;
      },
      { timeoutMs: 8_000, intervalMs: 40, description: 'redelivery deduplicated via Inbox and ACKed by survivor' },
    );
    const recoveryMs = Math.round(performance.now() - crashStartedMs);

    console.log(
      JSON.stringify({
        scenario: '7.4-B',
        killedInstanceId: killEvidence.instanceId,
        killedPid: killEvidence.pid,
        mechanism: killEvidence.mechanism,
        exitCode: killEvidence.exitCode,
        signalCode: killEvidence.signalCode,
        survivors: [
          { instanceId: node2.instanceId, pid: node2.pid },
          { instanceId: node3.instanceId, pid: node3.pid },
        ],
        recoveryMs,
      }),
    );

    // Confirm zero duplicate debit (`60.00 BRL`, `version = 2`, 1 wager_transactions row, 2 ledger rows):
    const [walletPostRecovery] = await appSql<DbWalletRow[]>`
      select id, balance_amount::text as balance_amount, currency, version
      from wallets
      where id = ${wallet.id}
    `;
    const [wagerRowCount] = await appSql<Array<{ cnt: number }>>`
      select count(*)::int as cnt
      from wager_transactions
      where external_transaction_id = ${envB.data.externalTransactionId}
    `;
    expect(walletPostRecovery?.balance_amount).toBe('60.00');
    expect(walletPostRecovery?.version).toBe(2);
    expect(wagerRowCount?.cnt).toBe(1);

    // Also verify HTTP idempotency_key + payload_hash replay & conflict detection on survivor node3:
    const replayRes = await submitWagerHttp(
      node3,
      {
        providerId: envB.data.providerId,
        externalTransactionId: envB.data.externalTransactionId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: envB.data.roundId,
        gameId: envB.data.gameId,
        kind: envB.data.kind,
        money: envB.data.money,
      },
      envB.data.idempotencyKey,
    );
    expect(replayRes.status).toBe(200);
    expect(replayRes.body.idempotentReplay).toBe(true);

    const conflictRes = await submitWagerHttp<{ code: string }>(
      node3,
      {
        providerId: envB.data.providerId,
        externalTransactionId: envB.data.externalTransactionId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: envB.data.roundId,
        gameId: envB.data.gameId,
        kind: envB.data.kind,
        money: { amount: '41.00', currency: wallet.balance.currency },
      },
      envB.data.idempotencyKey,
    );
    expect(conflictRes.status).toBe(409);

    await assertFullPostCrashInvariants({
      walletIds: [wallet.id],
      survivingInstance: node2,
      queues: activeQueues,
    });
  }, 35_000);

  test('Cenário C — Crash abrupto durante heartbeat ativo e ownership SQS FIFO (3 processos reais, parada imediata de renovações, ausência de VisibilityTimeout=0 e preservação de ordem causal no MessageGroupId)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 2, maxReceiveCount: 5 });
    const upstreamEndpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';

    interface NodeObserver {
      renewalCount: number;
      zeroReleaseCount: number;
      lastRenewalAtMonoMs: number;
      proxyUrl: string;
    }

    const observers = new Map<string, NodeObserver>();
    for (const instanceId of ['p74-scen-c-1', 'p74-scen-c-2', 'p74-scen-c-3']) {
      const obs: NodeObserver = {
        renewalCount: 0,
        zeroReleaseCount: 0,
        lastRenewalAtMonoMs: 0,
        proxyUrl: '',
      };
      const proxy = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        fetch: async (req) => {
          const bodyBuf = await req.arrayBuffer();
          const bodyText = new TextDecoder().decode(bodyBuf);
          const amzTarget = req.headers.get('x-amz-target') ?? '';
          if (amzTarget.includes('ChangeMessageVisibilityBatch') || bodyText.includes('ChangeMessageVisibilityBatch')) {
            if (bodyText.includes('"VisibilityTimeout":0') || bodyText.includes('VisibilityTimeout=0')) {
              obs.zeroReleaseCount += 1;
            } else {
              obs.renewalCount += 1;
              obs.lastRenewalAtMonoMs = performance.now();
            }
          }
          const targetUrl = new URL(req.url);
          const upstreamUrl = new URL(targetUrl.pathname + targetUrl.search, upstreamEndpoint);
          const headers = new Headers(req.headers);
          headers.set('host', upstreamUrl.host);
          return fetch(upstreamUrl.toString(), {
            method: req.method,
            headers,
            body: bodyBuf.byteLength > 0 ? bodyBuf : undefined,
          });
        },
      });
      activeProxyStops.push(() => proxy.stop(true));
      obs.proxyUrl = `http://127.0.0.1:${proxy.port}`;
      observers.set(instanceId, obs);
    }

    // 1. Start ALL 3 real OS processes simultaneously up front:
    const nodes = await Promise.all(
      ['p74-scen-c-1', 'p74-scen-c-2', 'p74-scen-c-3'].map((instanceId) =>
        cluster.spawn({
          instanceId,
          port: 0,
          queues: activeQueues!,
          workersEnabled: true,
          sqsWaitTimeSeconds: 1,
          envOverrides: { AWS_ENDPOINT_URL: observers.get(instanceId)!.proxyUrl },
        }),
      ),
    );

    const wallet = await openWallet(nodes[0]!, '50.00', 'BRL');

    // Lock wallet so whichever consumer acquires the FIFO group head M1 stays in-flight and renews its lease:
    let resolveWalletLocked!: (pid: number) => void;
    const walletLocked = new Promise<number>((resolve) => {
      resolveWalletLocked = resolve;
    });
    let releaseWalletLock!: () => void;
    const holdWalletLock = new Promise<void>((resolve) => {
      releaseWalletLock = resolve;
    });

    const gateTx = gateSql.begin(async (tx) => {
      const [r] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wallets where id = ${wallet.id} for update`;
      resolveWalletLocked(r!.pid);
      await holdWalletLock;
    });
    const gatePid = await walletLocked;

    // 3 causal messages in the SAME MessageGroupId (`wallet.id`):
    // Initial 50.00 -> M1 WIN +60.00 (110.00) -> M2 BET -90.00 (20.00; would FAIL if out of order!) -> M3 BET -15.00 (5.00)
    const m1 = buildSqsEnvelope(wallet, {
      externalTransactionId: `c-m1-win-${newId().slice(0, 8)}`,
      kind: WagerTransactionKind.Win,
      amount: '60.00',
    });
    const m2 = buildSqsEnvelope(wallet, {
      externalTransactionId: `c-m2-bet-${newId().slice(0, 8)}`,
      kind: WagerTransactionKind.Bet,
      amount: '90.00',
    });
    const m3 = buildSqsEnvelope(wallet, {
      externalTransactionId: `c-m3-bet-${newId().slice(0, 8)}`,
      kind: WagerTransactionKind.Bet,
      amount: '15.00',
    });

    let killEvidence!: AbruptKillEvidence;
    let survivors: RunningServiceInstance[] = [];
    let crashStartedMs = 0;
    let renewalsAtKill = 0;
    let ownerObserver!: NodeObserver;

    try {
      await sqs.send(
        new SendMessageBatchCommand({
          QueueUrl: activeQueues.txQueueUrl,
          Entries: [m1, m2, m3].map((env, idx) => {
            const single = toWagerTransactionSendInput(activeQueues!.txQueueUrl, env);
            return {
              Id: `c-${idx}`,
              MessageBody: single.MessageBody!,
              MessageGroupId: single.MessageGroupId!,
              MessageDeduplicationId: single.MessageDeduplicationId!,
            };
          }),
        }),
      );

      // Identify which of the 3 processes acquired the FIFO group and blocked on wallet:
      const ownerAppName = await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ application_name: string }>>`
            select application_name
            from pg_stat_activity
            where datname = current_database()
              and ${gatePid} = any(pg_blocking_pids(pid))
          `;
          return rows[0]?.application_name;
        },
        { timeoutMs: 6_000, intervalMs: 15, description: 'active FIFO group owner blocked on wallet lock' },
      );

      const ownerNode = nodes.find((n) => n.pgApplicationName === ownerAppName)!;
      expect(ownerNode).toBeDefined();
      survivors = nodes.filter((n) => n.instanceId !== ownerNode.instanceId);
      ownerObserver = observers.get(ownerNode.instanceId)!;

      // Confirm BatchVisibilityLeaseManager actively renews visibility >= 2 times (past the 2s initial timeout):
      const ownerAcquiredMono = performance.now();
      await pollUntil(
        async () =>
          ownerObserver.renewalCount >= 2 && performance.now() - ownerAcquiredMono >= 2_100 ? true : undefined,
        { timeoutMs: 6_000, intervalMs: 30, description: '>= 2 visibility lease renewals past 2s VisibilityTimeout' },
      );

      // Abruptly terminate the active lease owner!
      crashStartedMs = performance.now();
      renewalsAtKill = ownerObserver.renewalCount;
      killEvidence = await ownerNode.killAbruptly();

      // Immediately verify NO immediate VisibilityTimeout=0 release happened (`notVisible >= 1` right after crash!):
      const depthRightAfterCrash = await getQueueDepth(sqs, activeQueues.txQueueUrl);
      expect(ownerObserver.zeroReleaseCount).toBe(0);
      expect(depthRightAfterCrash.notVisible).toBeGreaterThanOrEqual(1);
    } finally {
      releaseWalletLock();
      await gateTx;
    }

    await assertAbruptDeathAndPgCleanup(killEvidence);

    // Dead owner must NEVER send another renewal after killAbruptly():
    expect(ownerObserver.renewalCount).toBe(renewalsAtKill);

    // Wait for last valid 2s lease to expire and surviving instances to process [M1, M2, M3] in strict causal order:
    await pollUntil(
      async () => {
        const rows = await appSql<Array<{ external_transaction_id: string; status: string }>>`
          select external_transaction_id, status
          from wager_transactions
          where wallet_id = ${wallet.id}
            and kind <> 'OPENING'
        `;
        return rows.length === 3 && rows.every((r) => r.status === WagerTransactionStatus.Processed)
          ? rows
          : undefined;
      },
      { timeoutMs: 10_000, intervalMs: 40, description: 'surviving consumers process M1 -> M2 -> M3 after lease expiry' },
    );
    const recoveryMs = Math.round(performance.now() - crashStartedMs);
    expect(ownerObserver.renewalCount).toBe(renewalsAtKill);

    console.log(
      JSON.stringify({
        scenario: '7.4-C',
        killedInstanceId: killEvidence.instanceId,
        killedPid: killEvidence.pid,
        mechanism: killEvidence.mechanism,
        exitCode: killEvidence.exitCode,
        signalCode: killEvidence.signalCode,
        renewalsBeforeCrash: renewalsAtKill,
        renewalsAfterCrash: ownerObserver.renewalCount - renewalsAtKill,
        survivors: survivors.map((s) => ({ instanceId: s.instanceId, pid: s.pid })),
        recoveryMs,
      }),
    );

    // Final balance MUST be 50.00 + 60.00 - 90.00 - 15.00 = 5.00 BRL (`version = 4`), proving causal order!
    const [finalWallet] = await appSql<DbWalletRow[]>`
      select id, balance_amount::text as balance_amount, currency, version
      from wallets
      where id = ${wallet.id}
    `;
    expect(finalWallet?.balance_amount).toBe('5.00');
    expect(finalWallet?.version).toBe(4);

    // Verify exact ledger version sequence [m1 (v2, +60), m2 (v3, -90), m3 (v4, -15)] and zero stale-ownership execution:
    const orderedExternalIds = await appSql<Array<{ external_transaction_id: string; wallet_version: number }>>`
      select wt.external_transaction_id, wle.wallet_version
      from wallet_ledger_entries wle
      join wager_transactions wt on wt.id = wle.transaction_id
      where wle.wallet_id = ${wallet.id}
        and wt.kind <> 'OPENING'
      order by wle.wallet_version asc
    `;
    expect([...orderedExternalIds]).toEqual([
      { external_transaction_id: m1.data.externalTransactionId, wallet_version: 2 },
      { external_transaction_id: m2.data.externalTransactionId, wallet_version: 3 },
      { external_transaction_id: m3.data.externalTransactionId, wallet_version: 4 },
    ]);

    await assertFullPostCrashInvariants({
      walletIds: [wallet.id],
      survivingInstance: survivors[0]!,
      queues: activeQueues,
    });
  }, 40_000);

  test('Cenário D — Crash abrupto do processo proprietário durante contenção real de locks PostgreSQL (pg_blocking_pids comprova bloqueio e desbloqueio imediato dos sobreviventes sem deadlock)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });
    const [node1, node2, node3] = await spawnClusterNodes('p74-scen-d', 3, activeQueues);

    const contendedWallet = await openWallet(node1!, '100.00', 'BRL');
    const helperWallet = await openWallet(node2!, '100.00', 'BRL');

    const node1HoldIdemKey = `idem-d-hold-${newId().slice(0, 8)}`;
    const barrier = await acquireUncommittedIdempotencyBarrier(helperWallet, node1HoldIdemKey);

    try {
      // 1. Launch an HTTP wager on node1 targeting contendedWallet so node1 itself acquires
      //    `SELECT ... FROM wallets WHERE id = contendedWallet.id FOR UPDATE` and pauses before COMMIT:
      const node1RequestPromise = submitWagerHttp(
        node1!,
        buildWagerPayload(contendedWallet, {
          externalTransactionId: `d-owner-bet-${newId().slice(0, 8)}`,
          kind: WagerTransactionKind.Bet,
          amount: '30.00',
        }),
        node1HoldIdemKey,
      ).catch((err: unknown) => ({ abortedByCrash: true as const, reason: String(err) }));

      const node1BackendPid = await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ pid: number; has_wallet_lock: boolean }>>`
            select a.pid::int as pid,
                   exists (
                     select 1
                     from pg_locks l
                     join pg_class c on c.oid = l.relation
                     where l.pid = a.pid and l.granted = true and c.relname = 'wallets'
                   ) as has_wallet_lock
            from pg_stat_activity a
            where a.datname = current_database()
              and a.application_name = ${node1!.pgApplicationName}
              and ${barrier.gatePid} = any(pg_blocking_pids(a.pid))
          `;
          return rows[0]?.has_wallet_lock === true ? rows[0].pid : undefined;
        },
        { timeoutMs: 6_000, intervalMs: 15, description: 'node1 holding granted wallets FOR UPDATE lock' },
      );

      // 2. Launch concurrent operations on node2 (BET 40.00) and node3 (WIN 25.00) targeting contendedWallet:
      const ext2 = `d-waiter2-bet-${newId().slice(0, 8)}`;
      const ext3 = `d-waiter3-win-${newId().slice(0, 8)}`;
      const survivorsPromise = Promise.all([
        submitWagerHttp(
          node2!,
          buildWagerPayload(contendedWallet, {
            externalTransactionId: ext2,
            kind: WagerTransactionKind.Bet,
            amount: '40.00',
          }),
          `idem-${ext2}`,
        ),
        submitWagerHttp(
          node3!,
          buildWagerPayload(contendedWallet, {
            externalTransactionId: ext3,
            kind: WagerTransactionKind.Win,
            amount: '25.00',
          }),
          `idem-${ext3}`,
        ),
      ]);

      // Confirm via pg_stat_activity + transitive pg_blocking_pids() that BOTH node2 and node3 are blocked behind node1BackendPid!
      const contentionSnapshot = await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ pid: number; application_name: string; blockers: number[] }>>`
            with recursive lock_tree as (
              select pid::int as pid,
                     application_name,
                     pg_blocking_pids(pid)::int[] as blockers
              from pg_stat_activity
              where datname = current_database()
                and pid <> ${node1BackendPid}
                and ${node1BackendPid} = any(pg_blocking_pids(pid))
              union
              select a.pid::int as pid,
                     a.application_name,
                     pg_blocking_pids(a.pid)::int[] as blockers
              from pg_stat_activity a
              join lock_tree lt on lt.pid = any(pg_blocking_pids(a.pid))
              where a.datname = current_database()
                and a.pid <> ${node1BackendPid}
            )
            select pid, application_name, blockers
            from lock_tree
            order by application_name asc
          `;
          const apps = new Set(rows.map((r) => r.application_name));
          return apps.has(node2!.pgApplicationName) && apps.has(node3!.pgApplicationName) ? rows : undefined;
        },
        { timeoutMs: 6_000, intervalMs: 15, description: 'node2 and node3 blocked behind node1BackendPid' },
      );
      expect(contentionSnapshot.length).toBeGreaterThanOrEqual(2);

      // 3. Abruptly kill node1 (the lock owner)!
      const crashStartedMs = performance.now();
      const killEvidence = await node1!.killAbruptly();
      await barrier.rollback();

      const node1ClientOutcome = await node1RequestPromise;
      expect('abortedByCrash' in node1ClientOutcome && node1ClientOutcome.abortedByCrash).toBe(true);

      // 4. Surviving processes node2 and node3 immediately acquire the released lock and succeed!
      const [res2, res3] = await survivorsPromise;
      const recoveryMs = Math.round(performance.now() - crashStartedMs);

      await assertAbruptDeathAndPgCleanup(killEvidence);

      expect(res2.status).toBe(200);
      expect(res2.instanceIdHeader).toBe(node2!.instanceId);
      expect(res3.status).toBe(200);
      expect(res3.instanceIdHeader).toBe(node3!.instanceId);

      console.log(
        JSON.stringify({
          scenario: '7.4-D',
          killedInstanceId: killEvidence.instanceId,
          killedPid: killEvidence.pid,
          killedPgBackendPid: node1BackendPid,
          mechanism: killEvidence.mechanism,
          exitCode: killEvidence.exitCode,
          blockedWaitersBeforeKill: contentionSnapshot,
          recoveryMs,
        }),
      );

      // node1's uncommitted BET -30.00 rolled back; node2 (-40.00) + node3 (+25.00) committed -> 85.00 BRL, version 3!
      const [finalWallet] = await appSql<DbWalletRow[]>`
        select id, balance_amount::text as balance_amount, currency, version
        from wallets
        where id = ${contendedWallet.id}
      `;
      expect(finalWallet?.balance_amount).toBe('85.00');
      expect(finalWallet?.version).toBe(3);

      await assertFullPostCrashInvariants({
        walletIds: [contendedWallet.id, helperWallet.id],
        survivingInstance: node2!,
        queues: activeQueues,
      });
    } finally {
      await barrier.rollback().catch(() => undefined);
    }
  }, 35_000);

  test('Cenário E — Crash abrupto durante os 4 pontos críticos da publicação Outbox (antes do envio, durante envio, após confirmação SQS antes de published_at, e após published_at)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });
    const upstreamEndpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';

    // Seed wallets via HTTP-only seeder (`workersEnabled: false`) so we control exact Outbox publication phases:
    const seeder = await cluster.spawn({
      instanceId: 'p74-scen-e-seeder',
      port: 0,
      queues: activeQueues,
      workersEnabled: false,
    });

    type OutboxCrashMode = 'pass' | 'hold_before_sqs' | 'hold_during_sqs' | 'hold_after_sqs_confirm';
    let proxyMode: OutboxCrashMode = 'pass';
    let notifyBarrierHit: (() => void) | undefined;
    let releaseProxyHold: (() => void) | undefined;

    const armProxyBarrier = (mode: OutboxCrashMode): { hit: Promise<void>; release: () => void } => {
      proxyMode = mode;
      const hit = new Promise<void>((resolve) => {
        notifyBarrierHit = resolve;
      });
      const hold = new Promise<void>((resolve) => {
        releaseProxyHold = resolve;
      });
      void hold;
      return {
        hit,
        release: () => {
          proxyMode = 'pass';
          releaseProxyHold?.();
        },
      };
    };

    const createControlledOutboxProxy = () => {
      const proxy = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        fetch: async (req) => {
          const bodyBuf = await req.arrayBuffer();
          const amzTarget = req.headers.get('x-amz-target') ?? '';
          const bodyText = new TextDecoder().decode(bodyBuf);
          const isSendBatch =
            amzTarget.includes('SendMessageBatch') || bodyText.includes('Action=SendMessageBatch');

          const forwardNow = () => {
            const targetUrl = new URL(req.url);
            const upstreamUrl = new URL(targetUrl.pathname + targetUrl.search, upstreamEndpoint);
            const headers = new Headers(req.headers);
            headers.set('host', upstreamUrl.host);
            return fetch(upstreamUrl.toString(), {
              method: req.method,
              headers,
              body: bodyBuf.byteLength > 0 ? bodyBuf : undefined,
            });
          };

          if (isSendBatch && proxyMode === 'hold_before_sqs') {
            const hold = new Promise<void>((r) => {
              const prev = releaseProxyHold;
              releaseProxyHold = () => {
                prev?.();
                r();
              };
            });
            notifyBarrierHit?.();
            await hold;
            return new Response('aborted before SQS send', { status: 500 });
          }

          if (isSendBatch && proxyMode === 'hold_during_sqs') {
            // Start in-flight HTTP call without returning response to publisher before kill:
            const hold = new Promise<void>((r) => {
              const prev = releaseProxyHold;
              releaseProxyHold = () => {
                prev?.();
                r();
              };
            });
            notifyBarrierHit?.();
            await hold;
            return new Response('aborted mid-flight SQS send', { status: 500 });
          }

          if (isSendBatch && proxyMode === 'hold_after_sqs_confirm') {
            // Actually send to LocalStack and wait for 200 OK confirmation FIRST, then hold response so
            // PostgreSQL `published_at` is NOT updated before publisher crashes!
            const confirmedUpstreamRes = await forwardNow();
            const confirmedText = await confirmedUpstreamRes.text();
            const hold = new Promise<void>((r) => {
              const prev = releaseProxyHold;
              releaseProxyHold = () => {
                prev?.();
                r();
              };
            });
            notifyBarrierHit?.();
            await hold;
            return new Response(confirmedText, {
              status: confirmedUpstreamRes.status,
              headers: confirmedUpstreamRes.headers,
            });
          }

          return forwardNow();
        },
      });
      activeProxyStops.push(() => proxy.stop(true));
      return `http://127.0.0.1:${proxy.port}`;
    };

    const killRecords: Array<Record<string, unknown>> = [];

    // --- Phase E1: Crash BEFORE SQS send (while holding `outbox_messages FOR UPDATE SKIP LOCKED`) ---
    const wE1 = await openWallet(seeder, '100.00', 'BRL'); // creates 2 unpublished causal events (seq 1, seq 2)
    const proxyE1Url = createControlledOutboxProxy();
    const barrierE1 = armProxyBarrier('hold_before_sqs');
    const pub1 = await cluster.spawn({
      instanceId: 'p74-scen-e-pub-1',
      port: 0,
      queues: activeQueues,
      workersEnabled: true,
      outboxPollIntervalMs: 30,
      envOverrides: { AWS_ENDPOINT_URL: proxyE1Url },
    });

    await barrierE1.hit;
    const depthBeforeSqsKill = await getQueueDepth(sqs, activeQueues.eventsQueueUrl);
    expect(depthBeforeSqsKill.visible + depthBeforeSqsKill.notVisible).toBe(0);

    const killE1 = await pub1.killAbruptly();
    barrierE1.release();
    await assertAbruptDeathAndPgCleanup(killE1);
    killRecords.push({ phase: 'E1-before-sqs', instanceId: killE1.instanceId, pid: killE1.pid, exitCode: killE1.exitCode });

    const [unpublishedAfterE1] = await appSql<Array<{ cnt: number }>>`
      select count(*)::int as cnt from outbox_messages where aggregate_id = ${wE1.id} and published_at is null
    `;
    expect(unpublishedAfterE1?.cnt).toBe(2);

    // --- Phase E2: Crash DURING in-flight SQS publish call ---
    const proxyE2Url = createControlledOutboxProxy();
    const barrierE2 = armProxyBarrier('hold_during_sqs');
    const pub2 = await cluster.spawn({
      instanceId: 'p74-scen-e-pub-2',
      port: 0,
      queues: activeQueues,
      workersEnabled: true,
      outboxPollIntervalMs: 30,
      envOverrides: { AWS_ENDPOINT_URL: proxyE2Url },
    });

    await barrierE2.hit;
    const killE2 = await pub2.killAbruptly();
    barrierE2.release();
    await assertAbruptDeathAndPgCleanup(killE2);
    killRecords.push({ phase: 'E2-during-sqs', instanceId: killE2.instanceId, pid: killE2.pid, exitCode: killE2.exitCode });

    // --- Phase E3: Crash AFTER confirmed SQS 200 OK, BEFORE `published_at` update/commit in PostgreSQL ---
    const wE3 = await openWallet(seeder, '120.00', 'BRL');
    const betE3Ext = `e3-bet-${newId().slice(0, 8)}`;
    const betE3Res = await submitWagerHttp(
      seeder,
      buildWagerPayload(wE3, { externalTransactionId: betE3Ext, amount: '20.00' }),
      `idem-${betE3Ext}`,
    );
    expect(betE3Res.status).toBe(200);

    const proxyE3Url = createControlledOutboxProxy();
    const barrierE3 = armProxyBarrier('hold_after_sqs_confirm');
    const pub3 = await cluster.spawn({
      instanceId: 'p74-scen-e-pub-3',
      port: 0,
      queues: activeQueues,
      workersEnabled: true,
      outboxPollIntervalMs: 30,
      envOverrides: { AWS_ENDPOINT_URL: proxyE3Url },
    });

    await barrierE3.hit;
    // Broker ALREADY confirmed receipt of head batch (`visible >= 1`), while PostgreSQL `published_at` is STILL NULL!
    const [publishedCountBeforeE3Kill] = await appSql<Array<{ cnt: number }>>`
      select count(*)::int as cnt from outbox_messages where published_at is not null
    `;
    const depthBeforeE3Kill = await getQueueDepth(sqs, activeQueues.eventsQueueUrl);
    expect(publishedCountBeforeE3Kill?.cnt).toBe(0);
    expect(depthBeforeE3Kill.visible + depthBeforeE3Kill.notVisible).toBeGreaterThanOrEqual(1);

    const killE3 = await pub3.killAbruptly();
    barrierE3.release();
    await assertAbruptDeathAndPgCleanup(killE3);
    killRecords.push({ phase: 'E3-after-sqs-before-db', instanceId: killE3.instanceId, pid: killE3.pid, exitCode: killE3.exitCode });

    // Confirm PostgreSQL rolled back pub3's uncommitted transaction (`published_at` remains NULL for all rows):
    const [publishedCountAfterE3Kill] = await appSql<Array<{ cnt: number }>>`
      select count(*)::int as cnt from outbox_messages where published_at is not null
    `;
    expect(publishedCountAfterE3Kill?.cnt).toBe(0);

    // --- Phase E4: Crash AFTER `published_at` committed + full recovery by 3 surviving publishers ---
    const wE4 = await openWallet(seeder, '200.00', 'BRL');
    await seeder.stopGracefully('SIGTERM');

    const crashRecoveryStartMs = performance.now();
    const [pub4, survivorA, survivorB] = await spawnClusterNodes('p74-scen-e-surv', 3, activeQueues, {
      outboxPollIntervalMs: 25,
    });

    // Wait until at least 2 outbox events have `published_at IS NOT NULL`, then abruptly kill pub4 AFTER commit:
    await pollUntil(
      async () => {
        const [row] = await appSql<Array<{ cnt: number }>>`
          select count(*)::int as cnt from outbox_messages where published_at is not null
        `;
        return (row?.cnt ?? 0) >= 2 ? true : undefined;
      },
      { timeoutMs: 6_000, intervalMs: 15, description: 'published_at committed before E4 kill' },
    );
    const killE4 = await pub4!.killAbruptly();
    await assertAbruptDeathAndPgCleanup(killE4);
    killRecords.push({ phase: 'E4-after-published-at', instanceId: killE4.instanceId, pid: killE4.pid, exitCode: killE4.exitCode });

    // Add one more wallet movement on survivorA to verify continued causal publishing after E4 crash:
    const postE4Ext = `e4-post-${newId().slice(0, 8)}`;
    const postE4Res = await submitWagerHttp(
      survivorA!,
      buildWagerPayload(wE4, { externalTransactionId: postE4Ext, kind: WagerTransactionKind.Win, amount: '35.00' }),
      `idem-${postE4Ext}`,
    );
    expect(postE4Res.status).toBe(200);

    // Verify all outbox rows publish with zero loss and strict per-wallet sequence_number order
    // (within SQS FIFO's 5m window `MessageDeduplicationId = eventId` absorbs E3's re-send; we also allow
    // at-least-once same-eventId redelivery if broker window were exceeded, while strictly enforcing causal order):
    const allEventRows = await appSql<Array<{ id: string }>>`select id from outbox_messages`;
    await assertFullPostCrashInvariants({
      walletIds: [wE1.id, wE3.id, wE4.id],
      survivingInstance: survivorB!,
      queues: activeQueues,
      allowAtLeastOnceDuplicateEventIds: new Set(allEventRows.map((r) => r.id)),
    });
    const recoveryMs = Math.round(performance.now() - crashRecoveryStartMs);

    console.log(
      JSON.stringify({
        scenario: '7.4-E',
        crashes: killRecords,
        survivors: [
          { instanceId: survivorA!.instanceId, pid: survivorA!.pid },
          { instanceId: survivorB!.instanceId, pid: survivorB!.pid },
        ],
        recoveryMs,
      }),
    );
  }, 45_000);

  test('Cenário F — Crash abrupto durante resolução de PENDING_REFERENCE por PendingReferenceWorker (3 processos reais, rollback de lock, estado preservado e resolução única por sobrevivente)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });

    // Create PENDING_REFERENCE transaction via HTTP-only node first so we arm the deterministic lock barrier cleanly:
    const httpNode = await cluster.spawn({
      instanceId: 'p74-scen-f-seed',
      port: 0,
      queues: activeQueues,
      workersEnabled: false,
    });

    const wallet = await openWallet(httpNode, '100.00', 'BRL');
    const betExtId = `f-bet-${newId().slice(0, 8)}`;
    const refundExtId = `f-refund-${newId().slice(0, 8)}`;

    // 1. Out-of-order REFUND arrives BEFORE referenced BET -> 202 PENDING_REFERENCE!
    const refundRes = await submitWagerHttp(
      httpNode,
      buildWagerPayload(wallet, {
        externalTransactionId: refundExtId,
        kind: WagerTransactionKind.Refund,
        amount: '25.00',
        referenceExternalTransactionId: betExtId,
      }),
      `idem-${refundExtId}`,
    );
    expect(refundRes.status).toBe(202);
    expect(refundRes.body.status).toBe(WagerTransactionStatus.PendingReference);
    const pendingRefundTxId = refundRes.body.transactionId;

    // 2. Submit the referenced BET -> 200 PROCESSED (`100.00 -> 75.00 BRL`, `version = 2`):
    const betRes = await submitWagerHttp(
      httpNode,
      buildWagerPayload(wallet, {
        externalTransactionId: betExtId,
        kind: WagerTransactionKind.Bet,
        amount: '25.00',
      }),
      `idem-${betExtId}`,
    );
    expect(betRes.status).toBe(200);
    await httpNode.stopGracefully('SIGTERM');

    // Make `next_attempt_at` immediately due BEFORE acquiring the row lock on `pendingRefundTxId`:
    await migratorOrm.em.fork().execute(
      `update wager_transactions set next_attempt_at = clock_timestamp() - interval '1 second' where id = ?`,
      [pendingRefundTxId],
    );

    // Hold `SELECT id FROM wager_transactions WHERE id = pendingRefundTxId FOR UPDATE` (Step 2 of lock order)
    // WITHOUT locking `wallets`, so the first PendingReferenceWorker to resolve `pendingRefundTxId`:
    //   - Acquires `wallets FOR UPDATE` on `wallet.id` (Step 1 — GRANTED!),
    //   - Blocks on `wager_transactions FOR UPDATE` (Step 2 — WAITING on gateSql)!
    let resolveTxRowLocked!: (pid: number) => void;
    const txRowLocked = new Promise<number>((resolve) => {
      resolveTxRowLocked = resolve;
    });
    let releaseTxRowLock!: () => void;
    const holdTxRowLock = new Promise<void>((resolve) => {
      releaseTxRowLock = resolve;
    });

    const gateTx = gateSql.begin(async (tx) => {
      const [r] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wager_transactions where id = ${pendingRefundTxId} for update`;
      resolveTxRowLocked(r!.pid);
      await holdTxRowLock;
    });
    const gatePid = await txRowLocked;

    let killEvidence!: AbruptKillEvidence;
    let survivors: RunningServiceInstance[] = [];
    let crashStartedMs = 0;

    try {
      const workers = await spawnClusterNodes('p74-scen-f-w', 3, activeQueues, {
        pendingReferencePollIntervalMs: 35,
        outboxPollIntervalMs: 40,
      });
      // 3. Deterministically confirm one worker holds `wallets FOR UPDATE` inside its open resolution TX:
      const blockedWorker = await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ pid: number; application_name: string; has_wallet_lock: boolean }>>`
            select a.pid::int as pid,
                   a.application_name,
                   exists (
                     select 1
                     from pg_locks l
                     join pg_class c on c.oid = l.relation
                     where l.pid = a.pid and l.granted = true and c.relname = 'wallets'
                   ) as has_wallet_lock
            from pg_stat_activity a
            where a.datname = current_database()
              and ${gatePid} = any(pg_blocking_pids(a.pid))
          `;
          const match = rows.find((r) => r.has_wallet_lock);
          return match;
        },
        { timeoutMs: 6_000, intervalMs: 15, description: 'PendingReferenceWorker holding wallets FOR UPDATE' },
      );

      const victimWorker = workers.find((w) => w.pgApplicationName === blockedWorker.application_name)!;
      expect(victimWorker).toBeDefined();
      survivors = workers.filter((w) => w.instanceId !== victimWorker.instanceId);
      expect(survivors).toHaveLength(2);

      // 4. Abruptly terminate the active PendingReferenceWorker!
      crashStartedMs = performance.now();
      killEvidence = await victimWorker.killAbruptly();
      await assertAbruptDeathAndPgCleanup(killEvidence);

      // 5. BEFORE releasing gateTx, verify persisted state in PostgreSQL:
      //    reference is STILL `PENDING_REFERENCE` and wallet balance is STILL `75.00 BRL` (`version = 2`)!
      const [pendingStillIntact] = await appSql<Array<{ status: string }>>`
        select status from wager_transactions where id = ${pendingRefundTxId}
      `;
      const [walletBeforeResume] = await appSql<DbWalletRow[]>`
        select id, balance_amount::text as balance_amount, currency, version
        from wallets
        where id = ${wallet.id}
      `;
      expect(pendingStillIntact?.status).toBe(WagerTransactionStatus.PendingReference);
      expect(walletBeforeResume?.balance_amount).toBe('75.00');
      expect(walletBeforeResume?.version).toBe(2);
    } finally {
      // 6. Release gateTx so surviving workers resume resolution:
      releaseTxRowLock();
      await gateTx;
    }

    // 7. Surviving worker resolves `pendingRefundTxId` ONCE (`75.00 + 25.00 = 100.00 BRL`, `version = 3`):
    await pollUntil(
      async () => {
        const [row] = await appSql<Array<{ status: string }>>`
          select status from wager_transactions where id = ${pendingRefundTxId}
        `;
        return row?.status === WagerTransactionStatus.Processed ? true : undefined;
      },
      { timeoutMs: 6_000, intervalMs: 25, description: 'surviving PendingReferenceWorker resolves refund once' },
    );
    const recoveryMs = Math.round(performance.now() - crashStartedMs);

    console.log(
      JSON.stringify({
        scenario: '7.4-F',
        killedInstanceId: killEvidence.instanceId,
        killedPid: killEvidence.pid,
        mechanism: killEvidence.mechanism,
        exitCode: killEvidence.exitCode,
        survivors: survivors.map((s) => ({ instanceId: s.instanceId, pid: s.pid })),
        recoveryMs,
      }),
    );

    const [finalWallet] = await appSql<DbWalletRow[]>`
      select id, balance_amount::text as balance_amount, currency, version
      from wallets
      where id = ${wallet.id}
    `;
    expect(finalWallet?.balance_amount).toBe('100.00');
    expect(finalWallet?.version).toBe(3);

    await assertFullPostCrashInvariants({
      walletIds: [wallet.id],
      survivingInstance: survivors[0]!,
      queues: activeQueues,
    });
  }, 35_000);

  test('Cenário G — Recuperação determinística do cluster após múltiplos crashes sequenciais (3 -> 2 -> 1 processo sobrevivente recuperando SQS, PendingReference e Outbox)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 2, maxReceiveCount: 5 });

    // 1. Start 3 real OS processes with distinct/full worker responsibilities across SQS, Outbox, and PendingReference:
    const allNodes = await spawnClusterNodes('p74-scen-g', 3, activeQueues, {
      sqsWaitTimeSeconds: 1,
      outboxPollIntervalMs: 35,
      pendingReferencePollIntervalMs: 35,
    });

    const wSqs = await openWallet(allNodes[0]!, '100.00', 'BRL');
    const wPending = await openWallet(allNodes[1]!, '100.00', 'BRL');
    const wFinal = await openWallet(allNodes[2]!, '100.00', 'BRL');
    const wHelper = await openWallet(allNodes[0]!, '100.00', 'BRL');

    // --- CRASH #1: Kill 1st process while consuming SQS message on wSqs (3 -> 2 processes) ---
    const envG1 = buildSqsEnvelope(wSqs, {
      externalTransactionId: `g-sqs-1-${newId().slice(0, 8)}`,
      idempotencyKey: `idem-g-sqs-1-${newId().slice(0, 8)}`,
      kind: WagerTransactionKind.Bet,
      amount: '30.00',
    });
    const barrier1 = await acquireUncommittedIdempotencyBarrier(wHelper, envG1.data.idempotencyKey);

    let kill1!: AbruptKillEvidence;
    let afterCrash1Nodes: RunningServiceInstance[] = [];
    const crash1StartMs = performance.now();

    try {
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envG1)));

      const victim1App = await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ application_name: string }>>`
            select application_name
            from pg_stat_activity
            where datname = current_database()
              and ${barrier1.gatePid} = any(pg_blocking_pids(pid))
          `;
          return rows[0]?.application_name;
        },
        { timeoutMs: 6_000, intervalMs: 15, description: 'victim1 blocked in SQS consumer TX' },
      );

      const victim1 = allNodes.find((n) => n.pgApplicationName === victim1App)!;
      afterCrash1Nodes = allNodes.filter((n) => n.instanceId !== victim1.instanceId);
      expect(afterCrash1Nodes).toHaveLength(2);

      kill1 = await victim1.killAbruptly();
    } finally {
      await barrier1.rollback();
    }
    await assertAbruptDeathAndPgCleanup(kill1);

    // Confirm both 2 surviving nodes remain healthy (`200 /health/ready`) and recover envG1 (`100 -> 70`):
    for (const s of afterCrash1Nodes) {
      const ready = await s.request('/health/ready');
      expect(ready.status).toBe(200);
    }
    await pollUntil(
      async () => {
        const [row] = await appSql<Array<{ status: string }>>`
          select status from wager_transactions where external_transaction_id = ${envG1.data.externalTransactionId}
        `;
        return row?.status === WagerTransactionStatus.Processed ? true : undefined;
      },
      { timeoutMs: 8_000, intervalMs: 35, description: '2 survivors recover envG1 after Crash #1' },
    );
    const crash1RecoveryMs = Math.round(performance.now() - crash1StartMs);

    // --- CRASH #2: Create PENDING_REFERENCE on wPending, kill 2nd process mid-resolution (2 -> 1 survivor!) ---
    const betG2Ext = `g-bet-2-${newId().slice(0, 8)}`;
    const refundG2Ext = `g-refund-2-${newId().slice(0, 8)}`;

    const refundG2Res = await submitWagerHttp(
      afterCrash1Nodes[0]!,
      buildWagerPayload(wPending, {
        externalTransactionId: refundG2Ext,
        kind: WagerTransactionKind.Refund,
        amount: '25.00',
        referenceExternalTransactionId: betG2Ext,
      }),
      `idem-${refundG2Ext}`,
    );
    expect(refundG2Res.status).toBe(202);
    const pendingG2Id = refundG2Res.body.transactionId;

    let resolvePendingGate!: (pid: number) => void;
    const pendingGateLocked = new Promise<number>((resolve) => {
      resolvePendingGate = resolve;
    });
    let releasePendingGate!: () => void;
    const holdPendingGate = new Promise<void>((resolve) => {
      releasePendingGate = resolve;
    });

    const gateTx2 = gateSql.begin(async (tx) => {
      const [r] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wager_transactions where id = ${pendingG2Id} for update`;
      resolvePendingGate(r!.pid);
      await holdPendingGate;
    });
    const gate2Pid = await pendingGateLocked;

    let kill2!: AbruptKillEvidence;
    let soleSurvivor!: RunningServiceInstance;
    const crash2StartMs = performance.now();

    try {
      const betG2Res = await submitWagerHttp(
        afterCrash1Nodes[1]!,
        buildWagerPayload(wPending, {
          externalTransactionId: betG2Ext,
          kind: WagerTransactionKind.Bet,
          amount: '25.00',
        }),
        `idem-${betG2Ext}`,
      );
      expect(betG2Res.status).toBe(200);
      const victim2App = await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ application_name: string }>>`
            select application_name
            from pg_stat_activity
            where datname = current_database()
              and ${gate2Pid} = any(pg_blocking_pids(pid))
          `;
          return rows[0]?.application_name;
        },
        { timeoutMs: 6_000, intervalMs: 15, description: 'victim2 blocked in PendingReferenceWorker' },
      );

      const victim2 = afterCrash1Nodes.find((n) => n.pgApplicationName === victim2App)!;
      const remaining = afterCrash1Nodes.filter((n) => n.instanceId !== victim2.instanceId);
      expect(remaining).toHaveLength(1);
      soleSurvivor = remaining[0]!;

      // Enqueue a fresh SQS message on wFinal right as victim2 crashes so soleSurvivor must recover ALL 3 subsystems:
      const envG3 = buildSqsEnvelope(wFinal, {
        externalTransactionId: `g-final-win-${newId().slice(0, 8)}`,
        kind: WagerTransactionKind.Win,
        amount: '45.00',
      });
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, envG3)));

      kill2 = await victim2.killAbruptly();
    } finally {
      releasePendingGate();
      await gateTx2;
    }
    await assertAbruptDeathAndPgCleanup(kill2);

    // Sole surviving process (`soleSurvivor`) recovers PendingReference (`wPending` -> 100.00),
    // consumes SQS (`wFinal` -> 145.00), and drains all Outbox events across all wallets!
    await pollUntil(
      async () => {
        const [pRow] = await appSql<Array<{ status: string }>>`
          select status from wager_transactions where id = ${pendingG2Id}
        `;
        const [fWallet] = await appSql<DbWalletRow[]>`
          select id, balance_amount::text as balance_amount, currency, version
          from wallets where id = ${wFinal.id}
        `;
        return pRow?.status === WagerTransactionStatus.Processed && fWallet?.balance_amount === '145.00'
          ? true
          : undefined;
      },
      { timeoutMs: 8_000, intervalMs: 35, description: 'sole survivor completes PendingReference + SQS + Outbox' },
    );
    const crash2RecoveryMs = Math.round(performance.now() - crash2StartMs);

    const readySole = await soleSurvivor.request('/health/ready');
    expect(readySole.status).toBe(200);
    expect(readySole.headers.get(INSTANCE_ID_HEADER)).toBe(soleSurvivor.instanceId);

    const distinctRoles = new Set([kill1.instanceId, kill2.instanceId, soleSurvivor.instanceId]);
    expect(distinctRoles.size).toBe(3);

    console.log(
      JSON.stringify({
        scenario: '7.4-G',
        crash1: {
          role: 'sqs-consumer',
          killedInstanceId: kill1.instanceId,
          killedPid: kill1.pid,
          exitCode: kill1.exitCode,
          recoveryMs: crash1RecoveryMs,
        },
        crash2: {
          role: 'pending-reference-worker',
          killedInstanceId: kill2.instanceId,
          killedPid: kill2.pid,
          exitCode: kill2.exitCode,
          recoveryMs: crash2RecoveryMs,
        },
        soleSurvivor: {
          role: 'outbox-publisher+sqs-consumer+pending-reference-recovery',
          instanceId: soleSurvivor.instanceId,
          pid: soleSurvivor.pid,
        },
      }),
    );

    await assertFullPostCrashInvariants({
      walletIds: [wSqs.id, wPending.id, wFinal.id, wHelper.id],
      survivingInstance: soleSurvivor,
      queues: activeQueues,
    });
  }, 45_000);
});
