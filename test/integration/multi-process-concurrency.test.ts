import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { SendMessageBatchCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import type { SQL } from 'bun';
import Decimal from 'decimal.js';
import { FailureCode } from '../../src/common/errors/failure-code';
import { newId } from '../../src/common/ids';
import { INSTANCE_ID_HEADER } from '../../src/common/instance/instance-identity';
import {
  toWagerTransactionSendInput,
  WAGER_CONSUMER_NAME,
  WAGER_REQUESTED_MESSAGE_TYPE,
  type WagerTransactionRequestedEnvelope,
} from '../../src/messaging/wager-message';
import type { SubmitWagerResponseView, WagerTransactionView } from '../../src/wagering/wagering.controller';
import { WagerTransactionKind, WagerTransactionStatus } from '../../src/wagering/wager-transaction';
import type { WalletView } from '../../src/wallets/wallets.controller';
import type { WalletReconciliation } from '../../src/wallets/wallet-reconciliation';
import { createTestOrm, createTestSql, resetSchema, testRoleName } from '../support/database';
import {
  createClusterQueues,
  createTestSqsClient,
  drainQueueMessages,
  getQueueDepth,
  type IsolatedClusterQueues,
  MultiProcessCluster,
  pollUntil,
  type RunningServiceInstance,
  withMultiProcessWalletLockBarrier,
} from '../support/multi-process';

interface HttpJsonResult<T = Record<string, unknown>> {
  readonly status: number;
  readonly body: T;
  readonly headers: Headers;
  readonly instanceIdHeader: string | null;
}

describe('Real multi-process concurrency >= 3 OS processes (Stage 7.3 — wagering_test + LocalStack)', () => {
  let migratorOrm: MikroORM;
  let adminSql: SQL;
  let appSql: SQL;
  let gateSql: SQL;
  let sqs: SQSClient;
  let cluster: MultiProcessCluster;
  let activeQueues: IsolatedClusterQueues | undefined;
  let activeProxyStop: (() => void) | undefined;

  beforeAll(async () => {
    migratorOrm = await createTestOrm('migrator');
    adminSql = createTestSql('admin');
    appSql = createTestSql('app');
    gateSql = createTestSql('app');
    sqs = createTestSqsClient();
    cluster = new MultiProcessCluster();

    // Explicit target database and role confirmation before executing any scenario:
    for (const [label, client, expectedUser] of [
      ['appSql', appSql, testRoleName('app')],
      ['gateSql', gateSql, testRoleName('app')],
      ['adminSql', adminSql, testRoleName('admin')],
    ] as const) {
      const [identity] = await client<Array<{ db: string; usr: string }>>`
        select current_database() as db, current_user as usr
      `;
      expect(identity?.db).toBe('wagering_test');
      expect(identity?.usr).toBe(expectedUser);
      if (identity?.db !== 'wagering_test') {
        throw new Error(`Refusing to run on unexpected database "${identity?.db}" (${label})`);
      }
    }
  });

  beforeEach(async () => {
    await resetSchema(migratorOrm);
  });

  afterEach(async () => {
    try {
      await cluster.stopAllGracefully(10_000);
    } catch (err) {
      await cluster.forceKillAll();
      throw err;
    } finally {
      activeProxyStop?.();
      activeProxyStop = undefined;
      if (activeQueues !== undefined) {
        await activeQueues.destroy();
        activeQueues = undefined;
      }
    }

    // Verify zero lingering PostgreSQL connections from any spawned service instance:
    const lingering = await adminSql<Array<{ pid: number; application_name: string }>>`
      select pid::int as pid, application_name
      from pg_stat_activity
      where datname = current_database()
        and application_name like 'wagering:%'
    `;
    expect([...lingering]).toEqual([]);
  });

  afterAll(async () => {
    activeProxyStop?.();
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
      databasePoolMax: number;
      shutdownTimeoutMs: number;
    }> = {},
  ): Promise<RunningServiceInstance[]> {
    expect(count).toBeGreaterThanOrEqual(3);
    const nodes = await Promise.all(
      Array.from({ length: count }, (_unused, idx) =>
        cluster.spawn({
          instanceId: `${prefix}-${idx + 1}`,
          port: 0,
          queues,
          workersEnabled: overrides.workersEnabled ?? true,
          sqsWaitTimeSeconds: overrides.sqsWaitTimeSeconds ?? 1,
          outboxPollIntervalMs: overrides.outboxPollIntervalMs ?? 40,
          pendingReferencePollIntervalMs: overrides.pendingReferencePollIntervalMs ?? 50,
          databasePoolMax: overrides.databasePoolMax ?? 8,
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
    const body = (await res.json()) as T;
    return {
      status: res.status,
      body,
      headers: res.headers,
      instanceIdHeader: res.headers.get(INSTANCE_ID_HEADER),
    };
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
      providerId: overrides.providerId ?? 'provider-73',
      externalTransactionId: overrides.externalTransactionId ?? `ext-${newId()}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: overrides.roundId ?? `round-${newId().slice(0, 8)}`,
      gameId: overrides.gameId ?? 'slots-73',
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
      kind: WagerTransactionKind;
      amount: string;
      referenceExternalTransactionId: string;
    }> = {},
  ): WagerTransactionRequestedEnvelope {
    const extId = overrides.externalTransactionId ?? `ext-sqs-${newId()}`;
    return {
      messageId: overrides.messageId ?? `msg-${newId()}`,
      type: WAGER_REQUESTED_MESSAGE_TYPE,
      occurredAt: new Date().toISOString(),
      correlationId: `corr-${newId().slice(0, 12)}`,
      data: {
        providerId: overrides.providerId ?? 'provider-73',
        externalTransactionId: extId,
        idempotencyKey: overrides.idempotencyKey ?? `idem-${extId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: overrides.roundId ?? `round-${newId().slice(0, 8)}`,
        gameId: 'slots-73',
        kind: overrides.kind ?? WagerTransactionKind.Bet,
        money: { amount: overrides.amount ?? '15.00', currency: wallet.balance.currency },
        ...(overrides.referenceExternalTransactionId !== undefined
          ? { referenceExternalTransactionId: overrides.referenceExternalTransactionId }
          : {}),
      },
    };
  }

  /**
   * Comprehensive direct PostgreSQL + HTTP reconciliation + SQS causal Outbox auditor (Section 6).
   * Never assumes correctness from HTTP 200 alone.
   */
  async function assertFullFinancialAndMessagingInvariants(options: {
    readonly walletIds: readonly string[];
    readonly verifyInstance: RunningServiceInstance;
    readonly queues: IsolatedClusterQueues;
    readonly verifyOutboxDrain?: boolean;
  }): Promise<void> {
    if (options.verifyOutboxDrain !== false) {
      await pollUntil(
        async () => {
          const [row] = await appSql<Array<{ pending: number }>>`
            select count(*)::int as pending
            from outbox_messages
            where published_at is null
          `;
          return row?.pending === 0 ? true : undefined;
        },
        {
          timeoutMs: 12_000,
          intervalMs: 30,
          description: 'all outbox_messages to be published across active instances',
        },
      );
    }

    for (const walletId of options.walletIds) {
      const [walletRow] = await appSql<
        Array<{ id: string; balance_amount: string; currency: string; version: number }>
      >`
        select id, balance_amount::text as balance_amount, currency, version::int as version
        from wallets
        where id = ${walletId}
      `;
      expect(walletRow).toBeDefined();

      const storedBalance = new Decimal(walletRow!.balance_amount);
      expect(storedBalance.gte(0)).toBe(true);

      const ledgerRows = await appSql<
        Array<{
          id: string;
          transaction_id: string;
          direction: 'CREDIT' | 'DEBIT';
          amount: string;
          balance_before: string;
          balance_after: string;
          wallet_version: number;
        }>
      >`
        select id,
               transaction_id,
               direction,
               amount::text as amount,
               balance_before::text as balance_before,
               balance_after::text as balance_after,
               wallet_version::int as wallet_version
        from wallet_ledger_entries
        where wallet_id = ${walletId}
        order by wallet_version asc
      `;

      let runningBalance = new Decimal(0);
      const seenTxIds = new Set<string>();

      if (ledgerRows.length === 0) {
        expect(storedBalance.eq(0)).toBe(true);
        expect(walletRow!.version).toBe(1);
      } else {
        const firstVersion = ledgerRows[0]!.wallet_version;
        expect([1, 2]).toContain(firstVersion);

        for (let idx = 0; idx < ledgerRows.length; idx += 1) {
          const entry = ledgerRows[idx]!;
          expect(entry.wallet_version).toBe(firstVersion + idx);
          expect(new Decimal(entry.balance_before).eq(runningBalance)).toBe(true);

          const amt = new Decimal(entry.amount);
          expect(amt.gt(0)).toBe(true);
          runningBalance =
            entry.direction === 'CREDIT' ? runningBalance.plus(amt) : runningBalance.minus(amt);

          expect(runningBalance.gte(0)).toBe(true);
          expect(new Decimal(entry.balance_after).eq(runningBalance)).toBe(true);
          expect(seenTxIds.has(entry.transaction_id)).toBe(false);
          seenTxIds.add(entry.transaction_id);
        }

        expect(ledgerRows.at(-1)!.wallet_version).toBe(walletRow!.version);
        expect(runningBalance.eq(storedBalance)).toBe(true);
      }

      // Cross-check via HTTP reconciliation endpoint on a live OS process:
      const reconRes = await options.verifyInstance.request(`/wallets/${walletId}/reconciliation`, {
        method: 'POST',
      });
      expect(reconRes.status).toBe(200);
      const recon = (await reconRes.json()) as WalletReconciliation;
      expect(recon.consistent).toBe(true);
      expect(new Decimal(recon.storedBalance.amount).eq(storedBalance)).toBe(true);
      expect(new Decimal(recon.calculatedBalance.amount).eq(storedBalance)).toBe(true);
      expect(recon.difference.amount).toBe('0.00');

      // Verify no REJECTED or LOSS transaction ever produced a ledger row, and every balance-affecting
      // PROCESSED transaction produced exactly 1 ledger row:
      const txRows = await appSql<
        Array<{
          id: string;
          kind: WagerTransactionKind;
          status: WagerTransactionStatus;
          observed_balance_amount: string | null;
        }>
      >`
        select id, kind, status, observed_balance_amount::text as observed_balance_amount
        from wager_transactions
        where wallet_id = ${walletId}
      `;
      for (const tx of txRows) {
        if (tx.status === WagerTransactionStatus.Processed && tx.kind !== WagerTransactionKind.Loss) {
          expect(seenTxIds.has(tx.id)).toBe(true);
        } else {
          expect(seenTxIds.has(tx.id)).toBe(false);
        }
      }
    }

    if (options.verifyOutboxDrain !== false) {
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
        10_000,
      );

      const receivedByWallet = new Map<string, string[]>();
      const distinctReceivedIds = new Set<string>();
      for (const msg of sqsMessages) {
        if (msg.Body === undefined) {
          continue;
        }
        const parsed = JSON.parse(msg.Body) as { eventId: string; aggregateId: string };
        // Deduplicate only if SQS delivered twice; in normal 7.3 operation there must be 0 duplicates too:
        expect(distinctReceivedIds.has(parsed.eventId)).toBe(false);
        distinctReceivedIds.add(parsed.eventId);

        let list = receivedByWallet.get(parsed.aggregateId);
        if (list === undefined) {
          list = [];
          receivedByWallet.set(parsed.aggregateId, list);
        }
        list.push(parsed.eventId);
      }

      // Every persisted outbox event was delivered to SQS with zero loss:
      expect(distinctReceivedIds.size).toBe(expectedOutbox.length);

      // Within each wallet (MessageGroupId = walletId), SQS arrival order strictly matches sequence_number ASC:
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
        expect(receivedByWallet.get(wId) ?? []).toEqual(expectedIds);
      }
    }

    // Confirm zero unexpected residual messages in txQueue and DLQ:
    const [txDepth, dlqDepth] = await Promise.all([
      getQueueDepth(sqs, options.queues.txQueueUrl),
      getQueueDepth(sqs, options.queues.dlqUrl),
    ]);
    expect(txDepth.visible).toBe(0);
    expect(txDepth.notVisible).toBe(0);
    expect(dlqDepth.visible).toBe(0);
    expect(dlqDepth.notVisible).toBe(0);
  }

  test('Cenário A — Concorrência real na mesma wallet com 4 processos simultâneos (exclusão mútua, overdraft, reversão única, LOSS e encadeamento de wallet_version)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });
    const nodes = await spawnClusterNodes('p73-scen-a', 4, activeQueues);

    // Confirm 4 distinct OS processes are simultaneously live in pg_stat_activity:
    const wallet = await openWallet(nodes[0]!, '100.00', 'BRL');

    // Round 1: 8 concurrent BETs of 30.00 BRL (2 per OS process = 240.00 BRL total against 100.00 BRL)
    // held behind a deterministic PostgreSQL wallet lock barrier until all 4 OS processes & 8 backends
    // are simultaneously blocked on `wallets FOR UPDATE`!
    const round1Barrier = await withMultiProcessWalletLockBarrier(
      gateSql,
      adminSql,
      {
        walletId: wallet.id,
        expectedWaiters: 8,
        expectedDistinctInstances: 4,
        timeoutMs: 10_000,
      },
      () =>
        Promise.all(
          Array.from({ length: 8 }, (_unused, i) => {
            const target = nodes[i % nodes.length]!;
            const extId = `bet-r1-${i}-${newId().slice(0, 8)}`;
            return submitWagerHttp(
              target,
              buildWagerPayload(wallet, {
                externalTransactionId: extId,
                kind: WagerTransactionKind.Bet,
                amount: '30.00',
              }),
              `idem-${extId}`,
            );
          }),
        ),
    );

    // Concurrency evidence: all 4 OS process application_names and 8 backend connections competed simultaneously!
    expect(round1Barrier.distinctApplicationNames).toEqual([
      'wagering:p73-scen-a-1',
      'wagering:p73-scen-a-2',
      'wagering:p73-scen-a-3',
      'wagering:p73-scen-a-4',
    ]);
    expect(round1Barrier.waiters).toHaveLength(8);
    expect(new Set(round1Barrier.result.map((r) => r.instanceIdHeader)).size).toBe(4);

    const processedBets = round1Barrier.result.filter((r) => r.status === 200);
    const rejectedBets = round1Barrier.result.filter((r) => r.status === 422);
    expect(processedBets).toHaveLength(3);
    expect(rejectedBets).toHaveLength(5);

    const observedProcessedBalances = processedBets
      .map((r) => (r.body as Extract<SubmitWagerResponseView, { status: WagerTransactionStatus.Processed }>).balance.amount)
      .sort();
    expect(observedProcessedBalances).toEqual(['10.00', '40.00', '70.00']);

    for (const rej of rejectedBets) {
      const body = rej.body as Extract<SubmitWagerResponseView, { status: WagerTransactionStatus.Rejected }>;
      expect(body.status).toBe(WagerTransactionStatus.Rejected);
      expect(body.failureCode).toBe(FailureCode.InsufficientFunds);
      expect(body.balance.amount).toBe('10.00');
    }

    // Identify one of the 3 winning BET externalTransactionIds so Round 2 races REFUND vs ROLLBACK + WIN + LOSS:
    const winningTxId = processedBets[0]!.body.transactionId;
    const winningLookupRes = await nodes[1]!.request(`/wagering/transactions/${winningTxId}`);
    expect(winningLookupRes.status).toBe(200);
    const winningBetView = (await winningLookupRes.json()) as WagerTransactionView;

    // Round 2: 4 simultaneous mixed operations across the 4 OS processes on the same wallet:
    // - Process 1: REFUND (+30.00) referencing winningBetView
    // - Process 2: ROLLBACK (+30.00) referencing winningBetView (must conflict with REFUND!)
    // - Process 3: WIN (+15.00)
    // - Process 4: LOSS (0.00 — must NOT increment wallet_version or create ledger entry)
    const round2Barrier = await withMultiProcessWalletLockBarrier(
      gateSql,
      adminSql,
      {
        walletId: wallet.id,
        expectedWaiters: 4,
        expectedDistinctInstances: 4,
        timeoutMs: 10_000,
      },
      () =>
        Promise.all([
          submitWagerHttp(
            nodes[0]!,
            buildWagerPayload(wallet, {
              externalTransactionId: `refund-r2-${newId().slice(0, 8)}`,
              kind: WagerTransactionKind.Refund,
              amount: '30.00',
              referenceExternalTransactionId: winningBetView.externalTransactionId,
            }),
            `idem-refund-r2-${newId().slice(0, 8)}`,
          ),
          submitWagerHttp(
            nodes[1]!,
            buildWagerPayload(wallet, {
              externalTransactionId: `rollback-r2-${newId().slice(0, 8)}`,
              kind: WagerTransactionKind.Rollback,
              amount: '30.00',
              referenceExternalTransactionId: winningBetView.externalTransactionId,
            }),
            `idem-rollback-r2-${newId().slice(0, 8)}`,
          ),
          submitWagerHttp(
            nodes[2]!,
            buildWagerPayload(wallet, {
              externalTransactionId: `win-r2-${newId().slice(0, 8)}`,
              kind: WagerTransactionKind.Win,
              amount: '15.00',
            }),
            `idem-win-r2-${newId().slice(0, 8)}`,
          ),
          submitWagerHttp(
            nodes[3]!,
            buildWagerPayload(wallet, {
              externalTransactionId: `loss-r2-${newId().slice(0, 8)}`,
              kind: WagerTransactionKind.Loss,
              amount: '0.00',
            }),
            `idem-loss-r2-${newId().slice(0, 8)}`,
          ),
        ]),
    );

    expect(round2Barrier.distinctApplicationNames).toHaveLength(4);
    const [refundRes, rollbackRes, winRes, lossRes] = round2Barrier.result;

    // Exactly ONE of REFUND / ROLLBACK wins (200 PROCESSED) and the other is REJECTED (422 REFERENCE_ALREADY_REVERSED):
    const reversalStatuses = [refundRes.status, rollbackRes.status].sort();
    expect(reversalStatuses).toEqual([200, 422]);
    const rejectedReversal = refundRes.status === 422 ? refundRes : rollbackRes;
    expect(
      (rejectedReversal.body as Extract<SubmitWagerResponseView, { status: WagerTransactionStatus.Rejected }>)
        .failureCode,
    ).toBe(FailureCode.ReferenceAlreadyReversed);

    expect(winRes.status).toBe(200);
    expect(lossRes.status).toBe(200);

    // Round 3: Out-of-order REFUND (202 PENDING_REFERENCE) followed by its referenced BET (-20.00),
    // resolved by the 4 competing PendingReferenceWorker OS processes with zero duplicate credits!
    const lateBetExtId = `late-bet-r3-${newId().slice(0, 8)}`;
    const earlyRefundExtId = `early-refund-r3-${newId().slice(0, 8)}`;
    const earlyRefundRes = await submitWagerHttp(
      nodes[0]!,
      buildWagerPayload(wallet, {
        externalTransactionId: earlyRefundExtId,
        kind: WagerTransactionKind.Refund,
        amount: '20.00',
        referenceExternalTransactionId: lateBetExtId,
      }),
      `idem-${earlyRefundExtId}`,
    );
    expect(earlyRefundRes.status).toBe(202);
    expect(earlyRefundRes.body.status).toBe(WagerTransactionStatus.PendingReference);

    const lateBetRes = await submitWagerHttp(
      nodes[3]!,
      buildWagerPayload(wallet, {
        externalTransactionId: lateBetExtId,
        kind: WagerTransactionKind.Bet,
        amount: '20.00',
      }),
      `idem-${lateBetExtId}`,
    );
    expect(lateBetRes.status).toBe(200);

    // Poll until one of the 4 competing PendingReferenceWorker processes resolves earlyRefundExtId to PROCESSED:
    await pollUntil(
      async () => {
        const res = await nodes[1]!.request(`/wagering/transactions/${earlyRefundRes.body.transactionId}`);
        if (res.status !== 200) {
          return undefined;
        }
        const view = (await res.json()) as WagerTransactionView;
        return view.status === WagerTransactionStatus.Processed ? view : undefined;
      },
      { timeoutMs: 6_000, intervalMs: 30, description: 'PendingReferenceWorker resolution across 4 OS processes' },
    );

    // Final wallet balance: 10.00 + 30.00 (single reversal) + 15.00 (WIN) + 0.00 (LOSS) - 20.00 (late BET) + 20.00 (resolved REFUND) = 55.00 BRL
    // Final wallet version: 1 (OPENING) + 3 (BETs) + 1 (reversal) + 1 (WIN) + 1 (late BET) + 1 (resolved REFUND) = 8 (LOSS does not increment version!)
    const finalWalletRes = await nodes[2]!.request(`/wallets/${wallet.id}`);
    expect(finalWalletRes.status).toBe(200);
    const finalWallet = (await finalWalletRes.json()) as WalletView;
    expect(finalWallet.balance.amount).toBe('55.00');
    expect(finalWallet.version).toBe(8);

    await assertFullFinancialAndMessagingInvariants({
      walletIds: [wallet.id],
      verifyInstance: nodes[3]!,
      queues: activeQueues,
    });
  }, 35_000);

  test('Cenário B — Idempotência distribuída entre 3 processos simultâneos (mesma chave + mesmo payload, saldo histórico imutável e conflito 409 com payload divergente na mesma e em distintas wallets)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });
    const nodes = await spawnClusterNodes('p73-scen-b', 3, activeQueues);

    const wallet1 = await openWallet(nodes[0]!, '100.00', 'BRL');
    const wallet2 = await openWallet(nodes[1]!, '100.00', 'BRL');
    const wallet3 = await openWallet(nodes[2]!, '100.00', 'BRL');

    const sharedKey = `idem-dist-${newId()}`;
    const sharedPayload = buildWagerPayload(wallet1, {
      externalTransactionId: `ext-dist-${newId()}`,
      kind: WagerTransactionKind.Bet,
      amount: '40.00',
    });

    // Part B1: 9 simultaneous submissions (3 per OS process across 3 distinct OS processes)
    // carrying the exact same Idempotency-Key and payload behind a PostgreSQL wallet lock barrier:
    const samePayloadBarrier = await withMultiProcessWalletLockBarrier(
      gateSql,
      adminSql,
      {
        walletId: wallet1.id,
        expectedWaiters: 9,
        expectedDistinctInstances: 3,
        timeoutMs: 10_000,
      },
      () =>
        Promise.all(
          Array.from({ length: 9 }, (_unused, idx) =>
            submitWagerHttp(nodes[idx % 3]!, sharedPayload, sharedKey),
          ),
        ),
    );

    expect(samePayloadBarrier.distinctApplicationNames).toEqual([
      'wagering:p73-scen-b-1',
      'wagering:p73-scen-b-2',
      'wagering:p73-scen-b-3',
    ]);
    expect(samePayloadBarrier.waiters).toHaveLength(9);

    const firstApplications = samePayloadBarrier.result.filter((r) => r.body.idempotentReplay === false);
    const replays = samePayloadBarrier.result.filter((r) => r.body.idempotentReplay === true);
    expect(firstApplications).toHaveLength(1);
    expect(replays).toHaveLength(8);

    const canonicalTxId = firstApplications[0]!.body.transactionId;
    for (const res of samePayloadBarrier.result) {
      expect(res.status).toBe(200);
      expect(res.body.transactionId).toBe(canonicalTxId);
      expect(
        (res.body as Extract<SubmitWagerResponseView, { status: WagerTransactionStatus.Processed }>).balance,
      ).toEqual({ amount: '60.00', currency: 'BRL' });
    }

    // Advance wallet1's balance with a WIN (+25.00 -> 85.00), then replay sharedKey across all 3 processes:
    // Every process MUST return the historical observed balance ('60.00'), NEVER the current balance ('85.00')!
    const winRes = await submitWagerHttp(
      nodes[1]!,
      buildWagerPayload(wallet1, {
        externalTransactionId: `ext-after-idem-${newId()}`,
        kind: WagerTransactionKind.Win,
        amount: '25.00',
      }),
      `idem-win-${newId()}`,
    );
    expect(winRes.status).toBe(200);

    const historicalReplays = await Promise.all(
      nodes.map((node) => submitWagerHttp(node, sharedPayload, sharedKey)),
    );
    for (const replay of historicalReplays) {
      expect(replay.status).toBe(200);
      expect(replay.body.idempotentReplay).toBe(true);
      expect(replay.body.transactionId).toBe(canonicalTxId);
      expect(
        (replay.body as Extract<SubmitWagerResponseView, { status: WagerTransactionStatus.Processed }>).balance
          .amount,
      ).toBe('60.00');
    }

    // Part B2: Divergent payloads for the existing sharedKey across all 3 processes -> all return 409:
    const divergentExisting = await Promise.all([
      submitWagerHttp<{ code: string }>(
        nodes[0]!,
        { ...sharedPayload, money: { amount: '41.00', currency: 'BRL' } },
        sharedKey,
      ),
      submitWagerHttp<{ code: string }>(
        nodes[1]!,
        { ...sharedPayload, kind: WagerTransactionKind.Win },
        sharedKey,
      ),
      submitWagerHttp<{ code: string }>(
        nodes[2]!,
        { ...sharedPayload, roundId: 'tampered-round-id' },
        sharedKey,
      ),
    ]);
    for (const conflict of divergentExisting) {
      expect(conflict.status).toBe(409);
      expect(conflict.body.code).toBe(FailureCode.IdempotencyPayloadConflict);
    }

    // Part B3: Brand-new Idempotency-Key raced simultaneously across 3 DIFFERENT wallets on 3 processes
    // (unserialized by wallet lock; resolved via SAVEPOINT + wager_transactions_idempotency_key_uq!):
    const crossWalletKey = `idem-cross-wallet-${newId()}`;
    const crossWalletRace = await Promise.all([
      submitWagerHttp<Record<string, unknown>>(
        nodes[0]!,
        buildWagerPayload(wallet1, { externalTransactionId: `cw-1-${newId()}`, amount: '10.00' }),
        crossWalletKey,
      ),
      submitWagerHttp<Record<string, unknown>>(
        nodes[1]!,
        buildWagerPayload(wallet2, { externalTransactionId: `cw-2-${newId()}`, amount: '10.00' }),
        crossWalletKey,
      ),
      submitWagerHttp<Record<string, unknown>>(
        nodes[2]!,
        buildWagerPayload(wallet3, { externalTransactionId: `cw-3-${newId()}`, amount: '10.00' }),
        crossWalletKey,
      ),
    ]);

    const crossWalletWinners = crossWalletRace.filter((r) => r.status === 200);
    const crossWalletConflicts = crossWalletRace.filter((r) => r.status === 409);
    expect(crossWalletWinners).toHaveLength(1);
    expect(crossWalletConflicts).toHaveLength(2);
    for (const c of crossWalletConflicts) {
      expect(c.body.code).toBe(FailureCode.IdempotencyPayloadConflict);
    }

    // Exactly 1 row exists in PostgreSQL for sharedKey and 1 row for crossWalletKey:
    const [keyCounts] = await appSql<Array<{ shared_cnt: number; cross_cnt: number }>>`
      select
        count(*) filter (where idempotency_key = ${sharedKey})::int as shared_cnt,
        count(*) filter (where idempotency_key = ${crossWalletKey})::int as cross_cnt
      from wager_transactions
    `;
    expect(keyCounts).toEqual({ shared_cnt: 1, cross_cnt: 1 });

    await assertFullFinancialAndMessagingInvariants({
      walletIds: [wallet1.id, wallet2.id, wallet3.id],
      verifyInstance: nodes[0]!,
      queues: activeQueues,
    });
  }, 35_000);

  test('Cenário C — Operações independentes em wallets distintas sem lock global (wallet bloqueada não impede paralelismo entre 3 processos nas demais wallets)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });
    const nodes = await spawnClusterNodes('p73-scen-c', 3, activeQueues);

    const hotWallet = await openWallet(nodes[0]!, '100.00', 'BRL');
    const freeWallets = await Promise.all(
      Array.from({ length: 6 }, (_u, idx) => openWallet(nodes[idx % 3]!, '100.00', 'BRL')),
    );

    // Lock ONLY hotWallet in PostgreSQL and put a request on Process 1 actively into the lock queue:
    let resolveHotLocked!: (pid: number) => void;
    const hotLocked = new Promise<number>((resolve) => {
      resolveHotLocked = resolve;
    });
    let releaseHotWallet!: () => void;
    const holdHotWallet = new Promise<void>((resolve) => {
      releaseHotWallet = resolve;
    });

    const hotGateTx = gateSql.begin(async (tx) => {
      const [row] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wallets where id = ${hotWallet.id} for update`;
      resolveHotLocked(row!.pid);
      await holdHotWallet;
    });
    const hotGatePid = await hotLocked;

    let hotBetSettled = false;
    const hotBetExtId = `hot-bet-${newId()}`;
    const hotBetPromise = submitWagerHttp(
      nodes[0]!,
      buildWagerPayload(hotWallet, { externalTransactionId: hotBetExtId, amount: '25.00' }),
      `idem-${hotBetExtId}`,
    ).then((res) => {
      hotBetSettled = true;
      return res;
    });

    try {
      // Deterministically confirm hotWallet's transaction is actively blocked inside PostgreSQL:
      await pollUntil(
        async () => {
          const blocked = await adminSql<Array<{ pid: number; application_name: string }>>`
            select pid::int as pid, application_name
            from pg_stat_activity
            where datname = current_database()
              and ${hotGatePid} = any(pg_blocking_pids(pid))
          `;
          return blocked.length === 1 ? blocked[0] : undefined;
        },
        { timeoutMs: 5_000, intervalMs: 10, description: 'hotWallet request blocked on row lock' },
      );

      // WHILE hotWallet remains locked (hotBetSettled === false!), execute 18 concurrent operations
      // across all 3 OS processes (including nodes[0]!) on the 6 independent freeWallets:
      const parallelStart = performance.now();
      const freeOps = await Promise.all(
        freeWallets.flatMap((fw, wIdx) =>
          [0, 1, 2].map((step) => {
            const target = nodes[(wIdx + step) % 3]!;
            const extId = `free-${wIdx}-${step}-${newId().slice(0, 8)}`;
            return submitWagerHttp(
              target,
              buildWagerPayload(fw, {
                externalTransactionId: extId,
                kind: step === 2 ? WagerTransactionKind.Win : WagerTransactionKind.Bet,
                amount: step === 2 ? '15.00' : '20.00',
              }),
              `idem-${extId}`,
            );
          }),
        ),
      );
      const parallelElapsedMs = performance.now() - parallelStart;

      // Proof of zero global lock: all 18 operations across all 3 processes completed BEFORE hotWallet was unlocked!
      expect(hotBetSettled).toBe(false);
      expect(parallelElapsedMs).toBeLessThan(5_000);
      expect(freeOps.every((r) => r.status === 200)).toBe(true);
      expect(new Set(freeOps.map((r) => r.instanceIdHeader))).toEqual(
        new Set(['p73-scen-c-1', 'p73-scen-c-2', 'p73-scen-c-3']),
      );
    } finally {
      releaseHotWallet();
      await hotGateTx;
    }

    const hotBetRes = await hotBetPromise;
    expect(hotBetRes.status).toBe(200);
    expect(
      (hotBetRes.body as Extract<SubmitWagerResponseView, { status: WagerTransactionStatus.Processed }>).balance
        .amount,
    ).toBe('75.00');

    // Every freeWallet applied 2 BETs (-20, -20) and 1 WIN (+15) => 100 - 40 + 15 = 75.00 BRL:
    for (const fw of freeWallets) {
      const wRes = await nodes[1]!.request(`/wallets/${fw.id}`);
      expect(wRes.status).toBe(200);
      expect(((await wRes.json()) as WalletView).balance.amount).toBe('75.00');
    }

    await assertFullFinancialAndMessagingInvariants({
      walletIds: [hotWallet.id, ...freeWallets.map((w) => w.id)],
      verifyInstance: nodes[2]!,
      queues: activeQueues,
    });
  }, 35_000);

  test('Cenário D — Inbox e consumo distribuído SQS FIFO com 3 consumidores reais (ordem causal por MessageGroupId, paralelismo entre grupos e deduplicação distribuída)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });
    const nodes = await spawnClusterNodes('p73-scen-d', 3, activeQueues, {
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
    });

    // Part D1: 6 wallets opened at 50.00 BRL. Each wallet receives a strict 4-step causal chain in SQS FIFO:
    //   1) WIN +60.00 (50 -> 110)
    //   2) BET -100.00 (110 -> 10; WOULD FAIL INSUFFICIENT_FUNDS if processed before step 1!)
    //   3) REFUND +100.00 referencing step 2 (10 -> 110; DEPENDS on step 2!)
    //   4) BET -40.00 (110 -> 70)
    const wallets = await Promise.all(
      Array.from({ length: 6 }, (_u, idx) => openWallet(nodes[idx % 3]!, '50.00', 'BRL')),
    );

    const expectedTerminalExtIds: string[] = [];
    for (const [wIdx, w] of wallets.entries()) {
      const bet2ExtId = `d-bet2-${wIdx}-${newId().slice(0, 8)}`;
      const bet4ExtId = `d-bet4-${wIdx}-${newId().slice(0, 8)}`;
      expectedTerminalExtIds.push(bet4ExtId);

      const chain: WagerTransactionRequestedEnvelope[] = [
        buildSqsEnvelope(w, {
          externalTransactionId: `d-win1-${wIdx}-${newId().slice(0, 8)}`,
          kind: WagerTransactionKind.Win,
          amount: '60.00',
        }),
        buildSqsEnvelope(w, {
          externalTransactionId: bet2ExtId,
          kind: WagerTransactionKind.Bet,
          amount: '100.00',
        }),
        buildSqsEnvelope(w, {
          externalTransactionId: `d-ref3-${wIdx}-${newId().slice(0, 8)}`,
          kind: WagerTransactionKind.Refund,
          amount: '100.00',
          referenceExternalTransactionId: bet2ExtId,
        }),
        buildSqsEnvelope(w, {
          externalTransactionId: bet4ExtId,
          kind: WagerTransactionKind.Bet,
          amount: '40.00',
        }),
      ];

      await sqs.send(
        new SendMessageBatchCommand({
          QueueUrl: activeQueues.txQueueUrl,
          Entries: chain.map((env, stepIdx) => {
            const single = toWagerTransactionSendInput(activeQueues!.txQueueUrl, env);
            return {
              Id: `w${wIdx}-s${stepIdx}`,
              MessageBody: single.MessageBody!,
              MessageGroupId: single.MessageGroupId!,
              MessageDeduplicationId: single.MessageDeduplicationId!,
            };
          }),
        }),
      );
    }

    // Wait deterministically until all 24 causal SQS messages are PROCESSED in PostgreSQL:
    await pollUntil(
      async () => {
        const [row] = await appSql<Array<{ processed: number }>>`
          select count(*)::int as processed
          from wager_transactions
          where kind <> 'OPENING'
            and status = 'PROCESSED'
        `;
        return (row?.processed ?? 0) >= 24 ? true : undefined;
      },
      { timeoutMs: 15_000, intervalMs: 40, description: '24 SQS FIFO causal chain transactions processed' },
    );

    // Every wallet must stand at exact balance 70.00 BRL and version 5 (1 OPENING + 4 movements):
    for (const w of wallets) {
      const wRes = await nodes[0]!.request(`/wallets/${w.id}`);
      expect(wRes.status).toBe(200);
      const view = (await wRes.json()) as WalletView;
      expect(view.balance.amount).toBe('70.00');
      expect(view.version).toBe(5);
    }

    // Part D2: Distributed Inbox deduplication across the 3 consumers:
    // Enqueue 3 SQS deliveries carrying the EXACT SAME logical envelope.messageId & idempotencyKey
    // (with distinct SQS MessageDeduplicationIds to simulate post-5-minute SQS redeliveries across consumers),
    // plus a different messageId carrying the same idempotencyKey:
    const dedupWallet = wallets[0]!;
    const canonicalEnvelope = buildSqsEnvelope(dedupWallet, {
      messageId: `msg-dup-shared-${newId()}`,
      externalTransactionId: `ext-dup-shared-${newId()}`,
      idempotencyKey: `idem-dup-shared-${newId()}`,
      kind: WagerTransactionKind.Bet,
      amount: '15.00',
    });

    for (let copy = 0; copy < 3; copy += 1) {
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: activeQueues.txQueueUrl,
          MessageBody: JSON.stringify(canonicalEnvelope),
          MessageGroupId: dedupWallet.id,
          MessageDeduplicationId: `redelivery-sim-${copy}-${newId()}`,
        }),
      );
    }

    const secondMsgSameIdem: WagerTransactionRequestedEnvelope = {
      ...canonicalEnvelope,
      messageId: `msg-dup-second-${newId()}`,
    };
    await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, secondMsgSameIdem)));

    // Wait until both logical messageIds are recorded as processed in inbox_messages and txQueue is empty:
    await pollUntil(
      async () => {
        const [inboxCheck] = await appSql<Array<{ cnt: number }>>`
          select count(*)::int as cnt
          from inbox_messages
          where consumer_name = ${WAGER_CONSUMER_NAME}
            and message_id in (${canonicalEnvelope.messageId}, ${secondMsgSameIdem.messageId})
            and processed_at is not null
        `;
        const depth = await getQueueDepth(sqs, activeQueues!.txQueueUrl);
        return inboxCheck?.cnt === 2 && depth.visible === 0 && depth.notVisible === 0 ? true : undefined;
      },
      { timeoutMs: 12_000, intervalMs: 40, description: 'duplicate SQS deliveries deduplicated and drained' },
    );

    // Verify only ONE financial debit of 15.00 occurred (70.00 -> 55.00) and only 1 wager_transactions row exists:
    const [txDedupCount] = await appSql<Array<{ cnt: number }>>`
      select count(*)::int as cnt
      from wager_transactions
      where idempotency_key = ${canonicalEnvelope.data.idempotencyKey}
    `;
    expect(txDedupCount?.cnt).toBe(1);

    const dedupWalletAfter = (await (await nodes[1]!.request(`/wallets/${dedupWallet.id}`)).json()) as WalletView;
    expect(dedupWalletAfter.balance.amount).toBe('55.00');
    expect(dedupWalletAfter.version).toBe(6);

    // Verify all inbox_messages rows use the fixed consumer_name 'wager-transactions' and processed_at >= received_at:
    const [inboxViolations] = await appSql<Array<{ bad: number; total: number }>>`
      select
        count(*) filter (
          where consumer_name <> ${WAGER_CONSUMER_NAME}
             or processed_at is null
             or processed_at < received_at
        )::int as bad,
        count(*)::int as total
      from inbox_messages
    `;
    expect(inboxViolations?.bad).toBe(0);
    expect(inboxViolations?.total).toBe(26); // 24 chain messages + 2 distinct dedup messageIds

    await assertFullFinancialAndMessagingInvariants({
      walletIds: wallets.map((w) => w.id),
      verifyInstance: nodes[2]!,
      queues: activeQueues,
    });
  }, 40_000);

  test('Cenário E — Outbox e publicação distribuída com 3 publicadores reais simultâneos (FOR UPDATE SKIP LOCKED, paralelismo entre wallets e ordem estrita de sequence_number)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 30 });

    // Phase 1: Seed 9 wallets with 3 transactions each (OPENING -> 2 events, BET -> 2 events, WIN -> 2 events
    // = 6 causal events per wallet = 54 unpublished Outbox events) using 3 HTTP-only instances (WORKERS_ENABLED=false)
    // so all 54 events accumulate deterministically in `outbox_messages` with `published_at IS NULL`:
    const seedNodes = await spawnClusterNodes('p73-scen-e-seed', 3, activeQueues, {
      workersEnabled: false,
    });

    const wallets = await Promise.all(
      Array.from({ length: 9 }, (_u, idx) => openWallet(seedNodes[idx % 3]!, '100.00', 'BRL')),
    );

    await Promise.all(
      wallets.map(async (w, idx) => {
        const node = seedNodes[idx % 3]!;
        const betExt = `e-bet-${idx}-${newId().slice(0, 8)}`;
        const winExt = `e-win-${idx}-${newId().slice(0, 8)}`;

        const r1 = await submitWagerHttp(
          node,
          buildWagerPayload(w, { externalTransactionId: betExt, kind: WagerTransactionKind.Bet, amount: '30.00' }),
          `idem-${betExt}`,
        );
        expect(r1.status).toBe(200);

        const r2 = await submitWagerHttp(
          node,
          buildWagerPayload(w, { externalTransactionId: winExt, kind: WagerTransactionKind.Win, amount: '45.00' }),
          `idem-${winExt}`,
        );
        expect(r2.status).toBe(200);
      }),
    );

    // Stop the HTTP-only seeders before starting the 3 concurrent Outbox publisher processes:
    await cluster.stopAllGracefully(8_000);

    const [beforePublish] = await appSql<Array<{ total: number; unpublished: number }>>`
      select count(*)::int as total,
             count(*) filter (where published_at is null)::int as unpublished
      from outbox_messages
    `;
    expect(beforePublish).toEqual({ total: 54, unpublished: 54 });

    // Phase 2: Spawn 3 real OS publisher processes simultaneously (`WORKERS_ENABLED=true`, fast poll)
    // competing directly on the 54 backlog rows via `FOR UPDATE OF o SKIP LOCKED`:
    const pubNodes = await spawnClusterNodes('p73-scen-e-pub', 3, activeQueues, {
      workersEnabled: true,
      outboxPollIntervalMs: 20,
    });

    // Confirm all 3 publisher processes have active PostgreSQL connections simultaneously while/after draining:
    await pollUntil(
      async () => {
        const [row] = await appSql<Array<{ unpublished: number }>>`
          select count(*)::int as unpublished
          from outbox_messages
          where published_at is null
        `;
        return row?.unpublished === 0 ? true : undefined;
      },
      { timeoutMs: 12_000, intervalMs: 25, description: 'all 54 outbox events published by 3 publisher processes' },
    );

    const activePubApps = await adminSql<Array<{ application_name: string }>>`
      select distinct application_name
      from pg_stat_activity
      where datname = current_database()
        and application_name like 'wagering:p73-scen-e-pub-%'
      order by application_name asc
    `;
    expect(activePubApps.map((r) => r.application_name)).toEqual([
      'wagering:p73-scen-e-pub-1',
      'wagering:p73-scen-e-pub-2',
      'wagering:p73-scen-e-pub-3',
    ]);

    // Verify 0 failed attempts, 0 quarantined rows, and strict causal FIFO arrival order across all 9 wallets:
    const [outboxHealth] = await appSql<Array<{ failed_attempts: number; published: number }>>`
      select count(*) filter (where attempts > 0)::int as failed_attempts,
             count(*) filter (where published_at is not null)::int as published
      from outbox_messages
    `;
    expect(outboxHealth).toEqual({ failed_attempts: 0, published: 54 });

    await assertFullFinancialAndMessagingInvariants({
      walletIds: wallets.map((w) => w.id),
      verifyInstance: pubNodes[0]!,
      queues: activeQueues,
    });
  }, 40_000);

  test('Cenário F — Contenção real de locks PostgreSQL entre 3 processos, renovação de lease SQS, fencing de ownership obsoleto e continuidade operacional', async () => {
    // 2-second VisibilityTimeout so heartbeat (~666ms) and lease expiry (~2000ms) exercise real timing fast:
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 2, maxReceiveCount: 5 });

    // Local pass-through HTTP proxy to LocalStack exclusively for Process 1 (`p73-scen-f-1`),
    // allowing deterministic simulation of partial SQS ChangeMessageVisibilityBatch failure for M2
    // while keeping M1's lease genuinely renewed in LocalStack!
    const upstreamEndpoint = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';
    const m1ExternalId = `f-m1-${newId().slice(0, 8)}`;
    const m2ExternalId = `f-m2-${newId().slice(0, 8)}`;
    let m2ReceiptHandle: string | undefined;
    let failVisibilityRenewalsOnNode1 = true;

    const forwardToUpstream = async (req: Request, rawBody: ArrayBuffer | string): Promise<Response> => {
      const targetUrl = new URL(req.url);
      const upstreamUrl = new URL(targetUrl.pathname + targetUrl.search, upstreamEndpoint);
      const headers = new Headers(req.headers);
      headers.set('host', upstreamUrl.host);
      headers.delete('content-length');
      const hasBody = typeof rawBody === 'string' ? rawBody.length > 0 : rawBody.byteLength > 0;
      return fetch(upstreamUrl.toString(), {
        method: req.method,
        headers,
        body: hasBody ? rawBody : undefined,
      });
    };

    const sqsProxy = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch: async (req) => {
        const bodyBuf = await req.arrayBuffer();
        const bodyText = new TextDecoder().decode(bodyBuf);
        const amzTarget = req.headers.get('x-amz-target') ?? '';

        if (amzTarget.includes('ReceiveMessage')) {
          const upstreamRes = await forwardToUpstream(req, bodyBuf);
          const respText = await upstreamRes.text();
          try {
            const parsed = JSON.parse(respText) as {
              Messages?: Array<{ Body?: string; ReceiptHandle?: string }>;
            };
            for (const msg of parsed.Messages ?? []) {
              if (msg.Body?.includes(m2ExternalId) && typeof msg.ReceiptHandle === 'string') {
                m2ReceiptHandle = msg.ReceiptHandle;
              }
            }
          } catch {
            // Ignore non-JSON payloads.
          }
          return new Response(respText, {
            status: upstreamRes.status,
            headers: upstreamRes.headers,
          });
        }

        if (
          failVisibilityRenewalsOnNode1 &&
          amzTarget.includes('ChangeMessageVisibilityBatch') &&
          m2ReceiptHandle !== undefined
        ) {
          const parsedReq = JSON.parse(bodyText) as {
            QueueUrl: string;
            Entries: Array<{ Id: string; ReceiptHandle: string; VisibilityTimeout: number }>;
          };
          const allowed = parsedReq.Entries.filter(
            (e) => e.ReceiptHandle !== m2ReceiptHandle || e.VisibilityTimeout === 0,
          );
          const blocked = parsedReq.Entries.filter(
            (e) => e.ReceiptHandle === m2ReceiptHandle && e.VisibilityTimeout > 0,
          );

          let successful: Array<Record<string, unknown>> = [];
          let failed: Array<Record<string, unknown>> = [];

          if (allowed.length > 0) {
            const upstreamRes = await forwardToUpstream(
              req,
              JSON.stringify({ QueueUrl: parsedReq.QueueUrl, Entries: allowed }),
            );
            const upstreamJson = (await upstreamRes.json()) as {
              Successful?: Array<Record<string, unknown>>;
              Failed?: Array<Record<string, unknown>>;
            };
            successful = upstreamJson.Successful ?? [];
            failed = upstreamJson.Failed ?? [];
          }

          for (const b of blocked) {
            failed.push({
              Id: b.Id,
              Code: 'ServiceUnavailable',
              SenderFault: false,
              Message: 'simulated transient visibility renewal failure for M2 on node1',
            });
          }

          return new Response(JSON.stringify({ Successful: successful, Failed: failed }), {
            status: 200,
            headers: { 'Content-Type': 'application/x-amz-json-1.0' },
          });
        }

        return forwardToUpstream(req, bodyBuf);
      },
    });
    activeProxyStop = () => sqsProxy.stop(true);
    const proxyEndpoint = `http://127.0.0.1:${sqsProxy.port}`;

    // Phase F1 (Fencing of stale local ownership across 3 real OS processes):
    // 1. Spawn Process 1 (via sqsProxy with renewal blocked for M2), lock walletFence1, and enqueue [M1, M2]
    //    so Process 1 deterministically holds [M1 (walletFence1), M2 (walletFence2)] in memory:
    const node1 = await cluster.spawn({
      instanceId: 'p73-scen-f-1',
      port: 0,
      queues: activeQueues,
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
      envOverrides: { AWS_ENDPOINT_URL: proxyEndpoint },
    });

    const walletFence1 = await openWallet(node1, '100.00', 'BRL');
    const walletFence2 = await openWallet(node1, '100.00', 'BRL');

    let resolveFence1Locked!: (pid: number) => void;
    const fence1Locked = new Promise<number>((resolve) => {
      resolveFence1Locked = resolve;
    });
    let releaseFence1!: () => void;
    const holdFence1 = new Promise<void>((resolve) => {
      releaseFence1 = resolve;
    });

    const fence1GateTx = gateSql.begin(async (tx) => {
      const [r] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wallets where id = ${walletFence1.id} for update`;
      resolveFence1Locked(r!.pid);
      await holdFence1;
    });
    const fence1GatePid = await fence1Locked;

    const envM1 = buildSqsEnvelope(walletFence1, {
      externalTransactionId: m1ExternalId,
      amount: '25.00',
    });
    const envM2 = buildSqsEnvelope(walletFence2, {
      externalTransactionId: m2ExternalId,
      amount: '40.00',
    });

    await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: activeQueues.txQueueUrl,
        Entries: [envM1, envM2].map((env, idx) => {
          const single = toWagerTransactionSendInput(activeQueues!.txQueueUrl, env);
          return {
            Id: `f-${idx}`,
            MessageBody: single.MessageBody!,
            MessageGroupId: single.MessageGroupId!,
            MessageDeduplicationId: single.MessageDeduplicationId!,
          };
        }),
      }),
    );

    // Wait until Process 1 is actively blocked on walletFence1's row lock while holding BOTH [M1, M2] in flight:
    await pollUntil(
      async () => {
        const [rows, depth] = await Promise.all([
          adminSql<Array<{ pid: number }>>`
            select pid::int as pid
            from pg_stat_activity
            where datname = current_database()
              and application_name = ${node1.pgApplicationName}
              and ${fence1GatePid} = any(pg_blocking_pids(pid))
          `,
          getQueueDepth(sqs, activeQueues!.txQueueUrl),
        ]);
        return rows.length >= 1 && depth.notVisible === 2 && depth.visible === 0 ? true : undefined;
      },
      { timeoutMs: 6_000, intervalMs: 15, description: 'node1 blocked on walletFence1 lock while holding [M1, M2]' },
    );

    // 2. Spawn Process 2 and Process 3 so all 3 real OS processes are simultaneously active!
    const [node2, node3] = await Promise.all([
      cluster.spawn({
        instanceId: 'p73-scen-f-2',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
      }),
      cluster.spawn({
        instanceId: 'p73-scen-f-3',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
      }),
    ]);

    try {
      // Because node1's visibility heartbeat is failing, M2's 2s visibility lease expires in LocalStack
      // and node2 or node3 acquires and processes M2 WHILE node1 is still blocked on walletFence1!
      await pollUntil(
        async () => {
          const [m2Row] = await appSql<Array<{ status: string }>>`
            select status
            from wager_transactions
            where external_transaction_id = ${envM2.data.externalTransactionId}
          `;
          return m2Row?.status === WagerTransactionStatus.Processed ? true : undefined;
        },
        {
          timeoutMs: 8_000,
          intervalMs: 40,
          description: 'M2 taken over and PROCESSED by surviving consumer after node1 lease expired',
        },
      );
    } finally {
      // Restore healthy SQS connectivity on node1 BEFORE releasing walletFence1 so node1's post-commit
      // or subsequent cycles recover cleanly, while M2's local lease on node1 is already expired/lost!
      failVisibilityRenewalsOnNode1 = false;
      releaseFence1();
      await fence1GateTx;
    }

    // Wait until M1 is also PROCESSED and node1 logs the stale-lease fencing warning for M2:
    await pollUntil(
      async () => {
        const [m1Row] = await appSql<Array<{ status: string }>>`
          select status
          from wager_transactions
          where external_transaction_id = ${envM1.data.externalTransactionId}
        `;
        const fencedInLogs = node1.logs().some((entry) => {
          const msg = entry.message as Record<string, unknown> | undefined;
          return (
            typeof msg === 'object' &&
            msg !== null &&
            msg.message === 'SQS message visibility lease lost; revoking local batch ownership for group tail' &&
            msg.messageGroupId === walletFence2.id
          );
        });
        return m1Row?.status === WagerTransactionStatus.Processed && fencedInLogs ? true : undefined;
      },
      { timeoutMs: 8_000, intervalMs: 40, description: 'M1 processed and M2 fenced on node1' },
    );

    // Phase F2 (3-way simultaneous PostgreSQL lock contention + healthy SQS visibility lease renewal > 2s):
    // Lock `walletContended` (150.00 BRL), send 1 SQS message + 2 HTTP requests across the 3 processes,
    // confirm all 3 distinct OS processes wait on the same wallet lock past the 2s initial VisibilityTimeout,
    // then release and confirm all 3 processes complete cleanly and remain operational!
    const walletContended = await openWallet(node2, '150.00', 'BRL');
    const sqsContendedEnv = buildSqsEnvelope(walletContended, {
      externalTransactionId: `f-contended-sqs-${newId().slice(0, 8)}`,
      kind: WagerTransactionKind.Bet,
      amount: '20.00',
    });

    let resolveContendedLocked!: (pid: number) => void;
    const contendedLocked = new Promise<number>((resolve) => {
      resolveContendedLocked = resolve;
    });
    let releaseContended!: () => void;
    const holdContended = new Promise<void>((resolve) => {
      releaseContended = resolve;
    });

    const contendedGateTx = gateSql.begin(async (tx) => {
      const [r] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wallets where id = ${walletContended.id} for update`;
      resolveContendedLocked(r!.pid);
      await holdContended;
    });
    const contendedGatePid = await contendedLocked;

    let httpContendersPromise: Promise<[HttpJsonResult, HttpJsonResult]> | undefined;
    try {
      await sqs.send(new SendMessageCommand(toWagerTransactionSendInput(activeQueues.txQueueUrl, sqsContendedEnv)));

      // Deterministically find which of the 3 OS processes picked up sqsContendedEnv and blocked on walletContended:
      const sqsOwnerApp = await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ application_name: string }>>`
            select application_name
            from pg_stat_activity
            where datname = current_database()
              and ${contendedGatePid} = any(pg_blocking_pids(pid))
          `;
          return rows[0]?.application_name;
        },
        { timeoutMs: 6_000, intervalMs: 15, description: 'SQS consumer blocked on walletContended' },
      );

      const sqsLockAcquiredAt = performance.now();
      const otherNodes = [node1, node2, node3].filter((n) => n.pgApplicationName !== sqsOwnerApp);
      expect(otherNodes).toHaveLength(2);

      // Launch HTTP wagers on the OTHER 2 OS processes so ALL 3 processes wait on walletContended simultaneously:
      const extHttp1 = `f-contended-h1-${newId().slice(0, 8)}`;
      const extHttp2 = `f-contended-h2-${newId().slice(0, 8)}`;
      httpContendersPromise = Promise.all([
        submitWagerHttp(
          otherNodes[0]!,
          buildWagerPayload(walletContended, {
            externalTransactionId: extHttp1,
            kind: WagerTransactionKind.Bet,
            amount: '30.00',
          }),
          `idem-${extHttp1}`,
        ),
        submitWagerHttp(
          otherNodes[1]!,
          buildWagerPayload(walletContended, {
            externalTransactionId: extHttp2,
            kind: WagerTransactionKind.Win,
            amount: '15.00',
          }),
          `idem-${extHttp2}`,
        ),
      ]);

      // Confirm all 3 distinct OS processes are simultaneously waiting in lock_tree AND keep holding
      // past the 2,000ms initial SQS VisibilityTimeout (>= 2,350ms) so heartbeat renewal is proven:
      const threeWayWaiters = await pollUntil(
        async () => {
          const rows = await adminSql<Array<{ pid: number; application_name: string }>>`
            with recursive lock_tree as (
              select pid::int as pid, application_name
              from pg_stat_activity
              where datname = current_database()
                and pid <> ${contendedGatePid}
                and ${contendedGatePid} = any(pg_blocking_pids(pid))
              union
              select a.pid::int as pid, a.application_name
              from pg_stat_activity a
              join lock_tree lt on lt.pid = any(pg_blocking_pids(a.pid))
              where a.datname = current_database()
                and a.pid <> ${contendedGatePid}
            )
            select pid, application_name from lock_tree
          `;
          const apps = new Set(rows.map((r) => r.application_name));
          if (rows.length >= 3 && apps.size === 3 && performance.now() - sqsLockAcquiredAt >= 2_350) {
            return rows;
          }
          return undefined;
        },
        {
          timeoutMs: 8_000,
          intervalMs: 25,
          description: 'all 3 OS processes simultaneously waiting > 2.35s on walletContended lock',
        },
      );
      expect(new Set(threeWayWaiters.map((w) => w.application_name))).toEqual(
        new Set(['wagering:p73-scen-f-1', 'wagering:p73-scen-f-2', 'wagering:p73-scen-f-3']),
      );
    } finally {
      releaseContended();
      await contendedGateTx;
    }

    const [httpRes1, httpRes2] = await httpContendersPromise!;
    expect(httpRes1.status).toBe(200);
    expect(httpRes2.status).toBe(200);

    await pollUntil(
      async () => {
        const [sqsRow] = await appSql<Array<{ status: string }>>`
          select status
          from wager_transactions
          where external_transaction_id = ${sqsContendedEnv.data.externalTransactionId}
        `;
        const depth = await getQueueDepth(sqs, activeQueues!.txQueueUrl);
        return sqsRow?.status === WagerTransactionStatus.Processed && depth.visible === 0 && depth.notVisible === 0
          ? true
          : undefined;
      },
      { timeoutMs: 8_000, intervalMs: 30, description: 'contended SQS wager committed and acknowledged' },
    );

    // Confirm all 3 OS processes remain healthy and operational after lock contention & recovery:
    const healthChecks = await Promise.all([node1, node2, node3].map((n) => n.request('/health/ready')));
    expect(healthChecks.map((r) => r.status)).toEqual([200, 200, 200]);

    const contendedFinal = (await (await node3.request(`/wallets/${walletContended.id}`)).json()) as WalletView;
    expect(contendedFinal.balance.amount).toBe('115.00'); // 150 - 20 - 30 + 15

    await assertFullFinancialAndMessagingInvariants({
      walletIds: [walletFence1.id, walletFence2.id, walletContended.id],
      verifyInstance: node2,
      queues: activeQueues,
    });
  }, 45_000);

  test('Cenário G — Shutdown gracioso (SIGTERM) de 1 instância com outras 2 ativas (3 processos simultâneos, handoff VisibilityTimeout=0, isolamento de conexões e continuidade sem perda)', async () => {
    activeQueues = await createClusterQueues(sqs, { visibilityTimeoutSeconds: 20 });

    // Start Node 1 first so it deterministically receives [M1, M2, M3] in a single batch while wallet1 is locked,
    // then start Node 2 and Node 3 so 3 real OS processes are simultaneously active before SIGTERM!
    const node1 = await cluster.spawn({
      instanceId: 'p73-scen-g-1',
      port: 0,
      queues: activeQueues,
      workersEnabled: true,
      sqsWaitTimeSeconds: 1,
      shutdownTimeoutMs: 6_000,
    });

    const wInFlight = await openWallet(node1, '100.00', 'BRL');
    const wHandoffA = await openWallet(node1, '100.00', 'BRL');
    const wHandoffB = await openWallet(node1, '100.00', 'BRL');

    let resolveW1Locked!: (pid: number) => void;
    const w1Locked = new Promise<number>((resolve) => {
      resolveW1Locked = resolve;
    });
    let releaseW1!: () => void;
    const holdW1 = new Promise<void>((resolve) => {
      releaseW1 = resolve;
    });

    const w1GateTx = gateSql.begin(async (tx) => {
      const [r] = await tx<Array<{ pid: number }>>`select pg_backend_pid()::int as pid`;
      await tx`select id from wallets where id = ${wInFlight.id} for update`;
      resolveW1Locked(r!.pid);
      await holdW1;
    });
    const w1GatePid = await w1Locked;

    const envInFlight = buildSqsEnvelope(wInFlight, {
      externalTransactionId: `g-m1-${newId().slice(0, 8)}`,
      amount: '30.00',
    });
    const envHandoffA = buildSqsEnvelope(wHandoffA, {
      externalTransactionId: `g-m2-${newId().slice(0, 8)}`,
      amount: '20.00',
    });
    const envHandoffB = buildSqsEnvelope(wHandoffB, {
      externalTransactionId: `g-m3-${newId().slice(0, 8)}`,
      amount: '45.00',
    });

    await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: activeQueues.txQueueUrl,
        Entries: [envInFlight, envHandoffA, envHandoffB].map((env, idx) => {
          const single = toWagerTransactionSendInput(activeQueues!.txQueueUrl, env);
          return {
            Id: `g-${idx}`,
            MessageBody: single.MessageBody!,
            MessageGroupId: single.MessageGroupId!,
            MessageDeduplicationId: single.MessageDeduplicationId!,
          };
        }),
      }),
    );

    // Wait until Node 1 is blocked on wInFlight's row lock (holding [envInFlight, envHandoffA, envHandoffB]):
    await pollUntil(
      async () => {
        const rows = await adminSql<Array<{ pid: number }>>`
          select pid::int as pid
          from pg_stat_activity
          where datname = current_database()
            and application_name = ${node1.pgApplicationName}
            and ${w1GatePid} = any(pg_blocking_pids(pid))
        `;
        return rows.length >= 1 ? true : undefined;
      },
      { timeoutMs: 6_000, intervalMs: 15, description: 'node1 blocked on wInFlight while holding 3-message batch' },
    );

    // Spawn Node 2 and Node 3 -> all 3 OS processes are now simultaneously active!
    const [node2, node3] = await Promise.all([
      cluster.spawn({
        instanceId: 'p73-scen-g-2',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
        shutdownTimeoutMs: 6_000,
      }),
      cluster.spawn({
        instanceId: 'p73-scen-g-3',
        port: 0,
        queues: activeQueues,
        workersEnabled: true,
        sqsWaitTimeSeconds: 1,
        shutdownTimeoutMs: 6_000,
      }),
    ]);

    const wHttp2 = await openWallet(node2, '100.00', 'BRL');
    const wHttp3 = await openWallet(node3, '100.00', 'BRL');

    // Confirm all 3 OS processes have active connections in pg_stat_activity BEFORE sending SIGTERM to Node 1:
    const preShutdownApps = await adminSql<Array<{ application_name: string }>>`
      select distinct application_name
      from pg_stat_activity
      where datname = current_database()
        and application_name in (${node1.pgApplicationName}, ${node2.pgApplicationName}, ${node3.pgApplicationName})
      order by application_name asc
    `;
    expect(preShutdownApps.map((r) => r.application_name)).toEqual([
      'wagering:p73-scen-g-1',
      'wagering:p73-scen-g-2',
      'wagering:p73-scen-g-3',
    ]);

    // Send graceful SIGTERM ONLY to Node 1!
    const sigtermStartedAt = Date.now();
    const stopNode1Promise = node1.stopGracefully('SIGTERM', 8_000);

    // Deterministically confirm Node 1 entered shutdown (returns 503 on /health/ready) while draining M1:
    await pollUntil(
      async () => {
        try {
          const r = await node1.request('/health/ready');
          return r.status === 503 ? true : undefined;
        } catch {
          return true;
        }
      },
      { timeoutMs: 4_000, intervalMs: 15, description: 'node1 entering shutdown readiness 503' },
    );

    // Simultaneously, Node 2 and Node 3 stay 200 /health/ready and process concurrent HTTP wagers uninterrupted!
    const [ready2, ready3, wagerOn2, wagerOn3] = await Promise.all([
      node2.request('/health/ready'),
      node3.request('/health/ready'),
      submitWagerHttp(
        node2,
        buildWagerPayload(wHttp2, { externalTransactionId: `g-h2-${newId().slice(0, 8)}`, amount: '15.00' }),
        `idem-g-h2-${newId().slice(0, 8)}`,
      ),
      submitWagerHttp(
        node3,
        buildWagerPayload(wHttp3, { externalTransactionId: `g-h3-${newId().slice(0, 8)}`, amount: '25.00' }),
        `idem-g-h3-${newId().slice(0, 8)}`,
      ),
    ]);

    expect(ready2.status).toBe(200);
    expect(ready3.status).toBe(200);
    expect(wagerOn2.status).toBe(200);
    expect(wagerOn3.status).toBe(200);

    // Release wInFlight lock -> Node 1 commits M1, releases M2 & M3 (VisibilityTimeout=0 on 20s queue)
    // to Node 2 & Node 3, closes ONLY its connections, and exits with code 0:
    releaseW1();
    await w1GateTx;

    const exitCode1 = await stopNode1Promise;
    expect(exitCode1).toBe(0);
    expect(node1.isExited()).toBe(true);
    expect(node2.isExited()).toBe(false);
    expect(node3.isExited()).toBe(false);

    // Node 2 and Node 3 must pick up and process M2 and M3 immediately (< 10s << 20s VisibilityTimeout!):
    await pollUntil(
      async () => {
        const [row] = await appSql<Array<{ cnt: number }>>`
          select count(*)::int as cnt
          from wager_transactions
          where external_transaction_id in (
            ${envInFlight.data.externalTransactionId},
            ${envHandoffA.data.externalTransactionId},
            ${envHandoffB.data.externalTransactionId}
          )
            and status = 'PROCESSED'
        `;
        return row?.cnt === 3 ? true : undefined;
      },
      { timeoutMs: 8_000, intervalMs: 30, description: 'M1 committed by node1 and M2+M3 handed off to node2/node3' },
    );
    expect(Date.now() - sigtermStartedAt).toBeLessThan(10_000);

    // Confirm Node 1 has ZERO connections left in pg_stat_activity while Node 2 and Node 3 retain theirs:
    const postShutdownSessions = await adminSql<Array<{ application_name: string; cnt: number }>>`
      select application_name, count(*)::int as cnt
      from pg_stat_activity
      where datname = current_database()
        and application_name in (${node1.pgApplicationName}, ${node2.pgApplicationName}, ${node3.pgApplicationName})
      group by application_name
    `;
    const postByApp = new Map(postShutdownSessions.map((r) => [r.application_name, r.cnt]));
    expect(postByApp.get(node1.pgApplicationName) ?? 0).toBe(0);
    expect(postByApp.get(node2.pgApplicationName) ?? 0).toBeGreaterThanOrEqual(1);
    expect(postByApp.get(node3.pgApplicationName) ?? 0).toBeGreaterThanOrEqual(1);

    // Verify exact final balances across all 5 wallets:
    const expectedBalances: ReadonlyArray<readonly [string, string]> = [
      [wInFlight.id, '70.00'],
      [wHandoffA.id, '80.00'],
      [wHandoffB.id, '55.00'],
      [wHttp2.id, '85.00'],
      [wHttp3.id, '75.00'],
    ];
    for (const [wId, expectedAmt] of expectedBalances) {
      const wView = (await (await node2.request(`/wallets/${wId}`)).json()) as WalletView;
      expect(wView.balance.amount).toBe(expectedAmt);
    }

    await assertFullFinancialAndMessagingInvariants({
      walletIds: expectedBalances.map(([id]) => id),
      verifyInstance: node3,
      queues: activeQueues,
    });
  }, 40_000);
});
