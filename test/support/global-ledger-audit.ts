import { createHash } from 'node:crypto';
import type { SQL } from 'bun';
import { OUTBOX_QUARANTINED_UNTIL_MS } from '../../src/common/messaging/outbox-message';

export interface AuditViolation {
  readonly category:
    | 'F1_BALANCE_ARITHMETIC'
    | 'F2_LEDGER_SEQUENCE'
    | 'F3_REFERENTIAL_INTEGRITY'
    | 'F3_PAYLOAD_HASH'
    | 'F4_INBOX_INTEGRITY'
    | 'F5_OUTBOX_INTEGRITY';
  readonly code: string;
  readonly entityId: string;
  readonly detail: string;
}

export interface ExpectedInboxAuditRecord {
  readonly messageId: string;
  readonly idempotencyKey: string;
  readonly expectBusinessConflict?: boolean;
}

export interface GlobalLedgerAuditOptions {
  /** Optional list of SQS envelopes known to have been submitted in the test for cross-checking Inbox -> TX state. */
  readonly expectedInboxMessages?: readonly ExpectedInboxAuditRecord[];
}

export interface GlobalLedgerAuditReport {
  readonly databaseName: string;
  readonly currentRole: string;
  readonly totalWalletsAudited: number;
  readonly totalTransactionsExamined: number;
  readonly totalLedgerEntriesExamined: number;
  readonly totalInboxRecordsExamined: number;
  readonly totalOutboxEventsExamined: number;
  readonly balanceDivergenceCount: number;
  readonly versionGapOrSequenceViolationCount: number;
  readonly duplicateViolationCount: number;
  readonly referentialInconsistencyCount: number;
  readonly payloadHashMismatchCount: number;
  readonly inboxViolationCount: number;
  readonly outboxViolationCount: number;
  readonly pendingOutboxEventCount: number;
  readonly recoverablePendingOutboxCount: number;
  readonly quarantinedOutboxCount: number;
  readonly pendingReferenceTransactionCount: number;
  readonly totalFinancialViolations: number;
  readonly violations: readonly AuditViolation[];
}

/**
 * Independent canonical JSON SHA-256 digest (does not import or depend on application services).
 */
function independentCanonicalSha256(value: unknown): string {
  return createHash('sha256').update(serializeCanonical(value)).digest('hex');
}

function serializeCanonical(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => serializeCanonical(item === undefined ? null : item)).join(',')}]`;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj)
    .filter((k) => obj[k] !== undefined)
    .sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${serializeCanonical(obj[k])}`).join(',')}}`;
}

/**
 * Executes an independent, SQL-first global audit of `wallets`, `wager_transactions`,
 * `wallet_ledger_entries`, `inbox_messages`, and `outbox_messages` directly on `wagering_test`.
 *
 * All monetary comparisons and aggregations run inside PostgreSQL using `numeric(19,2)` / `numeric`
 * without ever converting monetary values to IEEE-754 floating point numbers.
 */
export async function runIndependentGlobalLedgerAudit(
  db: SQL,
  options: GlobalLedgerAuditOptions = {},
): Promise<GlobalLedgerAuditReport> {
  const [connInfo] = await db<Array<{ db_name: string; role_name: string }>>`
    select current_database() as db_name, current_user as role_name
  `;
  if (!connInfo || connInfo.db_name !== 'wagering_test') {
    throw new Error(
      `Global Ledger Audit refused to run on database "${connInfo?.db_name ?? 'unknown'}" (must be "wagering_test")`,
    );
  }

  const violations: AuditViolation[] = [];
  let balanceDivergenceCount = 0;
  let versionGapOrSequenceViolationCount = 0;
  let duplicateViolationCount = 0;
  let referentialInconsistencyCount = 0;
  let payloadHashMismatchCount = 0;
  let inboxViolationCount = 0;
  let outboxViolationCount = 0;

  // Global counts across all 5 core tables in wagering_test:
  const [counts] = await db<
    Array<{
      wallets_count: number;
      tx_count: number;
      ledger_count: number;
      inbox_count: number;
      outbox_count: number;
      pending_ref_count: number;
      pending_outbox_count: number;
      recoverable_outbox_count: number;
      quarantined_outbox_count: number;
    }>
  >`
    select
      (select count(*)::int from wallets) as wallets_count,
      (select count(*)::int from wager_transactions) as tx_count,
      (select count(*)::int from wallet_ledger_entries) as ledger_count,
      (select count(*)::int from inbox_messages) as inbox_count,
      (select count(*)::int from outbox_messages) as outbox_count,
      (select count(*)::int from wager_transactions where status = 'PENDING_REFERENCE') as pending_ref_count,
      (select count(*)::int from outbox_messages where published_at is null) as pending_outbox_count,
      (
        select count(*)::int
        from outbox_messages
        where published_at is null
          and (next_attempt_at is null or next_attempt_at < ${new Date(OUTBOX_QUARANTINED_UNTIL_MS)})
      ) as recoverable_outbox_count,
      (
        select count(*)::int
        from outbox_messages
        where published_at is null
          and next_attempt_at >= ${new Date(OUTBOX_QUARANTINED_UNTIL_MS)}
      ) as quarantined_outbox_count
  `;

  // =========================================================================
  // F.1 — Saldo e integridade aritmética (pure PostgreSQL numeric(19,2))
  // =========================================================================
  const f1WalletRows = await db<
    Array<{
      wallet_id: string;
      currency: string;
      stored_balance: string;
      net_ledger_balance: string;
      diff_amount: string;
      entry_count: number;
      first_balance_before: string | null;
      last_balance_after: string | null;
      is_negative: boolean;
      invalid_scale: boolean;
    }>
  >`
    with ledger_agg as (
      select
        le.wallet_id,
        count(*)::int as entry_count,
        coalesce(
          sum(
            case
              when le.direction = 'CREDIT' then le.amount::numeric(19,2)
              when le.direction = 'DEBIT' then (-le.amount)::numeric(19,2)
              else 0.00::numeric(19,2)
            end
          ),
          0.00::numeric(19,2)
        )::numeric(19,2) as net_balance,
        (
          array_agg(le.balance_before order by le.wallet_version asc, le.created_at asc)
        )[1]::text as first_balance_before,
        (
          array_agg(le.balance_after order by le.wallet_version desc, le.created_at desc)
        )[1]::text as last_balance_after,
        bool_or(
          scale(le.amount) > 2 or scale(le.balance_before) > 2 or scale(le.balance_after) > 2
          or le.amount <= 0.00::numeric(19,2)
          or le.balance_before < 0.00::numeric(19,2)
          or le.balance_after < 0.00::numeric(19,2)
        ) as any_bad_entry_precision_or_sign
      from wallet_ledger_entries le
      group by le.wallet_id
    )
    select
      w.id::text as wallet_id,
      trim(w.currency) as currency,
      w.balance_amount::numeric(19,2)::text as stored_balance,
      coalesce(la.net_balance, 0.00::numeric(19,2))::text as net_ledger_balance,
      (w.balance_amount::numeric(19,2) - coalesce(la.net_balance, 0.00::numeric(19,2)))::numeric(19,2)::text as diff_amount,
      coalesce(la.entry_count, 0) as entry_count,
      la.first_balance_before,
      la.last_balance_after,
      (w.balance_amount < 0.00::numeric(19,2)) as is_negative,
      (scale(w.balance_amount) > 2 or coalesce(la.any_bad_entry_precision_or_sign, false)) as invalid_scale
    from wallets w
    left join ledger_agg la on la.wallet_id = w.id
    order by w.id asc
  `;

  for (const row of f1WalletRows) {
    if (row.is_negative) {
      balanceDivergenceCount += 1;
      violations.push({
        category: 'F1_BALANCE_ARITHMETIC',
        code: 'NEGATIVE_STORED_BALANCE',
        entityId: row.wallet_id,
        detail: `Stored wallet balance is negative (${row.stored_balance} ${row.currency})`,
      });
    }
    if (row.invalid_scale) {
      balanceDivergenceCount += 1;
      violations.push({
        category: 'F1_BALANCE_ARITHMETIC',
        code: 'DECIMAL_PRECISION_OR_ENTRY_SIGN_VIOLATION',
        entityId: row.wallet_id,
        detail: `Invalid decimal scale or negative/non-positive ledger entry detected for wallet ${row.wallet_id}`,
      });
    }
    if (row.diff_amount !== '0.00') {
      balanceDivergenceCount += 1;
      violations.push({
        category: 'F1_BALANCE_ARITHMETIC',
        code: 'STORED_VS_LEDGER_SUM_MISMATCH',
        entityId: row.wallet_id,
        detail: `stored=${row.stored_balance}, ledgerSum=${row.net_ledger_balance}, diff=${row.diff_amount}`,
      });
    }
    if (row.entry_count === 0 && row.stored_balance !== '0.00') {
      balanceDivergenceCount += 1;
      violations.push({
        category: 'F1_BALANCE_ARITHMETIC',
        code: 'NON_ZERO_BALANCE_WITHOUT_LEDGER',
        entityId: row.wallet_id,
        detail: `Wallet has 0 ledger entries but stored balance is ${row.stored_balance}`,
      });
    }
    if (row.entry_count > 0) {
      if (row.first_balance_before !== '0.00') {
        balanceDivergenceCount += 1;
        violations.push({
          category: 'F1_BALANCE_ARITHMETIC',
          code: 'NON_ZERO_INITIAL_BALANCE_BEFORE',
          entityId: row.wallet_id,
          detail: `First ledger entry balance_before=${row.first_balance_before}, expected 0.00`,
        });
      }
      if (row.last_balance_after !== row.stored_balance) {
        balanceDivergenceCount += 1;
        violations.push({
          category: 'F1_BALANCE_ARITHMETIC',
          code: 'FINAL_LEDGER_BALANCE_MISMATCH',
          entityId: row.wallet_id,
          detail: `Last ledger balance_after=${row.last_balance_after} != wallet.balance_amount=${row.stored_balance}`,
        });
      }
    }
  }

  // =========================================================================
  // F.2 — Sequência do ledger (start version, contiguity, chain, final version)
  // =========================================================================
  const f2WalletSummary = await db<
    Array<{
      wallet_id: string;
      wallet_version: number;
      has_opening: boolean;
      entry_count: number;
      distinct_versions: number;
      min_version: number | null;
      max_version: number | null;
    }>
  >`
    select
      w.id::text as wallet_id,
      w.version as wallet_version,
      exists(
        select 1 from wager_transactions t
        where t.wallet_id = w.id and t.kind = 'OPENING'
      ) as has_opening,
      count(le.id)::int as entry_count,
      count(distinct le.wallet_version)::int as distinct_versions,
      min(le.wallet_version)::int as min_version,
      max(le.wallet_version)::int as max_version
    from wallets w
    left join wallet_ledger_entries le on le.wallet_id = w.id
    group by w.id, w.version
  `;

  for (const row of f2WalletSummary) {
    if (row.entry_count === 0) {
      if (row.has_opening || row.wallet_version !== 1) {
        versionGapOrSequenceViolationCount += 1;
        violations.push({
          category: 'F2_LEDGER_SEQUENCE',
          code: 'EMPTY_WALLET_VERSION_MISMATCH',
          entityId: row.wallet_id,
          detail: `Empty-ledger wallet has version=${row.wallet_version}, hasOpening=${row.has_opening} (expected version=1, hasOpening=false)`,
        });
      }
      continue;
    }

    const expectedStart = row.has_opening ? 1 : 2;
    if (row.min_version !== expectedStart) {
      versionGapOrSequenceViolationCount += 1;
      violations.push({
        category: 'F2_LEDGER_SEQUENCE',
        code: 'UNEXPECTED_INITIAL_WALLET_VERSION',
        entityId: row.wallet_id,
        detail: `min(wallet_version)=${row.min_version}, expected=${expectedStart} (hasOpening=${row.has_opening})`,
      });
    }
    if (row.distinct_versions !== row.entry_count) {
      duplicateViolationCount += 1;
      versionGapOrSequenceViolationCount += 1;
      violations.push({
        category: 'F2_LEDGER_SEQUENCE',
        code: 'DUPLICATE_WALLET_VERSION',
        entityId: row.wallet_id,
        detail: `entry_count=${row.entry_count} != distinct_versions=${row.distinct_versions}`,
      });
    }
    if (
      row.min_version !== null &&
      row.max_version !== null &&
      row.max_version - row.min_version + 1 !== row.entry_count
    ) {
      versionGapOrSequenceViolationCount += 1;
      violations.push({
        category: 'F2_LEDGER_SEQUENCE',
        code: 'WALLET_VERSION_GAP',
        entityId: row.wallet_id,
        detail: `Version range [${row.min_version}..${row.max_version}] has ${row.entry_count} entries`,
      });
    }
    if (row.max_version !== row.wallet_version) {
      versionGapOrSequenceViolationCount += 1;
      violations.push({
        category: 'F2_LEDGER_SEQUENCE',
        code: 'FINAL_WALLET_VERSION_MISMATCH',
        entityId: row.wallet_id,
        detail: `max(ledger.wallet_version)=${row.max_version} != wallet.version=${row.wallet_version}`,
      });
    }
  }

  const f2ChainViolations = await db<
    Array<{
      entry_id: string;
      wallet_id: string;
      wallet_version: number;
      prev_version: number | null;
      balance_before: string;
      prev_balance_after: string | null;
      balance_after: string;
      expected_balance_after: string;
    }>
  >`
    with ordered as (
      select
        le.id::text as entry_id,
        le.wallet_id::text as wallet_id,
        le.wallet_version,
        lag(le.wallet_version) over (partition by le.wallet_id order by le.wallet_version asc) as prev_version,
        le.balance_before::numeric(19,2) as balance_before_num,
        lag(le.balance_after::numeric(19,2)) over (partition by le.wallet_id order by le.wallet_version asc) as prev_balance_after_num,
        le.balance_after::numeric(19,2) as balance_after_num,
        (
          le.balance_before::numeric(19,2) +
          case when le.direction = 'CREDIT' then le.amount::numeric(19,2) else (-le.amount)::numeric(19,2) end
        )::numeric(19,2) as expected_after_num
      from wallet_ledger_entries le
    )
    select
      entry_id,
      wallet_id,
      wallet_version,
      prev_version,
      balance_before_num::text as balance_before,
      prev_balance_after_num::text as prev_balance_after,
      balance_after_num::text as balance_after,
      expected_after_num::text as expected_balance_after
    from ordered
    where (prev_version is not null and wallet_version <> prev_version + 1)
       or (prev_balance_after_num is null and balance_before_num <> 0.00::numeric(19,2))
       or (prev_balance_after_num is not null and balance_before_num <> prev_balance_after_num)
       or (balance_after_num <> expected_after_num)
  `;

  for (const v of f2ChainViolations) {
    versionGapOrSequenceViolationCount += 1;
    violations.push({
      category: 'F2_LEDGER_SEQUENCE',
      code: 'LEDGER_CHAIN_TRANSITION_MISMATCH',
      entityId: v.entry_id,
      detail:
        `wallet=${v.wallet_id} v=${v.wallet_version} (prev=${v.prev_version}) ` +
        `before=${v.balance_before} (prevAfter=${v.prev_balance_after}) ` +
        `after=${v.balance_after} (expectedAfter=${v.expected_balance_after})`,
    });
  }

  // =========================================================================
  // F.3 — Integridade referencial financeira + payload hashes
  // =========================================================================

  // F.3a: Duplicate transaction_id in ledger, duplicate idempotency_key, duplicate (provider_id, external_transaction_id), or multiple active reversals
  const duplicateChecks = await db<
    Array<{ dup_type: string; dup_key: string; cnt: number }>
  >`
    select 'LEDGER_TRANSACTION_ID' as dup_type, transaction_id::text as dup_key, count(*)::int as cnt
    from wallet_ledger_entries
    group by transaction_id
    having count(*) > 1
    union all
    select 'IDEMPOTENCY_KEY' as dup_type, idempotency_key as dup_key, count(*)::int as cnt
    from wager_transactions
    group by idempotency_key
    having count(*) > 1
    union all
    select 'PROVIDER_EXTERNAL_TX' as dup_type, (provider_id || ':' || external_transaction_id) as dup_key, count(*)::int as cnt
    from wager_transactions
    group by provider_id, external_transaction_id
    having count(*) > 1
    union all
    select 'ACTIVE_REVERSAL' as dup_type, (provider_id || ':' || reference_external_transaction_id) as dup_key, count(*)::int as cnt
    from wager_transactions
    where kind in ('REFUND', 'ROLLBACK')
      and status in ('PENDING_REFERENCE', 'PROCESSED')
      and reference_external_transaction_id is not null
    group by provider_id, reference_external_transaction_id
    having count(*) > 1
  `;

  for (const dup of duplicateChecks) {
    duplicateViolationCount += 1;
    violations.push({
      category: 'F3_REFERENTIAL_INTEGRITY',
      code: `DUPLICATE_${dup.dup_type}`,
      entityId: dup.dup_key,
      detail: `Duplicate ${dup.dup_type} count=${dup.cnt}`,
    });
  }

  // F.3b: Orphan ledger entries (no matching transaction or wallet)
  const orphanLedgers = await db<Array<{ entry_id: string; tx_id: string; wallet_id: string }>>`
    select le.id::text as entry_id, le.transaction_id::text as tx_id, le.wallet_id::text as wallet_id
    from wallet_ledger_entries le
    left join wager_transactions t on t.id = le.transaction_id
    left join wallets w on w.id = le.wallet_id
    where t.id is null or w.id is null
  `;
  for (const orphan of orphanLedgers) {
    referentialInconsistencyCount += 1;
    violations.push({
      category: 'F3_REFERENTIAL_INTEGRITY',
      code: 'ORPHAN_LEDGER_ENTRY',
      entityId: orphan.entry_id,
      detail: `Ledger entry points to missing tx=${orphan.tx_id} or wallet=${orphan.wallet_id}`,
    });
  }

  // F.3c: Effective balance-moving transactions (status='PROCESSED' AND kind <> 'LOSS') MUST match 1:1 with ledger
  const effectiveTxMismatches = await db<
    Array<{
      tx_id: string;
      kind: string;
      status: string;
      issue: string;
    }>
  >`
    select
      t.id::text as tx_id,
      t.kind,
      t.status,
      case
        when le.id is null then 'MISSING_LEDGER_FOR_EFFECTIVE_TX'
        when le.wallet_id <> t.wallet_id then 'LEDGER_WALLET_MISMATCH'
        when trim(le.currency) <> trim(t.currency) then 'LEDGER_CURRENCY_MISMATCH'
        when le.amount::numeric(19,2) <> t.amount::numeric(19,2) then 'LEDGER_AMOUNT_MISMATCH'
        when t.observed_balance_amount is null or le.balance_after::numeric(19,2) <> t.observed_balance_amount::numeric(19,2) then 'OBSERVED_BALANCE_MISMATCH'
        when t.kind = 'BET' and le.direction <> 'DEBIT' then 'INVALID_BET_DIRECTION'
        when t.kind in ('OPENING', 'WIN', 'REFUND') and le.direction <> 'CREDIT' then 'INVALID_CREDIT_DIRECTION'
        when t.kind = 'ROLLBACK' and ref.id is null then 'ROLLBACK_MISSING_REFERENCE_ROW'
        when t.kind = 'ROLLBACK' and ref.kind = 'BET' and le.direction <> 'CREDIT' then 'INVALID_ROLLBACK_BET_DIRECTION'
        when t.kind = 'ROLLBACK' and ref.kind in ('WIN', 'REFUND') and le.direction <> 'DEBIT' then 'INVALID_ROLLBACK_CREDIT_DIRECTION'
        else null
      end as issue
    from wager_transactions t
    left join wallet_ledger_entries le on le.transaction_id = t.id
    left join wager_transactions ref on ref.id = t.reference_transaction_id
    where t.status = 'PROCESSED'
      and t.kind <> 'LOSS'
      and (
        le.id is null
        or le.wallet_id <> t.wallet_id
        or trim(le.currency) <> trim(t.currency)
        or le.amount::numeric(19,2) <> t.amount::numeric(19,2)
        or t.observed_balance_amount is null
        or le.balance_after::numeric(19,2) <> t.observed_balance_amount::numeric(19,2)
        or (t.kind = 'BET' and le.direction <> 'DEBIT')
        or (t.kind in ('OPENING', 'WIN', 'REFUND') and le.direction <> 'CREDIT')
        or (t.kind = 'ROLLBACK' and (
          ref.id is null
          or (ref.kind = 'BET' and le.direction <> 'CREDIT')
          or (ref.kind in ('WIN', 'REFUND') and le.direction <> 'DEBIT')
        ))
      )
  `;

  for (const m of effectiveTxMismatches) {
    referentialInconsistencyCount += 1;
    violations.push({
      category: 'F3_REFERENTIAL_INTEGRITY',
      code: m.issue,
      entityId: m.tx_id,
      detail: `Effective transaction (${m.kind}/${m.status}) violated ledger invariant: ${m.issue}`,
    });
  }

  // F.3d: Non-balance-moving operations (LOSS PROCESSED, REJECTED, PENDING_REFERENCE, FAILED) MUST NOT have ledger entries,
  // and transient PENDING must never exist in PostgreSQL.
  const nonBalanceViolations = await db<
    Array<{ tx_id: string; kind: string; status: string; issue: string }>
  >`
    select
      t.id::text as tx_id,
      t.kind,
      t.status,
      case
        when t.status = 'PENDING' then 'TRANSIENT_PENDING_PERSISTED'
        when le.id is not null then 'UNEXPECTED_LEDGER_FOR_NON_BALANCE_TX'
        when t.status = 'PROCESSED' and t.kind = 'LOSS' and (t.observed_balance_amount is null or t.processed_at is null or t.failure_code is not null)
          then 'INVALID_PROCESSED_LOSS_FIELDS'
        when t.status = 'REJECTED' and (t.failure_code is null or t.observed_balance_amount is null or t.processed_at is not null)
          then 'INVALID_REJECTED_FIELDS'
        when t.status = 'PENDING_REFERENCE' and (t.processed_at is not null or t.next_attempt_at is null or t.reference_external_transaction_id is null)
          then 'INVALID_PENDING_REFERENCE_FIELDS'
        else null
      end as issue
    from wager_transactions t
    left join wallet_ledger_entries le on le.transaction_id = t.id
    where t.status = 'PENDING'
       or (
         (t.kind = 'LOSS' or t.status in ('REJECTED', 'PENDING_REFERENCE', 'FAILED'))
         and (
           le.id is not null
           or (t.status = 'PROCESSED' and t.kind = 'LOSS' and (t.observed_balance_amount is null or t.processed_at is null or t.failure_code is not null))
           or (t.status = 'REJECTED' and (t.failure_code is null or t.observed_balance_amount is null or t.processed_at is not null))
           or (t.status = 'PENDING_REFERENCE' and (t.processed_at is not null or t.next_attempt_at is null or t.reference_external_transaction_id is null))
         )
       )
  `;

  for (const v of nonBalanceViolations) {
    referentialInconsistencyCount += 1;
    violations.push({
      category: 'F3_REFERENTIAL_INTEGRITY',
      code: v.issue,
      entityId: v.tx_id,
      detail: `Non-balance/state invariant violated on (${v.kind}/${v.status}): ${v.issue}`,
    });
  }

  // F.3e: Processed reference rules (WIN/LOSS/REFUND/ROLLBACK with reference_transaction_id)
  const referenceViolations = await db<
    Array<{ tx_id: string; kind: string; issue: string }>
  >`
    select
      t.id::text as tx_id,
      t.kind,
      case
        when t.reference_external_transaction_id is not null and t.reference_transaction_id is null then 'MISSING_RESOLVED_REFERENCE_ID'
        when ref.id is null then 'BROKEN_REFERENCE_FK'
        when ref.status <> 'PROCESSED' then 'REFERENCED_TX_NOT_PROCESSED'
        when ref.provider_id <> t.provider_id or ref.wallet_id <> t.wallet_id or ref.round_id <> t.round_id then 'REFERENCE_SCOPE_MISMATCH'
        when t.kind in ('WIN', 'LOSS', 'REFUND') and ref.kind <> 'BET' then 'INVALID_REFERENCED_KIND_FOR_SETTLEMENT_OR_REFUND'
        when t.kind = 'ROLLBACK' and ref.kind not in ('BET', 'WIN', 'REFUND') then 'INVALID_REFERENCED_KIND_FOR_ROLLBACK'
        when t.kind in ('REFUND', 'ROLLBACK') and ref.amount::numeric(19,2) <> t.amount::numeric(19,2) then 'REVERSAL_AMOUNT_MISMATCH'
        else null
      end as issue
    from wager_transactions t
    left join wager_transactions ref on ref.id = t.reference_transaction_id
    where t.status = 'PROCESSED'
      and t.reference_external_transaction_id is not null
      and (
        t.reference_transaction_id is null
        or ref.id is null
        or ref.status <> 'PROCESSED'
        or ref.provider_id <> t.provider_id
        or ref.wallet_id <> t.wallet_id
        or ref.round_id <> t.round_id
        or (t.kind in ('WIN', 'LOSS', 'REFUND') and ref.kind <> 'BET')
        or (t.kind = 'ROLLBACK' and ref.kind not in ('BET', 'WIN', 'REFUND'))
        or (t.kind in ('REFUND', 'ROLLBACK') and ref.amount::numeric(19,2) <> t.amount::numeric(19,2))
      )
  `;

  for (const rv of referenceViolations) {
    referentialInconsistencyCount += 1;
    violations.push({
      category: 'F3_REFERENTIAL_INTEGRITY',
      code: rv.issue,
      entityId: rv.tx_id,
      detail: `Processed ${rv.kind} reference invariant violated: ${rv.issue}`,
    });
  }

  // F.3f: Payload hash correspondence for EVERY transaction in wager_transactions
  const allTxForHashes = await db<
    Array<{
      id: string;
      provider_id: string;
      external_transaction_id: string;
      idempotency_key: string;
      payload_hash: string;
      wallet_id: string;
      player_id: string;
      round_id: string;
      game_id: string;
      kind: string;
      amount: string;
      currency: string;
      reference_external_transaction_id: string | null;
    }>
  >`
    select
      id::text as id,
      provider_id,
      external_transaction_id,
      idempotency_key,
      trim(payload_hash) as payload_hash,
      wallet_id::text as wallet_id,
      player_id::text as player_id,
      round_id,
      game_id,
      kind,
      amount::numeric(19,2)::text as amount,
      trim(currency) as currency,
      reference_external_transaction_id
    from wager_transactions
  `;

  const txByIdempotencyKey = new Map<string, (typeof allTxForHashes)[number]>();
  for (const tx of allTxForHashes) {
    txByIdempotencyKey.set(tx.idempotency_key, tx);
    const expectedHash =
      tx.kind === 'OPENING'
        ? independentCanonicalSha256({
            walletId: tx.wallet_id,
            playerId: tx.player_id,
            money: { amount: tx.amount, currency: tx.currency },
          })
        : independentCanonicalSha256({
            providerId: tx.provider_id,
            externalTransactionId: tx.external_transaction_id,
            playerId: tx.player_id,
            walletId: tx.wallet_id,
            roundId: tx.round_id,
            gameId: tx.game_id,
            kind: tx.kind,
            money: { amount: tx.amount, currency: tx.currency },
            ...(tx.reference_external_transaction_id !== null
              ? { referenceExternalTransactionId: tx.reference_external_transaction_id }
              : {}),
          });

    if (tx.payload_hash !== expectedHash) {
      payloadHashMismatchCount += 1;
      referentialInconsistencyCount += 1;
      violations.push({
        category: 'F3_PAYLOAD_HASH',
        code: 'TRANSACTION_PAYLOAD_HASH_MISMATCH',
        entityId: tx.id,
        detail: `stored=${tx.payload_hash} != expected=${expectedHash}`,
      });
    }
  }

  // =========================================================================
  // F.4 — Integridade do Inbox
  // =========================================================================
  const inboxRows = await db<
    Array<{
      consumer_name: string;
      message_id: string;
      payload_hash: string;
      has_null_timestamps: boolean;
      processed_before_received: boolean;
      bad_hash_format: boolean;
      caused_tx_count: number;
      caused_ledger_count: number;
      matched_tx_idempotency_key: string | null;
      matched_tx_payload_hash: string | null;
    }>
  >`
    with inbox_causation as (
      select
        o.payload->>'causationId' as causation_message_id,
        count(distinct (o.payload->'data'->>'transactionId'))::int as caused_tx_count,
        count(distinct le.id)::int as caused_ledger_count,
        min(t.idempotency_key) as sample_idempotency_key,
        min(trim(t.payload_hash)) as sample_tx_payload_hash
      from outbox_messages o
      left join wager_transactions t
        on (o.payload->'data'->>'transactionId') is not null
       and t.id::text = (o.payload->'data'->>'transactionId')
      left join wallet_ledger_entries le
        on le.transaction_id = t.id
      where o.payload->>'causationId' is not null
      group by o.payload->>'causationId'
    )
    select
      i.consumer_name,
      i.message_id,
      trim(i.payload_hash) as payload_hash,
      (i.received_at is null or i.processed_at is null) as has_null_timestamps,
      (i.processed_at is not null and i.received_at is not null and i.processed_at < i.received_at) as processed_before_received,
      (trim(i.payload_hash) !~ '^[0-9a-f]{64}$') as bad_hash_format,
      coalesce(ic.caused_tx_count, 0) as caused_tx_count,
      coalesce(ic.caused_ledger_count, 0) as caused_ledger_count,
      ic.sample_idempotency_key as matched_tx_idempotency_key,
      ic.sample_tx_payload_hash as matched_tx_payload_hash
    from inbox_messages i
    left join inbox_causation ic on ic.causation_message_id = i.message_id
  `;

  const seenInboxKeys = new Set<string>();
  const inboxByMessageId = new Map<string, (typeof inboxRows)[number]>();
  for (const ib of inboxRows) {
    const compositeKey = `${ib.consumer_name}:${ib.message_id}`;
    if (seenInboxKeys.has(compositeKey)) {
      duplicateViolationCount += 1;
      inboxViolationCount += 1;
      violations.push({
        category: 'F4_INBOX_INTEGRITY',
        code: 'DUPLICATE_INBOX_KEY',
        entityId: compositeKey,
        detail: 'Duplicate (consumer_name, message_id) found in inbox_messages',
      });
    }
    seenInboxKeys.add(compositeKey);
    inboxByMessageId.set(ib.message_id, ib);

    if (ib.has_null_timestamps || ib.processed_before_received) {
      inboxViolationCount += 1;
      violations.push({
        category: 'F4_INBOX_INTEGRITY',
        code: 'INVALID_INBOX_TIMESTAMPS',
        entityId: compositeKey,
        detail: `nullTimestamps=${ib.has_null_timestamps}, processedBeforeReceived=${ib.processed_before_received}`,
      });
    }
    if (ib.bad_hash_format) {
      inboxViolationCount += 1;
      violations.push({
        category: 'F4_INBOX_INTEGRITY',
        code: 'INVALID_INBOX_PAYLOAD_HASH_FORMAT',
        entityId: compositeKey,
        detail: `Inbox payload_hash "${ib.payload_hash}" is not 64-char hex`,
      });
    }
    if (ib.caused_tx_count > 1 || ib.caused_ledger_count > 1) {
      duplicateViolationCount += 1;
      inboxViolationCount += 1;
      violations.push({
        category: 'F4_INBOX_INTEGRITY',
        code: 'DUPLICATE_FINANCIAL_EFFECT_FROM_MESSAGE',
        entityId: compositeKey,
        detail: `Single SQS message produced caused_tx_count=${ib.caused_tx_count}, caused_ledger_count=${ib.caused_ledger_count}`,
      });
    }
    if (ib.matched_tx_idempotency_key !== null && ib.matched_tx_payload_hash !== null) {
      const expectedInboxHash = independentCanonicalSha256({
        type: 'WagerTransactionRequested',
        idempotencyKey: ib.matched_tx_idempotency_key,
        payloadHash: ib.matched_tx_payload_hash,
      });
      if (ib.payload_hash !== expectedInboxHash) {
        inboxViolationCount += 1;
        violations.push({
          category: 'F4_INBOX_INTEGRITY',
          code: 'INBOX_TO_TX_HASH_MISMATCH',
          entityId: compositeKey,
          detail: `inbox.payload_hash=${ib.payload_hash} != canonical(tx)=${expectedInboxHash}`,
        });
      }
    }
  }

  if (options.expectedInboxMessages !== undefined) {
    for (const expected of options.expectedInboxMessages) {
      const ib = inboxByMessageId.get(expected.messageId);
      if (ib !== undefined && !expected.expectBusinessConflict) {
        const linkedTx = txByIdempotencyKey.get(expected.idempotencyKey);
        if (linkedTx === undefined) {
          inboxViolationCount += 1;
          violations.push({
            category: 'F4_INBOX_INTEGRITY',
            code: 'PROCESSED_INBOX_WITHOUT_EXPECTED_TX',
            entityId: expected.messageId,
            detail: `Inbox message ${expected.messageId} marked processed without persisting expected idempotencyKey=${expected.idempotencyKey}`,
          });
        }
      }
    }
  }

  // =========================================================================
  // F.5 — Integridade do Outbox
  // =========================================================================

  // F.5a: Orphan events (aggregate_id not in wallets, or transactionId not in wager_transactions)
  const orphanOutbox = await db<Array<{ event_id: string; event_type: string; aggregate_id: string; tx_id: string | null }>>`
    select
      o.id::text as event_id,
      o.event_type,
      o.aggregate_id::text as aggregate_id,
      (o.payload->'data'->>'transactionId') as tx_id
    from outbox_messages o
    left join wallets w on w.id = o.aggregate_id
    left join wager_transactions t on t.id::text = (o.payload->'data'->>'transactionId')
    where w.id is null
       or (o.payload->'data'->>'transactionId') is null
       or t.id is null
       or t.wallet_id <> o.aggregate_id
  `;
  for (const oo of orphanOutbox) {
    outboxViolationCount += 1;
    violations.push({
      category: 'F5_OUTBOX_INTEGRITY',
      code: 'ORPHAN_OR_MISROUTED_OUTBOX_EVENT',
      entityId: oo.event_id,
      detail: `type=${oo.event_type} aggregate=${oo.aggregate_id} txId=${oo.tx_id}`,
    });
  }

  // F.5b: Missing or duplicate expected events per wager_transactions row + internal sequence ordering
  const txOutboxChecks = await db<
    Array<{
      tx_id: string;
      kind: string;
      status: string;
      processed_events: number;
      balance_events: number;
      rejected_events: number;
      pending_events: number;
      min_pending_seq: string | null;
      min_processed_seq: string | null;
      min_balance_seq: string | null;
      min_rejected_seq: string | null;
    }>
  >`
    select
      t.id::text as tx_id,
      t.kind,
      t.status,
      count(o.id) filter (where o.event_type = 'WagerTransactionProcessed')::int as processed_events,
      count(o.id) filter (where o.event_type = 'WalletBalanceChanged')::int as balance_events,
      count(o.id) filter (where o.event_type = 'WagerTransactionRejected')::int as rejected_events,
      count(o.id) filter (where o.event_type = 'WagerTransactionPendingReference')::int as pending_events,
      min(o.sequence_number) filter (where o.event_type = 'WagerTransactionPendingReference')::text as min_pending_seq,
      min(o.sequence_number) filter (where o.event_type = 'WagerTransactionProcessed')::text as min_processed_seq,
      min(o.sequence_number) filter (where o.event_type = 'WalletBalanceChanged')::text as min_balance_seq,
      min(o.sequence_number) filter (where o.event_type = 'WagerTransactionRejected')::text as min_rejected_seq
    from wager_transactions t
    left join outbox_messages o
      on o.aggregate_id = t.wallet_id
     and (o.payload->'data'->>'transactionId') = t.id::text
    group by t.id, t.kind, t.status
  `;

  for (const row of txOutboxChecks) {
    if (row.status === 'PROCESSED') {
      const expectedBalanceEvents = row.kind === 'LOSS' ? 0 : 1;
      if (
        row.processed_events !== 1 ||
        row.balance_events !== expectedBalanceEvents ||
        row.rejected_events !== 0 ||
        row.pending_events > 1
      ) {
        outboxViolationCount += 1;
        violations.push({
          category: 'F5_OUTBOX_INTEGRITY',
          code: 'PROCESSED_TX_OUTBOX_CARDINALITY_MISMATCH',
          entityId: row.tx_id,
          detail:
            `kind=${row.kind} processed=${row.processed_events} balance=${row.balance_events} ` +
            `(expected ${expectedBalanceEvents}) rejected=${row.rejected_events} pending=${row.pending_events}`,
        });
      }
      if (
        expectedBalanceEvents === 1 &&
        row.min_processed_seq !== null &&
        row.min_balance_seq !== null &&
        BigInt(row.min_processed_seq) >= BigInt(row.min_balance_seq)
      ) {
        outboxViolationCount += 1;
        violations.push({
          category: 'F5_OUTBOX_INTEGRITY',
          code: 'OUTBOX_PROCESSED_BEFORE_BALANCE_ORDER_VIOLATION',
          entityId: row.tx_id,
          detail: `seq(Processed)=${row.min_processed_seq} >= seq(BalanceChanged)=${row.min_balance_seq}`,
        });
      }
      if (
        row.min_pending_seq !== null &&
        row.min_processed_seq !== null &&
        BigInt(row.min_pending_seq) >= BigInt(row.min_processed_seq)
      ) {
        outboxViolationCount += 1;
        violations.push({
          category: 'F5_OUTBOX_INTEGRITY',
          code: 'OUTBOX_PENDING_BEFORE_PROCESSED_ORDER_VIOLATION',
          entityId: row.tx_id,
          detail: `seq(Pending)=${row.min_pending_seq} >= seq(Processed)=${row.min_processed_seq}`,
        });
      }
    } else if (row.status === 'REJECTED') {
      if (
        row.rejected_events !== 1 ||
        row.processed_events !== 0 ||
        row.balance_events !== 0 ||
        row.pending_events > 1
      ) {
        outboxViolationCount += 1;
        violations.push({
          category: 'F5_OUTBOX_INTEGRITY',
          code: 'REJECTED_TX_OUTBOX_CARDINALITY_MISMATCH',
          entityId: row.tx_id,
          detail: `rejected=${row.rejected_events} processed=${row.processed_events} balance=${row.balance_events} pending=${row.pending_events}`,
        });
      }
      if (
        row.min_pending_seq !== null &&
        row.min_rejected_seq !== null &&
        BigInt(row.min_pending_seq) >= BigInt(row.min_rejected_seq)
      ) {
        outboxViolationCount += 1;
        violations.push({
          category: 'F5_OUTBOX_INTEGRITY',
          code: 'OUTBOX_PENDING_BEFORE_REJECTED_ORDER_VIOLATION',
          entityId: row.tx_id,
          detail: `seq(Pending)=${row.min_pending_seq} >= seq(Rejected)=${row.min_rejected_seq}`,
        });
      }
    } else if (row.status === 'PENDING_REFERENCE') {
      if (
        row.pending_events !== 1 ||
        row.processed_events !== 0 ||
        row.balance_events !== 0 ||
        row.rejected_events !== 0
      ) {
        outboxViolationCount += 1;
        violations.push({
          category: 'F5_OUTBOX_INTEGRITY',
          code: 'PENDING_TX_OUTBOX_CARDINALITY_MISMATCH',
          entityId: row.tx_id,
          detail: `pending=${row.pending_events} processed=${row.processed_events} balance=${row.balance_events} rejected=${row.rejected_events}`,
        });
      }
    }
  }

  // F.5c: 1:1 correspondence & causal sequence_number ordering between WalletBalanceChanged events and wallet_ledger_entries
  const outboxLedgerCausalMismatches = await db<
    Array<{
      event_id: string;
      aggregate_id: string;
      issue: string;
    }>
  >`
    with balance_events as (
      select
        o.id::text as event_id,
        o.aggregate_id::text as aggregate_id,
        o.sequence_number,
        (o.payload->'data'->>'transactionId') as tx_id_text,
        (o.payload->'data'->>'walletVersion')::int as ev_version,
        (o.payload->'data'->>'direction') as ev_direction,
        (o.payload->'data'->'money'->>'amount')::numeric(19,2) as ev_amount,
        (o.payload->'data'->'balanceBefore'->>'amount')::numeric(19,2) as ev_before,
        (o.payload->'data'->'balanceAfter'->>'amount')::numeric(19,2) as ev_after,
        lag((o.payload->'data'->>'walletVersion')::int) over (
          partition by o.aggregate_id order by o.sequence_number asc
        ) as prev_ev_version
      from outbox_messages o
      where o.event_type = 'WalletBalanceChanged'
    )
    select
      be.event_id,
      be.aggregate_id,
      case
        when le.id is null then 'MISSING_LEDGER_ENTRY_FOR_BALANCE_EVENT'
        when le.wallet_version <> be.ev_version then 'EVENT_WALLET_VERSION_MISMATCH'
        when le.direction <> be.ev_direction then 'EVENT_DIRECTION_MISMATCH'
        when le.amount::numeric(19,2) <> be.ev_amount then 'EVENT_AMOUNT_MISMATCH'
        when le.balance_before::numeric(19,2) <> be.ev_before then 'EVENT_BALANCE_BEFORE_MISMATCH'
        when le.balance_after::numeric(19,2) <> be.ev_after then 'EVENT_BALANCE_AFTER_MISMATCH'
        when be.prev_ev_version is not null and be.ev_version <> be.prev_ev_version + 1 then 'CAUSAL_SEQUENCE_VERSION_ORDER_VIOLATION'
        else null
      end as issue
    from balance_events be
    left join wallet_ledger_entries le
      on le.wallet_id::text = be.aggregate_id
     and le.transaction_id::text = be.tx_id_text
    where le.id is null
       or le.wallet_version <> be.ev_version
       or le.direction <> be.ev_direction
       or le.amount::numeric(19,2) <> be.ev_amount
       or le.balance_before::numeric(19,2) <> be.ev_before
       or le.balance_after::numeric(19,2) <> be.ev_after
       or (be.prev_ev_version is not null and be.ev_version <> be.prev_ev_version + 1)
  `;

  for (const cm of outboxLedgerCausalMismatches) {
    outboxViolationCount += 1;
    violations.push({
      category: 'F5_OUTBOX_INTEGRITY',
      code: cm.issue,
      entityId: cm.event_id,
      detail: `Aggregate ${cm.aggregate_id} WalletBalanceChanged violation: ${cm.issue}`,
    });
  }

  // F.5d: Head-of-line publication order per aggregate (no published event ahead of an unpublished predecessor)
  const outOfOrderPublications = await db<
    Array<{ later_id: string; aggregate_id: string; prev_seq: string; later_seq: string }>
  >`
    select
      later.id::text as later_id,
      later.aggregate_id::text as aggregate_id,
      prev.sequence_number::text as prev_seq,
      later.sequence_number::text as later_seq
    from outbox_messages later
    join outbox_messages prev
      on prev.aggregate_id = later.aggregate_id
     and prev.sequence_number < later.sequence_number
    where later.published_at is not null
      and prev.published_at is null
    limit 25
  `;
  for (const oop of outOfOrderPublications) {
    outboxViolationCount += 1;
    violations.push({
      category: 'F5_OUTBOX_INTEGRITY',
      code: 'PUBLISHED_AHEAD_OF_UNPUBLISHED_PREDECESSOR',
      entityId: oop.later_id,
      detail: `Aggregate ${oop.aggregate_id} published seq=${oop.later_seq} while earlier seq=${oop.prev_seq} is still unpublished`,
    });
  }

  return {
    databaseName: connInfo.db_name,
    currentRole: connInfo.role_name,
    totalWalletsAudited: counts?.wallets_count ?? 0,
    totalTransactionsExamined: counts?.tx_count ?? 0,
    totalLedgerEntriesExamined: counts?.ledger_count ?? 0,
    totalInboxRecordsExamined: counts?.inbox_count ?? 0,
    totalOutboxEventsExamined: counts?.outbox_count ?? 0,
    balanceDivergenceCount,
    versionGapOrSequenceViolationCount,
    duplicateViolationCount,
    referentialInconsistencyCount,
    payloadHashMismatchCount,
    inboxViolationCount,
    outboxViolationCount,
    pendingOutboxEventCount: counts?.pending_outbox_count ?? 0,
    recoverablePendingOutboxCount: counts?.recoverable_outbox_count ?? 0,
    quarantinedOutboxCount: counts?.quarantined_outbox_count ?? 0,
    pendingReferenceTransactionCount: counts?.pending_ref_count ?? 0,
    totalFinancialViolations: violations.length,
    violations,
  };
}
