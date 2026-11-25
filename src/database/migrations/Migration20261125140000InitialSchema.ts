import { Migration } from '@mikro-orm/migrations';

/**
 * Financial invariants live here, not only in application code: balances
 * never go negative, every balance change has exactly one ledger entry (and
 * vice versa), the ledger is append-only, terminal transactions are frozen
 * and a reference is reversed at most once.
 *
 * Cross-table rules use DEFERRABLE foreign keys and constraint triggers so
 * they are checked at COMMIT, independent of statement order inside the
 * transaction.
 */
export class Migration20261125140000InitialSchema extends Migration {
  override up(): void {
    // ---------------------------------------------------------------- wallets
    this.addSql(`
      create table wallets (
        id uuid primary key,
        player_id uuid not null,
        currency char(3) not null,
        balance_amount numeric(19,2) not null,
        version integer not null,
        created_at timestamptz not null,
        updated_at timestamptz not null,
        constraint wallets_player_currency_uq unique (player_id, currency),
        constraint wallets_identity_uq unique (id, player_id, currency),
        constraint wallets_currency_ck check (currency ~ '^[A-Z]{3}$'),
        constraint wallets_balance_non_negative_ck check (balance_amount >= 0),
        constraint wallets_version_ck check (version >= 1)
      )`);

    // ----------------------------------------------------- wager_transactions
    this.addSql(`
      create table wager_transactions (
        id uuid primary key,
        provider_id text not null,
        external_transaction_id text not null,
        idempotency_key text not null,
        payload_hash char(64) not null,
        wallet_id uuid not null,
        player_id uuid not null,
        currency char(3) not null,
        round_id text not null,
        game_id text not null,
        kind text not null,
        amount numeric(19,2) not null,
        reference_external_transaction_id text,
        reference_transaction_id uuid,
        status text not null,
        failure_code text,
        observed_balance_amount numeric(19,2),
        attempt_count integer not null default 0,
        next_attempt_at timestamptz,
        processed_at timestamptz,
        created_at timestamptz not null,

        constraint wager_transactions_idempotency_key_uq unique (idempotency_key),
        constraint wager_transactions_provider_external_uq unique (provider_id, external_transaction_id),
        -- targets of the composite foreign keys below
        constraint wager_transactions_ledger_target_uq unique (id, wallet_id, currency, amount),
        constraint wager_transactions_reference_target_uq
          unique (id, provider_id, external_transaction_id, wallet_id, currency, round_id),

        -- player and currency must be the wallet's own
        constraint wager_transactions_wallet_fk foreign key (wallet_id, player_id, currency)
          references wallets (id, player_id, currency) deferrable initially deferred,
        -- a reference belongs to the same provider, wallet (hence player), currency and round
        constraint wager_transactions_reference_fk
          foreign key (reference_transaction_id, provider_id, reference_external_transaction_id, wallet_id, currency, round_id)
          references wager_transactions (id, provider_id, external_transaction_id, wallet_id, currency, round_id)
          deferrable initially deferred,

        constraint wager_transactions_kind_ck
          check (kind in ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
        -- PENDING only exists in memory while the outcome is decided
        constraint wager_transactions_status_ck
          check (status in ('PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
        constraint wager_transactions_currency_ck check (currency ~ '^[A-Z]{3}$'),
        constraint wager_transactions_payload_hash_ck check (payload_hash ~ '^[0-9a-f]{64}$'),
        constraint wager_transactions_text_lengths_ck check (
          char_length(provider_id) between 1 and 100
          and char_length(external_transaction_id) between 1 and 200
          and char_length(idempotency_key) between 1 and 255
          and char_length(round_id) between 1 and 200
          and char_length(game_id) between 1 and 200
          and char_length(reference_external_transaction_id) between 1 and 200
        ),
        constraint wager_transactions_amount_ck
          check (amount > 0 or (kind = 'LOSS' and amount = 0)),
        -- OPENING is internal: only the reserved provider issues it, and it issues nothing else
        constraint wager_transactions_internal_provider_ck
          check ((kind = 'OPENING') = (provider_id = 'internal')),
        constraint wager_transactions_opening_processed_ck
          check (kind <> 'OPENING' or status = 'PROCESSED'),
        constraint wager_transactions_reference_required_ck
          check (kind not in ('REFUND', 'ROLLBACK') or reference_external_transaction_id is not null),
        constraint wager_transactions_reference_forbidden_ck
          check (kind not in ('OPENING', 'BET') or reference_external_transaction_id is null),
        constraint wager_transactions_self_reference_ck
          check (reference_external_transaction_id <> external_transaction_id),
        -- without the external id, MATCH SIMPLE would skip the reference foreign key
        constraint wager_transactions_reference_resolution_ck check (
          reference_transaction_id is null
          or (reference_external_transaction_id is not null and status = 'PROCESSED')
        ),
        constraint wager_transactions_processed_reference_resolved_ck check (
          status <> 'PROCESSED' or reference_external_transaction_id is null or reference_transaction_id is not null
        ),
        constraint wager_transactions_failure_code_ck
          check ((status in ('REJECTED', 'FAILED')) = (failure_code is not null)),
        constraint wager_transactions_failure_code_format_ck
          check (failure_code ~ '^[A-Z][A-Z0-9_]*$'),
        constraint wager_transactions_processed_at_ck
          check ((status = 'PROCESSED') = (processed_at is not null)),
        constraint wager_transactions_observed_balance_ck check (
          observed_balance_amount >= 0
          and (status not in ('PROCESSED', 'REJECTED') or observed_balance_amount is not null)
        ),
        constraint wager_transactions_pending_reference_ck check (
          status <> 'PENDING_REFERENCE'
          or (reference_external_transaction_id is not null and next_attempt_at is not null)
        ),
        constraint wager_transactions_attempt_count_ck check (attempt_count >= 0)
      )`);

    // A reference is reversed at most once, whatever the reversal kind (REFUND or ROLLBACK).
    this.addSql(`
      create unique index wager_transactions_single_active_reversal_uq
        on wager_transactions (provider_id, reference_external_transaction_id)
        where kind in ('REFUND', 'ROLLBACK') and status in ('PENDING_REFERENCE', 'PROCESSED')`);
    this.addSql(`
      create unique index wager_transactions_single_opening_uq
        on wager_transactions (wallet_id) where kind = 'OPENING'`);
    this.addSql(`
      create index wager_transactions_pending_due_idx
        on wager_transactions (next_attempt_at) where status = 'PENDING_REFERENCE'`);
    this.addSql(`
      create index wager_transactions_awaiting_reference_idx
        on wager_transactions (provider_id, reference_external_transaction_id)
        where status = 'PENDING_REFERENCE'`);

    // -------------------------------------------------- wallet_ledger_entries
    this.addSql(`
      create table wallet_ledger_entries (
        id uuid primary key,
        wallet_id uuid not null,
        transaction_id uuid not null,
        direction text not null,
        amount numeric(19,2) not null,
        currency char(3) not null,
        balance_before numeric(19,2) not null,
        balance_after numeric(19,2) not null,
        wallet_version integer not null,
        created_at timestamptz not null,
        -- at most one entry per transaction, and one per wallet version
        constraint wallet_ledger_entries_transaction_uq unique (transaction_id),
        constraint wallet_ledger_entries_wallet_version_uq unique (wallet_id, wallet_version),
        -- wallet, currency and amount must be the transaction's own
        constraint wallet_ledger_entries_transaction_fk foreign key (transaction_id, wallet_id, currency, amount)
          references wager_transactions (id, wallet_id, currency, amount) deferrable initially deferred,
        constraint wallet_ledger_entries_direction_ck check (direction in ('DEBIT', 'CREDIT')),
        constraint wallet_ledger_entries_amount_ck check (amount > 0),
        constraint wallet_ledger_entries_balances_ck check (balance_before >= 0 and balance_after >= 0),
        constraint wallet_ledger_entries_arithmetic_ck check (
          balance_after = case direction
            when 'CREDIT' then balance_before + amount
            else balance_before - amount
          end
        ),
        constraint wallet_ledger_entries_wallet_version_ck check (wallet_version >= 1)
      )`);
    this.addSql(`
      create index wallet_ledger_entries_wallet_created_idx
        on wallet_ledger_entries (wallet_id, created_at, id)`);

    // --------------------------------------------------------- inbox / outbox
    this.addSql(`
      create table inbox_messages (
        consumer_name text not null,
        message_id text not null,
        payload_hash char(64) not null,
        received_at timestamptz not null,
        processed_at timestamptz,
        constraint inbox_messages_pk primary key (consumer_name, message_id),
        constraint inbox_messages_key_lengths_ck
          check (char_length(consumer_name) between 1 and 255 and char_length(message_id) between 1 and 255),
        constraint inbox_messages_payload_hash_ck check (payload_hash ~ '^[0-9a-f]{64}$'),
        constraint inbox_messages_processed_after_received_ck check (processed_at >= received_at)
      )`);
    this.addSql(`
      create table outbox_messages (
        id uuid primary key,
        aggregate_id uuid not null,
        event_type text not null,
        payload jsonb not null,
        occurred_at timestamptz not null,
        attempts integer not null default 0,
        next_attempt_at timestamptz,
        published_at timestamptz,
        constraint outbox_messages_event_type_ck check (char_length(event_type) between 1 and 200),
        constraint outbox_messages_payload_ck check (jsonb_typeof(payload) = 'object'),
        constraint outbox_messages_attempts_ck check (attempts >= 0)
      )`);
    this.addSql(`
      create index outbox_messages_unpublished_idx
        on outbox_messages (next_attempt_at nulls first, occurred_at) where published_at is null`);

    // ------------------------------------------------------- wallet triggers
    this.addSql(`
      create function wallets_guard() returns trigger language plpgsql as $$
      begin
        if tg_op = 'INSERT' then
          if new.version <> 1 then
            raise exception 'wallet % must be created at version 1', new.id
              using errcode = 'check_violation', constraint = 'wallets_initial_version';
          end if;
          return new;
        end if;
        if (new.id, new.player_id, new.currency, new.created_at)
           is distinct from (old.id, old.player_id, old.currency, old.created_at) then
          raise exception 'wallet % identity columns are immutable', old.id
            using errcode = 'restrict_violation', constraint = 'wallets_identity_immutable';
        end if;
        if new.balance_amount <> old.balance_amount then
          if new.version <> old.version + 1 then
            raise exception 'wallet % balance changed without advancing version by one', old.id
              using errcode = 'check_violation', constraint = 'wallets_version_tracks_balance';
          end if;
        elsif new.version <> old.version then
          raise exception 'wallet % version changed without a balance change', old.id
            using errcode = 'check_violation', constraint = 'wallets_version_tracks_balance';
        end if;
        return new;
      end
      $$`);
    this.addSql(`
      create trigger wallets_guard before insert or update on wallets
        for each row execute function wallets_guard()`);

    this.addSql(`
      create function wallets_balance_matches_ledger() returns trigger language plpgsql as $$
      declare
        previous_balance numeric(19,2);
      begin
        if tg_op = 'INSERT' then
          if new.balance_amount = 0 then
            return null;
          end if;
          previous_balance := 0;
        elsif new.balance_amount = old.balance_amount then
          return null;
        else
          previous_balance := old.balance_amount;
        end if;
        if not exists (
          select 1 from wallet_ledger_entries e
          where e.wallet_id = new.id
            and e.wallet_version = new.version
            and e.balance_before = previous_balance
            and e.balance_after = new.balance_amount
        ) then
          raise exception 'wallet % moved from % to % (version %) without a matching ledger entry',
            new.id, previous_balance, new.balance_amount, new.version
            using errcode = 'check_violation', constraint = 'wallets_balance_matches_ledger';
        end if;
        return null;
      end
      $$`);
    this.addSql(`
      create constraint trigger wallets_balance_matches_ledger
        after insert or update on wallets deferrable initially deferred
        for each row execute function wallets_balance_matches_ledger()`);

    // -------------------------------------------------- transaction triggers
    this.addSql(`
      create function wager_transactions_guard() returns trigger language plpgsql as $$
      begin
        if tg_op = 'DELETE' then
          raise exception 'wager transaction % cannot be deleted', old.id
            using errcode = 'restrict_violation', constraint = 'wager_transactions_no_delete';
        end if;
        if old.status in ('PROCESSED', 'REJECTED', 'FAILED') then
          raise exception 'wager transaction % is % (terminal) and cannot change', old.id, old.status
            using errcode = 'restrict_violation', constraint = 'wager_transactions_terminal_immutable';
        end if;
        if (new.id, new.provider_id, new.external_transaction_id, new.idempotency_key, new.payload_hash,
            new.wallet_id, new.player_id, new.currency, new.round_id, new.game_id, new.kind, new.amount,
            new.reference_external_transaction_id, new.created_at)
           is distinct from
           (old.id, old.provider_id, old.external_transaction_id, old.idempotency_key, old.payload_hash,
            old.wallet_id, old.player_id, old.currency, old.round_id, old.game_id, old.kind, old.amount,
            old.reference_external_transaction_id, old.created_at) then
          raise exception 'wager transaction % identity and payload are immutable', old.id
            using errcode = 'restrict_violation', constraint = 'wager_transactions_payload_immutable';
        end if;
        return new;
      end
      $$`);
    this.addSql(`
      create trigger wager_transactions_guard before update or delete on wager_transactions
        for each row execute function wager_transactions_guard()`);

    this.addSql(`
      create function wager_transactions_consistency() returns trigger language plpgsql as $$
      declare
        ref record;
      begin
        if new.reference_transaction_id is not null then
          select kind, status, amount into ref from wager_transactions where id = new.reference_transaction_id;
          -- a missing row is reported by wager_transactions_reference_fk
          if found then
            if not ((new.kind in ('WIN', 'LOSS', 'REFUND') and ref.kind = 'BET')
                    or (new.kind = 'ROLLBACK' and ref.kind in ('BET', 'WIN', 'REFUND'))) then
              raise exception '% cannot reference a % transaction', new.kind, ref.kind
                using errcode = 'check_violation', constraint = 'wager_transactions_reference_kind';
            end if;
            if ref.status <> 'PROCESSED' then
              raise exception 'reference % of transaction % is %, not PROCESSED',
                new.reference_transaction_id, new.id, ref.status
                using errcode = 'check_violation', constraint = 'wager_transactions_reference_processed';
            end if;
            if new.kind in ('REFUND', 'ROLLBACK') and new.amount <> ref.amount then
              raise exception '% of % must equal the reference amount %', new.kind, new.amount, ref.amount
                using errcode = 'check_violation', constraint = 'wager_transactions_reversal_amount';
            end if;
          end if;
        end if;
        if new.status = 'PROCESSED' and new.kind <> 'LOSS'
           and not exists (select 1 from wallet_ledger_entries where transaction_id = new.id) then
          raise exception 'processed % transaction % has no ledger entry', new.kind, new.id
            using errcode = 'check_violation', constraint = 'wager_transactions_processed_has_ledger';
        end if;
        return null;
      end
      $$`);
    this.addSql(`
      create constraint trigger wager_transactions_consistency
        after insert or update on wager_transactions deferrable initially deferred
        for each row execute function wager_transactions_consistency()`);

    // ------------------------------------------------------- ledger triggers
    this.addSql(`
      create function wallet_ledger_entries_append_only() returns trigger language plpgsql as $$
      begin
        raise exception 'wallet_ledger_entries is append-only: % is not allowed', tg_op
          using errcode = 'restrict_violation', constraint = 'wallet_ledger_entries_append_only';
      end
      $$`);
    this.addSql(`
      create trigger wallet_ledger_entries_no_update_delete before update or delete on wallet_ledger_entries
        for each row execute function wallet_ledger_entries_append_only()`);
    this.addSql(`
      create trigger wallet_ledger_entries_no_truncate before truncate on wallet_ledger_entries
        for each statement execute function wallet_ledger_entries_append_only()`);

    this.addSql(`
      create function wallet_ledger_entries_chain() returns trigger language plpgsql as $$
      declare
        previous_after numeric(19,2);
      begin
        select balance_after into previous_after from wallet_ledger_entries
          where wallet_id = new.wallet_id and wallet_version = new.wallet_version - 1;
        if found then
          if previous_after <> new.balance_before then
            raise exception 'ledger entry for wallet % version % starts at %, previous entry ended at %',
              new.wallet_id, new.wallet_version, new.balance_before, previous_after
              using errcode = 'check_violation', constraint = 'wallet_ledger_entries_chain';
          end if;
        -- only the first movement may lack a predecessor: version 1 (opening credit) or
        -- version 2 when the wallet was opened with a zero balance
        elsif new.balance_before <> 0 or new.wallet_version > 2 then
          raise exception 'ledger entry for wallet % version % has no predecessor', new.wallet_id, new.wallet_version
            using errcode = 'check_violation', constraint = 'wallet_ledger_entries_chain';
        end if;
        return new;
      end
      $$`);
    this.addSql(`
      create trigger wallet_ledger_entries_chain before insert on wallet_ledger_entries
        for each row execute function wallet_ledger_entries_chain()`);

    this.addSql(`
      create function wallet_ledger_entries_consistency() returns trigger language plpgsql as $$
      declare
        tx record;
        w record;
        expected_direction text;
      begin
        select t.kind, t.status, r.kind as reference_kind into tx
          from wager_transactions t
          left join wager_transactions r on r.id = t.reference_transaction_id
          where t.id = new.transaction_id;
        -- a missing transaction is reported by wallet_ledger_entries_transaction_fk
        if found then
          if tx.status <> 'PROCESSED' or tx.kind = 'LOSS' then
            raise exception 'ledger entry % belongs to a % % transaction, which cannot move the balance',
              new.id, tx.status, tx.kind
              using errcode = 'check_violation', constraint = 'wallet_ledger_entries_transaction_applied';
          end if;
          expected_direction := case
            when tx.kind = 'BET' then 'DEBIT'
            when tx.kind = 'ROLLBACK' then case when tx.reference_kind = 'BET' then 'CREDIT' else 'DEBIT' end
            else 'CREDIT'
          end;
          if new.direction <> expected_direction then
            raise exception 'ledger entry % for % must be %, got %', new.id, tx.kind, expected_direction, new.direction
              using errcode = 'check_violation', constraint = 'wallet_ledger_entries_direction_matches_kind';
          end if;
        end if;
        select version, balance_amount into w from wallets where id = new.wallet_id;
        if w.version < new.wallet_version
           or (w.version = new.wallet_version and w.balance_amount <> new.balance_after) then
          raise exception 'ledger entry % (version %) does not match wallet % (version %, balance %)',
            new.id, new.wallet_version, new.wallet_id, w.version, w.balance_amount
            using errcode = 'check_violation', constraint = 'wallet_ledger_entries_match_wallet';
        end if;
        return null;
      end
      $$`);
    this.addSql(`
      create constraint trigger wallet_ledger_entries_consistency
        after insert on wallet_ledger_entries deferrable initially deferred
        for each row execute function wallet_ledger_entries_consistency()`);

    // ------------------------------------------------- inbox/outbox triggers
    this.addSql(`
      create function inbox_messages_guard() returns trigger language plpgsql as $$
      begin
        if (new.consumer_name, new.message_id, new.payload_hash, new.received_at)
           is distinct from (old.consumer_name, old.message_id, old.payload_hash, old.received_at) then
          raise exception 'inbox message %/% is immutable', old.consumer_name, old.message_id
            using errcode = 'restrict_violation', constraint = 'inbox_messages_immutable';
        end if;
        if old.processed_at is not null and new.processed_at is distinct from old.processed_at then
          raise exception 'inbox message %/% is already processed', old.consumer_name, old.message_id
            using errcode = 'restrict_violation', constraint = 'inbox_messages_processed_once';
        end if;
        return new;
      end
      $$`);
    this.addSql(`
      create trigger inbox_messages_guard before update on inbox_messages
        for each row execute function inbox_messages_guard()`);

    this.addSql(`
      create function outbox_messages_guard() returns trigger language plpgsql as $$
      begin
        if (new.id, new.aggregate_id, new.event_type, new.payload, new.occurred_at)
           is distinct from (old.id, old.aggregate_id, old.event_type, old.payload, old.occurred_at) then
          raise exception 'outbox message % is immutable', old.id
            using errcode = 'restrict_violation', constraint = 'outbox_messages_immutable';
        end if;
        if old.published_at is not null and new.published_at is distinct from old.published_at then
          raise exception 'outbox message % is already published', old.id
            using errcode = 'restrict_violation', constraint = 'outbox_messages_published_once';
        end if;
        if new.attempts < old.attempts then
          raise exception 'outbox message % attempts cannot decrease', old.id
            using errcode = 'check_violation', constraint = 'outbox_messages_attempts_monotonic';
        end if;
        return new;
      end
      $$`);
    this.addSql(`
      create trigger outbox_messages_guard before update on outbox_messages
        for each row execute function outbox_messages_guard()`);
  }

  override down(): void {
    this.addSql('drop table if exists outbox_messages');
    this.addSql('drop table if exists inbox_messages');
    this.addSql('drop table if exists wallet_ledger_entries');
    this.addSql('drop table if exists wager_transactions');
    this.addSql('drop table if exists wallets');
    for (const fn of [
      'outbox_messages_guard',
      'inbox_messages_guard',
      'wallet_ledger_entries_consistency',
      'wallet_ledger_entries_chain',
      'wallet_ledger_entries_append_only',
      'wager_transactions_consistency',
      'wager_transactions_guard',
      'wallets_balance_matches_ledger',
      'wallets_guard',
    ]) {
      this.addSql(`drop function if exists ${fn}()`);
    }
  }
}
