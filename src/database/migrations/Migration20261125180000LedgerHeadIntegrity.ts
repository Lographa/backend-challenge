import { Migration } from '@mikro-orm/migrations';

/**
 * The ledger is a contiguous chain per wallet and the wallet always sits at its head:
 *
 * - a new entry is appended at exactly MAX(wallet_version) + 1 (no retroactive or skipped versions);
 * - version 1 belongs to the OPENING credit only; a wallet opened with zero balance starts at version 2;
 * - at COMMIT, the head entry's version and balance equal the wallet's version and balance.
 *
 * The head comparison reads the wallet and the ledger as they are at COMMIT, so several movements
 * in one SQL transaction are checked against their final state, not against intermediate rows.
 */
export class Migration20261125180000LedgerHeadIntegrity extends Migration {
  override up(): void {
    // Installing stricter rules over data that already breaks them would hide the inconsistency.
    this.addSql(`
      do $$
      begin
        if exists (
          with chains as (
            select w.id, w.version, w.balance_amount,
                   count(e.id) as entries,
                   min(e.wallet_version) as first_version,
                   max(e.wallet_version) as last_version,
                   (array_agg(e.balance_after order by e.wallet_version desc))[1] as head_balance
            from wallets w
            left join wallet_ledger_entries e on e.wallet_id = w.id
            group by w.id
          )
          select 1 from chains
          where (entries = 0 and (version <> 1 or balance_amount <> 0))
             or (entries > 0 and (last_version <> version
                                  or head_balance <> balance_amount
                                  or first_version not in (1, 2)
                                  or entries <> last_version - first_version + 1))
          union all
          select 1 from wallet_ledger_entries e
          join wager_transactions t on t.id = e.transaction_id
          where (e.wallet_version = 1) <> (t.kind = 'OPENING')
        ) then
          raise exception 'existing wallets or ledger entries violate the ledger head rules; fix the data before migrating'
            using errcode = 'check_violation';
        end if;
      end
      $$`);

    this.addSql(`
      create or replace function wallet_ledger_entries_chain() returns trigger language plpgsql as $$
      declare
        head record;
      begin
        select wallet_version, balance_after into head from wallet_ledger_entries
          where wallet_id = new.wallet_id
          order by wallet_version desc
          limit 1;
        if found then
          if new.wallet_version <> head.wallet_version + 1 then
            raise exception 'ledger entry for wallet % must be appended at version %, got version %',
              new.wallet_id, head.wallet_version + 1, new.wallet_version
              using errcode = 'check_violation', constraint = 'wallet_ledger_entries_chain';
          end if;
          if new.balance_before <> head.balance_after then
            raise exception 'ledger entry for wallet % version % starts at %, previous entry ended at %',
              new.wallet_id, new.wallet_version, new.balance_before, head.balance_after
              using errcode = 'check_violation', constraint = 'wallet_ledger_entries_chain';
          end if;
        -- the first entry is the OPENING credit (version 1) or, for a wallet opened with
        -- zero balance, its first movement (version 2)
        elsif new.wallet_version not in (1, 2) or new.balance_before <> 0 then
          raise exception 'ledger entry for wallet % version % has no predecessor', new.wallet_id, new.wallet_version
            using errcode = 'check_violation', constraint = 'wallet_ledger_entries_chain';
        end if;
        return new;
      end
      $$`);

    this.addSql(`
      create or replace function wallet_ledger_entries_consistency() returns trigger language plpgsql as $$
      declare
        tx record;
        w record;
        head record;
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
          if (new.wallet_version = 1) <> (tx.kind = 'OPENING') then
            raise exception 'ledger entry % is at version % for a % transaction; version 1 is reserved for OPENING',
              new.id, new.wallet_version, tx.kind
              using errcode = 'check_violation', constraint = 'wallet_ledger_entries_opening_version';
          end if;
        end if;
        -- a missing wallet is reported by wager_transactions_wallet_fk
        select version, balance_amount into w from wallets where id = new.wallet_id;
        if found then
          select wallet_version, balance_after into head from wallet_ledger_entries
            where wallet_id = new.wallet_id
            order by wallet_version desc
            limit 1;
          if head.wallet_version <> w.version or head.balance_after <> w.balance_amount then
            raise exception 'ledger head of wallet % is version % (balance %), wallet is version % (balance %)',
              new.wallet_id, head.wallet_version, head.balance_after, w.version, w.balance_amount
              using errcode = 'check_violation', constraint = 'wallet_ledger_entries_match_wallet';
          end if;
        end if;
        return null;
      end
      $$`);

    this.addSql(`
      create or replace function wallets_balance_matches_ledger() returns trigger language plpgsql as $$
      declare
        previous_balance numeric(19,2);
        w record;
        head record;
      begin
        if (tg_op = 'INSERT' and new.balance_amount <> 0)
           or (tg_op = 'UPDATE' and new.balance_amount <> old.balance_amount) then
          previous_balance := case when tg_op = 'INSERT' then 0 else old.balance_amount end;
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
        end if;
        -- NEW may be an intermediate row of this transaction; the head rule applies to the final one
        select version, balance_amount into w from wallets where id = new.id;
        select wallet_version, balance_after into head from wallet_ledger_entries
          where wallet_id = new.id
          order by wallet_version desc
          limit 1;
        if not found then
          if w.version <> 1 or w.balance_amount <> 0 then
            raise exception 'wallet % is at version % with balance % but has no ledger entries',
              new.id, w.version, w.balance_amount
              using errcode = 'check_violation', constraint = 'wallets_balance_matches_ledger';
          end if;
        elsif head.wallet_version <> w.version or head.balance_after <> w.balance_amount then
          raise exception 'wallet % is version % (balance %), its ledger head is version % (balance %)',
            new.id, w.version, w.balance_amount, head.wallet_version, head.balance_after
            using errcode = 'check_violation', constraint = 'wallets_balance_matches_ledger';
        end if;
        return null;
      end
      $$`);
  }

  /** Restores the function bodies of Migration20261125140000InitialSchema. */
  override down(): void {
    this.addSql(`
      create or replace function wallet_ledger_entries_chain() returns trigger language plpgsql as $$
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
      create or replace function wallet_ledger_entries_consistency() returns trigger language plpgsql as $$
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
      create or replace function wallets_balance_matches_ledger() returns trigger language plpgsql as $$
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
  }
}
