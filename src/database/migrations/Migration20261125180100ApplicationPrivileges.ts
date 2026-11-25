import { Migration } from '@mikro-orm/migrations';

/** NOLOGIN role created by `bun run db:provision`; the application's login user inherits from it. */
const APPLICATION_ROLE = 'wagering_application';

const FINANCIAL_TABLES = [
  'wallets',
  'wager_transactions',
  'wallet_ledger_entries',
  'inbox_messages',
  'outbox_messages',
];

const TRIGGER_FUNCTIONS = [
  'wallets_guard()',
  'wallets_balance_matches_ledger()',
  'wager_transactions_guard()',
  'wager_transactions_consistency()',
  'wallet_ledger_entries_append_only()',
  'wallet_ledger_entries_chain()',
  'wallet_ledger_entries_consistency()',
  'inbox_messages_guard()',
  'outbox_messages_guard()',
];

/**
 * Least privilege for the application role: no DELETE or TRUNCATE anywhere, the ledger is
 * SELECT/INSERT only, and UPDATE is limited to the columns that state transitions change.
 * Trigger functions run as the invoking user, so they get a fixed search_path (temporary
 * schema last) and lose the default EXECUTE grant to PUBLIC.
 */
export class Migration20261125180100ApplicationPrivileges extends Migration {
  override up(): void {
    this.addSql(`
      do $$
      begin
        if not exists (select 1 from pg_roles where rolname = '${APPLICATION_ROLE}') then
          raise exception 'role ${APPLICATION_ROLE} does not exist; run "bun run db:provision" before migrating';
        end if;
      end
      $$`);

    this.addSql(`revoke all on table ${FINANCIAL_TABLES.join(', ')} from public`);

    this.addSql(`grant select, insert on table wallets to ${APPLICATION_ROLE}`);
    this.addSql(`grant update (balance_amount, version, updated_at) on table wallets to ${APPLICATION_ROLE}`);

    this.addSql(`grant select, insert on table wager_transactions to ${APPLICATION_ROLE}`);
    this.addSql(`
      grant update (reference_transaction_id, status, failure_code, observed_balance_amount,
                    attempt_count, next_attempt_at, processed_at)
        on table wager_transactions to ${APPLICATION_ROLE}`);

    this.addSql(`grant select, insert on table wallet_ledger_entries to ${APPLICATION_ROLE}`);

    this.addSql(`grant select, insert on table inbox_messages to ${APPLICATION_ROLE}`);
    this.addSql(`grant update (processed_at) on table inbox_messages to ${APPLICATION_ROLE}`);

    this.addSql(`grant select, insert on table outbox_messages to ${APPLICATION_ROLE}`);
    this.addSql(`grant update (attempts, next_attempt_at, published_at) on table outbox_messages to ${APPLICATION_ROLE}`);

    for (const fn of TRIGGER_FUNCTIONS) {
      this.addSql(`revoke execute on function ${fn} from public`);
      this.addSql(`alter function ${fn} set search_path = public, pg_temp`);
    }
  }

  override down(): void {
    for (const fn of TRIGGER_FUNCTIONS) {
      this.addSql(`alter function ${fn} reset search_path`);
      this.addSql(`grant execute on function ${fn} to public`);
    }
    this.addSql(`revoke all on table ${FINANCIAL_TABLES.join(', ')} from ${APPLICATION_ROLE}`);
  }
}
