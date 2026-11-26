import { Migration } from '@mikro-orm/migrations';

const IMMUTABLE_COLUMNS = ['id', 'aggregate_id', 'event_type', 'payload', 'occurred_at'];

/**
 * Production order of outbox events. Timestamps and UUIDv7 ids cannot order events written by
 * different processes (clock skew, equal timestamps), but an identity value drawn while the
 * wallet row is locked follows the order in which that wallet's events were produced.
 *
 * Events of one aggregate are published by ascending sequence_number; the partial index serves
 * the "oldest unpublished event of this aggregate" lookup. The guard makes the number immutable,
 * including `SET sequence_number = DEFAULT`, which would draw a new value.
 */
export class Migration20261126100000OutboxSequence extends Migration {
  override up(): void {
    this.addSql('alter table outbox_messages add column sequence_number bigint generated always as identity');
    this.addSql(`
      create index outbox_messages_aggregate_sequence_idx
        on outbox_messages (aggregate_id, sequence_number) where published_at is null`);
    this.addSql(outboxGuard([...IMMUTABLE_COLUMNS, 'sequence_number']));
  }

  override down(): void {
    this.addSql(outboxGuard(IMMUTABLE_COLUMNS));
    this.addSql('drop index if exists outbox_messages_aggregate_sequence_idx');
    this.addSql('alter table outbox_messages drop column if exists sequence_number');
  }
}

/**
 * CREATE OR REPLACE keeps the owner and the EXECUTE grants but replaces the function's SET
 * clauses, so the search_path pinned by ApplicationPrivileges is declared again here.
 */
function outboxGuard(immutableColumns: readonly string[]): string {
  const tuple = (row: 'new' | 'old') => immutableColumns.map((column) => `${row}.${column}`).join(', ');
  return `
    create or replace function outbox_messages_guard() returns trigger language plpgsql
      set search_path = public, pg_temp as $$
    begin
      if (${tuple('new')}) is distinct from (${tuple('old')}) then
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
    $$`;
}
