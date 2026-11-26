import { Migration } from '@mikro-orm/migrations';

const APPLICATION_ROLE = 'wagering_application';

/** Every outbox column except sequence_number. */
const INSERTABLE_COLUMNS = [
  'id',
  'aggregate_id',
  'event_type',
  'payload',
  'occurred_at',
  'attempts',
  'next_attempt_at',
  'published_at',
].join(', ');

/**
 * Only PostgreSQL assigns outbox sequence numbers. A table-wide INSERT grant covers every column,
 * so `INSERT ... OVERRIDING SYSTEM VALUE` could pick any number, duplicates included, and move an
 * event ahead of its aggregate's pending ones. Column-level INSERT leaves sequence_number out; the
 * unique constraint also binds the owner.
 */
export class Migration20261126110000OutboxSequenceIntegrity extends Migration {
  override up(): void {
    this.addSql(`
      do $$
      begin
        if exists (select 1 from outbox_messages group by sequence_number having count(*) > 1) then
          raise exception 'outbox_messages has duplicate sequence_number values; resolve them before migrating';
        end if;
      end
      $$`);
    this.addSql('alter table outbox_messages add constraint outbox_messages_sequence_number_uq unique (sequence_number)');
    this.addSql(`revoke insert on table outbox_messages from ${APPLICATION_ROLE}`);
    this.addSql(`grant insert (${INSERTABLE_COLUMNS}) on table outbox_messages to ${APPLICATION_ROLE}`);
  }

  override down(): void {
    this.addSql(`revoke insert (${INSERTABLE_COLUMNS}) on table outbox_messages from ${APPLICATION_ROLE}`);
    this.addSql(`grant insert on table outbox_messages to ${APPLICATION_ROLE}`);
    this.addSql('alter table outbox_messages drop constraint if exists outbox_messages_sequence_number_uq');
  }
}
