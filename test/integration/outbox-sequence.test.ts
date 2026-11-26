import type { MikroORM } from '@mikro-orm/postgresql';
import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { InvalidStateError } from '../../src/common/errors/domain.error';
import { newId } from '../../src/common/ids';
import { IntegrationEvent, type IntegrationEventProps } from '../../src/common/messaging/integration-event';
import { OUTBOX_MAX_AUTO_ATTEMPTS, OutboxMessage } from '../../src/common/messaging/outbox-message';
import { OutboxMessageRecord } from '../../src/database/entities';
import { enqueueEvents } from '../../src/database/outbox';
import {
  capturePostgresError,
  createTestOrm,
  createTestSql,
  expectConstraintViolation,
  expectPermissionDenied,
  resetSchema,
  testRoleName,
} from '../support/database';

const occurredAt = new Date('2026-01-01T00:00:00.000Z');

class StepEvent extends IntegrationEvent<{ step: string }> {
  readonly eventType = 'step.happened';
  readonly version = 1;

  constructor(props: IntegrationEventProps<{ step: string }>) {
    super(props);
  }
}

function stepEvent(aggregateId: string, step: string, eventId = newId()): StepEvent {
  return new StepEvent({ eventId, aggregateId, correlationId: 'corr', occurredAt, data: { step } });
}

let orm: MikroORM;
let app: SQL;
let migrator: SQL;

beforeAll(async () => {
  const owner = await createTestOrm('migrator');
  try {
    await resetSchema(owner);
  } finally {
    await owner.close();
  }
  orm = await createTestOrm('app');
  app = createTestSql('app');
  migrator = createTestSql('migrator');
});

afterAll(async () => {
  await app.close();
  await migrator.close();
  await orm.close();
});

/** Steps of an aggregate in publication order, with their sequence numbers. */
async function stream(aggregateId: string): Promise<Array<{ id: string; step: string; sequence: bigint }>> {
  const rows: Array<{ id: string; step: string; sequence_number: string }> = await app`
    select id, payload #>> '{data,step}' as step, sequence_number from outbox_messages
    where aggregate_id = ${aggregateId} order by sequence_number`;
  return rows.map((row) => ({ id: row.id, step: row.step, sequence: BigInt(row.sequence_number) }));
}

async function count(aggregateId: string): Promise<number> {
  const [{ total }] = await app`select count(*)::int as total from outbox_messages where aggregate_id = ${aggregateId}`;
  return total;
}

function expectIncreasing(values: bigint[]): void {
  for (let i = 1; i < values.length; i++) {
    expect(values[i]! > values[i - 1]!).toBe(true);
  }
}

describe('enqueueEvents (as the application user)', () => {
  test('orders events by the order they were given, not by event id or timestamp', async () => {
    const walletA = newId();
    const walletB = newId();
    // UUIDv7 ids ascend with creation, so these sort in the opposite order to the insertion below.
    const [id1, id2, id3] = [newId(), newId(), newId()];
    const events = [
      stepEvent(walletA, 'a-first', id3),
      stepEvent(walletB, 'b-first'),
      stepEvent(walletA, 'a-second', id2),
      stepEvent(walletA, 'a-third', id1),
    ];

    await orm.em.fork().transactional((em) => enqueueEvents(em, events));

    const a = await stream(walletA);
    expect(a.map((row) => row.step)).toEqual(['a-first', 'a-second', 'a-third']);
    expect(a.map((row) => row.id)).toEqual([id3, id2, id1]);
    expectIncreasing(a.map((row) => row.sequence));
    const [b] = await stream(walletB);
    expect(b!.sequence > a[0]!.sequence && b!.sequence < a[1]!.sequence).toBe(true);
  });

  test('later transactions on the same aggregate continue after the earlier ones', async () => {
    const wallet = newId();
    await orm.em.fork().transactional((em) => enqueueEvents(em, [stepEvent(wallet, '1'), stepEvent(wallet, '2')]));
    await orm.em.fork().transactional((em) => enqueueEvents(em, [stepEvent(wallet, '3')]));

    const rows = await stream(wallet);
    expect(rows.map((row) => row.step)).toEqual(['1', '2', '3']);
    expectIncreasing(rows.map((row) => row.sequence));
  });

  test('the stored row is the enqueued message, readable through the record', async () => {
    const event = stepEvent(newId(), 'only');
    await orm.em.fork().transactional((em) => enqueueEvents(em, [event]));

    const record = await orm.em.fork().findOneOrFail(OutboxMessageRecord, event.eventId);
    expect(OutboxMessage.rehydrate(record).toState()).toEqual(OutboxMessage.enqueue(event).toState());
  });

  test('a rolled back transaction leaves none of its events', async () => {
    const wallet = newId();
    const work = orm.em.fork().transactional(async (em) => {
      await enqueueEvents(em, [stepEvent(wallet, '1'), stepEvent(wallet, '2')]);
      throw new Error('business decision failed after the events were written');
    });

    await expect(work).rejects.toThrow('business decision failed');
    expect(await count(wallet)).toBe(0);
  });

  test('refuses to write outside a transaction', async () => {
    const wallet = newId();
    await expect(enqueueEvents(orm.em.fork(), [stepEvent(wallet, '1')])).rejects.toThrow(InvalidStateError);
    expect(await count(wallet)).toBe(0);
  });
});

describe('quarantine is persisted with the granted columns only', () => {
  test('a quarantined message keeps its attempts and can be made due again through next_attempt_at', async () => {
    const event = stepEvent(newId(), 'stuck');
    await orm.em.fork().transactional((em) => enqueueEvents(em, [event]));

    await orm.em.fork().transactional(async (em) => {
      const record = await em.findOneOrFail(OutboxMessageRecord, event.eventId);
      const message = OutboxMessage.rehydrate(record);
      for (let i = 0; i < OUTBOX_MAX_AUTO_ATTEMPTS; i++) {
        message.scheduleRetry(occurredAt);
      }
      em.assign(record, { attempts: message.attempts, nextAttemptAt: message.nextAttemptAt ?? null });
    });

    const stored = OutboxMessage.rehydrate(await orm.em.fork().findOneOrFail(OutboxMessageRecord, event.eventId));
    expect(stored.isQuarantined()).toBe(true);
    expect(stored.attempts).toBe(OUTBOX_MAX_AUTO_ATTEMPTS);
    expect(stored.nextAttemptAt!.toISOString()).toBe('9999-12-31T23:59:59.999Z');

    const now = new Date();
    await app`update outbox_messages set next_attempt_at = ${now} where id = ${event.eventId}`;
    const [row] = await app`select attempts, published_at from outbox_messages where id = ${event.eventId}`;
    expect(row).toEqual({ attempts: OUTBOX_MAX_AUTO_ATTEMPTS, published_at: null });
    await expectConstraintViolation(
      app`update outbox_messages set attempts = 0 where id = ${event.eventId}`,
      'outbox_messages_attempts_monotonic',
    );
  });
});

describe('sequence_number is generated and immutable', () => {
  async function enqueueOne(): Promise<string> {
    const event = stepEvent(newId(), 'x');
    await orm.em.fork().transactional((em) => enqueueEvents(em, [event]));
    return event.eventId;
  }

  test('is a non-null bigint identity, generated always, indexed per aggregate for unpublished events', async () => {
    const [column] = await migrator`
      select data_type, is_nullable, is_identity, identity_generation from information_schema.columns
      where table_schema = 'public' and table_name = 'outbox_messages' and column_name = 'sequence_number'`;
    expect(column).toEqual({ data_type: 'bigint', is_nullable: 'NO', is_identity: 'YES', identity_generation: 'ALWAYS' });

    const [index] = await migrator`
      select indexdef from pg_indexes where indexname = 'outbox_messages_aggregate_sequence_idx'`;
    expect(index?.indexdef).toContain('(aggregate_id, sequence_number) WHERE (published_at IS NULL)');
  });

  test('the owner cannot renumber a message, not even to a freshly drawn value', async () => {
    const id = await enqueueOne();
    await expectConstraintViolation(
      migrator`update outbox_messages set sequence_number = default where id = ${id}`,
      'outbox_messages_immutable',
    );
    const explicit = await capturePostgresError(migrator`update outbox_messages set sequence_number = 1 where id = ${id}`);
    expect(explicit.errno).toBe('428C9');
  });

  test('the application user cannot update the column nor drive its sequence (42501)', async () => {
    const id = await enqueueOne();
    await expectPermissionDenied(app`update outbox_messages set sequence_number = default where id = ${id}`);
    await expectPermissionDenied(app`select nextval(pg_get_serial_sequence('outbox_messages', 'sequence_number'))`);
    await expectPermissionDenied(app`select setval(pg_get_serial_sequence('outbox_messages', 'sequence_number'), 1)`);
  });
});

describe('only PostgreSQL assigns sequence_number', () => {
  const forged = (sql: SQL, aggregateId: string, sequenceNumber: bigint | number, overriding: boolean) =>
    overriding
      ? sql`insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts, sequence_number)
            overriding system value
            values (${newId()}, ${aggregateId}, 'forged', ${{ step: 'forged' }}, ${occurredAt}, 0, ${sequenceNumber.toString()})`
      : sql`insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts, sequence_number)
            values (${newId()}, ${aggregateId}, 'forged', ${{ step: 'forged' }}, ${occurredAt}, 0, ${sequenceNumber.toString()})`;

  test('the application user cannot name the column, with or without OVERRIDING SYSTEM VALUE', async () => {
    const wallet = newId();
    await orm.em.fork().transactional((em) => enqueueEvents(em, [stepEvent(wallet, 'legit-1'), stepEvent(wallet, 'legit-2')]));
    const before = await stream(wallet);

    const explicit = await capturePostgresError(forged(app, wallet, 1, false));
    expect(explicit.errno).toBe('428C9');
    // A number below the pending events would put the forged event at the head of the aggregate.
    await expectPermissionDenied(forged(app, wallet, 1, true));
    // Duplicating the head would leave two events competing for the same position.
    await expectPermissionDenied(forged(app, wallet, before[0]!.sequence, true));
    await expectPermissionDenied(
      app`insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts, sequence_number)
          values (${newId()}, ${wallet}, 'forged', ${{ step: 'forged' }}, ${occurredAt}, 0, default)`,
    );

    expect(await stream(wallet)).toEqual(before);
  });

  test('a duplicate number is refused even for the owner', async () => {
    const wallet = newId();
    await orm.em.fork().transactional((em) => enqueueEvents(em, [stepEvent(wallet, 'legit')]));
    const [legit] = await stream(wallet);

    await expectConstraintViolation(forged(migrator, wallet, legit!.sequence, true), 'outbox_messages_sequence_number_uq');
    expect(await stream(wallet)).toEqual([legit!]);
  });

  test('the application user may insert every column except sequence_number, and nothing on its sequence', async () => {
    const appUser = testRoleName('app');
    const migratorUser = testRoleName('migrator');
    const columns = await migrator`
      select a.attname as column, has_column_privilege(${appUser}, 'outbox_messages', a.attname, 'INSERT') as insert
      from pg_attribute a
      where a.attrelid = 'outbox_messages'::regclass and a.attnum > 0 and not a.attisdropped order by a.attnum`;
    expect(columns).toEqual([
      { column: 'id', insert: true },
      { column: 'aggregate_id', insert: true },
      { column: 'event_type', insert: true },
      { column: 'payload', insert: true },
      { column: 'occurred_at', insert: true },
      { column: 'attempts', insert: true },
      { column: 'next_attempt_at', insert: true },
      { column: 'published_at', insert: true },
      { column: 'sequence_number', insert: false },
    ]);

    const [privileges] = await migrator`
      select has_table_privilege(${appUser}, 'outbox_messages', 'INSERT') as table_insert,
             has_table_privilege(${appUser}, 'outbox_messages', 'SELECT') as table_select,
             has_sequence_privilege(${appUser}, pg_get_serial_sequence('outbox_messages', 'sequence_number'), 'USAGE') as seq_usage,
             has_sequence_privilege(${appUser}, pg_get_serial_sequence('outbox_messages', 'sequence_number'), 'SELECT') as seq_select,
             has_sequence_privilege(${appUser}, pg_get_serial_sequence('outbox_messages', 'sequence_number'), 'UPDATE') as seq_update,
             (select relacl::text from pg_class where oid = 'outbox_messages'::regclass) as table_acl`;
    expect(privileges).toEqual({
      table_insert: false,
      table_select: true,
      seq_usage: false,
      seq_select: false,
      seq_update: false,
      // Granted to the group only: no entry for the login user or for PUBLIC.
      table_acl: `{${migratorUser}=arwdDxtm/${migratorUser},wagering_application=r/${migratorUser}}`,
    });
  });

  test('the ORM unit of work still inserts, and PostgreSQL numbers the row', async () => {
    const wallet = newId();
    const event = stepEvent(wallet, 'via-flush');
    await orm.em.fork().transactional((em) => {
      em.create(OutboxMessageRecord, OutboxMessage.enqueue(event).toState());
    });
    await orm.em.fork().transactional((em) => enqueueEvents(em, [stepEvent(wallet, 'via-insert')]));

    const rows = await stream(wallet);
    expect(rows.map((row) => row.step)).toEqual(['via-flush', 'via-insert']);
    expectIncreasing(rows.map((row) => row.sequence));
  });
});
