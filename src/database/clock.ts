import type { EntityManager } from '@mikro-orm/postgresql';

interface DatabaseClockRow {
  readonly now: Date | string;
}

/**
 * Reads PostgreSQL's real-time wall clock (`clock_timestamp()`) so independent OS processes
 * share a single authoritative time source for `next_attempt_at` eligibility, retry backoff,
 * `PENDING_REFERENCE` TTL evaluation, and `published_at` / `processed_at` stamps regardless
 * of host-clock skew across nodes.
 */
export async function readDatabaseClock(em: EntityManager): Promise<Date> {
  const rows = await em.execute<DatabaseClockRow[]>('select clock_timestamp() as now');
  const raw = rows[0]?.now;
  if (raw instanceof Date) {
    return raw;
  }
  if (typeof raw === 'string') {
    return new Date(raw);
  }
  return new Date();
}
