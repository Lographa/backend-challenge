import { MikroORM } from '@mikro-orm/postgresql';
import { SQL } from 'bun';
import { expect } from 'bun:test';
import { AppConfig } from '../../src/common/config/env';
import { createOrmOptions } from '../../src/database/mikro-orm.config';

/**
 * Integration tests run against a dedicated database in the same Docker
 * PostgreSQL ("<dev db>_test"), so resetting the schema never touches dev data.
 */
export function testDatabaseUrl(): string {
  const url = new URL(requiredDatabaseUrl());
  url.pathname = `${url.pathname.replace(/^\//, '')}_test`;
  return url.toString();
}

export async function ensureTestDatabase(): Promise<void> {
  const name = new URL(testDatabaseUrl()).pathname.slice(1);
  const admin = new SQL(requiredDatabaseUrl());
  try {
    const existing = await admin`select 1 from pg_database where datname = ${name}`;
    if (existing.length === 0) {
      await admin.unsafe(`create database "${name}"`);
    }
  } finally {
    await admin.close();
  }
}

export async function createTestOrm(): Promise<MikroORM> {
  await ensureTestDatabase();
  const config = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl() });
  return MikroORM.init(createOrmOptions(config));
}

/** Drops everything through the real down() migrations, then applies them again. */
export async function resetSchema(orm: MikroORM): Promise<void> {
  await orm.migrator.down({ to: 0 });
  await orm.migrator.up();
}

export function createTestSql(): SQL {
  return new SQL(testDatabaseUrl());
}

/** Asserts that `work` fails with the given PostgreSQL constraint (or index) name. */
export async function expectConstraintViolation(work: Promise<unknown>, constraint: string): Promise<void> {
  const error = await work.then(
    () => undefined,
    (reason: unknown) => reason,
  );
  expect(error).toBeInstanceOf(Error);
  expect({ constraint: (error as { constraint?: string }).constraint, message: (error as Error).message }).toMatchObject({
    constraint,
  });
}

function requiredDatabaseUrl(): string {
  const url = process.env.DATABASE_URL;
  if (!url) {
    throw new Error('DATABASE_URL must be set (see .env.example) to run integration tests');
  }
  return url;
}
