import { MikroORM } from '@mikro-orm/postgresql';
import { SQL } from 'bun';
import { expect } from 'bun:test';
import { createOrmOptions } from '../../src/database/mikro-orm.config';
import { provisionDatabase } from '../../src/database/provision';

/** admin: server superuser (setup only); migrator: schema owner; app: the application user. */
export type TestRole = 'admin' | 'migrator' | 'app';

const SOURCE_VARIABLES: Record<TestRole, string> = {
  admin: 'DATABASE_ADMIN_URL',
  migrator: 'MIGRATION_DATABASE_URL',
  app: 'DATABASE_URL',
};
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '::1', '[::1]']);
export const NON_LOCAL_TEST_HOST_VARIABLE = 'ALLOW_NON_LOCAL_TEST_DATABASE_HOST';

/** Refuses non-local servers unless the override names that exact host. */
export function assertTestDatabaseHostAllowed(
  variable: string,
  url: URL,
  env: Record<string, string | undefined>,
): void {
  if (LOCAL_HOSTS.has(url.hostname)) {
    return;
  }
  if (env[NON_LOCAL_TEST_HOST_VARIABLE]?.trim() === url.hostname) {
    return;
  }
  throw new Error(
    `${variable} points to "${url.hostname}", which is not local. Integration tests create a database and ` +
      `reset its schema; set ${NON_LOCAL_TEST_HOST_VARIABLE}=${url.hostname} only if that server is meant for tests.`,
  );
}

/**
 * Integration tests use "<database>_test" on the same server and with the same users as the
 * configured URLs, so resetting the schema never touches the development database.
 */
export function testDatabaseUrl(role: TestRole): string {
  const url = sourceUrl(role);
  url.pathname = `/${baseDatabaseName()}_test`;
  return url.toString();
}

export function testRoleName(role: TestRole): string {
  return decodeURIComponent(sourceUrl(role).username);
}

let prepared: Promise<void> | undefined;

/** Creates the test database if missing and provisions both users on it (idempotent, once per run). */
export function ensureTestDatabase(): Promise<void> {
  prepared ??= prepareTestDatabase();
  return prepared;
}

async function prepareTestDatabase(): Promise<void> {
  const name = `${baseDatabaseName()}_test`;
  const maintenance = sourceUrl('admin');
  maintenance.pathname = '/postgres';
  const admin = new SQL(maintenance.toString());
  try {
    const existing = await admin`select 1 from pg_database where datname = ${name}`;
    if (existing.length === 0) {
      await admin.unsafe(`create database "${name}"`);
    }
  } finally {
    await admin.close();
  }
  await provisionDatabase({
    adminUrl: testDatabaseUrl('admin'),
    applicationUrl: testDatabaseUrl('app'),
    migrationUrl: testDatabaseUrl('migrator'),
  });
}

export async function createTestOrm(role: 'migrator' | 'app' = 'migrator'): Promise<MikroORM> {
  await ensureTestDatabase();
  return MikroORM.init(createOrmOptions({ databaseUrl: testDatabaseUrl(role), databasePoolMax: 5 }));
}

/** Drops everything through the real down() migrations, then applies them again. Needs the migrator ORM. */
export async function resetSchema(orm: MikroORM): Promise<void> {
  await orm.migrator.down({ to: 0 });
  await orm.migrator.up();
}

export function createTestSql(role: TestRole = 'migrator'): SQL {
  return new SQL(testDatabaseUrl(role));
}

export interface CapturedPostgresError {
  errno?: string;
  constraint?: string;
  message: string;
}

/** Resolves with the PostgreSQL error raised by `work`; fails the test if it succeeds. */
export async function capturePostgresError(work: Promise<unknown>): Promise<CapturedPostgresError> {
  const error = await work.then(
    () => undefined,
    (reason: unknown) => reason,
  );
  expect(error).toBeInstanceOf(Error);
  const { errno, constraint, message } = error as Error & { errno?: string; constraint?: string };
  return { errno, constraint, message };
}

/** Asserts that `work` fails with the given PostgreSQL constraint (or index) name. */
export async function expectConstraintViolation(work: Promise<unknown>, constraint: string): Promise<void> {
  const error = await capturePostgresError(work);
  expect(error).toMatchObject({ constraint });
}

/** Asserts that `work` fails with SQLSTATE 42501 (insufficient_privilege). */
export async function expectPermissionDenied(work: Promise<unknown>): Promise<void> {
  const error = await capturePostgresError(work);
  expect(error).toMatchObject({ errno: '42501' });
}

function sourceUrl(role: TestRole): URL {
  const variable = SOURCE_VARIABLES[role];
  const value = process.env[variable]?.trim();
  if (!value || !URL.canParse(value)) {
    throw new Error(`${variable} must be set to a valid URL (see .env.example) to run integration tests`);
  }
  const url = new URL(value);
  assertTestDatabaseHostAllowed(variable, url, process.env);
  return url;
}

function baseDatabaseName(): string {
  const names = new Set(
    (Object.keys(SOURCE_VARIABLES) as TestRole[]).map((role) => decodeURIComponent(sourceUrl(role).pathname.slice(1))),
  );
  if (names.size !== 1) {
    throw new Error(
      `DATABASE_ADMIN_URL, MIGRATION_DATABASE_URL and DATABASE_URL must name the same database; got ${[...names].join(', ')}`,
    );
  }
  const [name] = names;
  if (name === undefined || name === '' || name.endsWith('_test')) {
    throw new Error('Configure the development database name; the "_test" suffix is added by the test setup');
  }
  return name;
}
