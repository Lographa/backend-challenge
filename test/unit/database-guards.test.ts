import { describe, expect, test } from 'bun:test';
import { downMigrationBlockReason, migrationDatabaseUrl } from '../../src/database/migrate';
import { provisionDatabase } from '../../src/database/provision';
import { assertTestDatabaseHostAllowed } from '../support/database';

describe('migrationDatabaseUrl', () => {
  test('uses MIGRATION_DATABASE_URL', () => {
    const url = 'postgresql://owner:pw@localhost:5433/wagering';
    expect(migrationDatabaseUrl({ MIGRATION_DATABASE_URL: url, DATABASE_URL: 'postgresql://app:pw@localhost/x' })).toBe(
      url,
    );
  });

  test.each([[undefined], [''], ['   ']])('never falls back to DATABASE_URL (MIGRATION_DATABASE_URL=%p)', (value) => {
    expect(() =>
      migrationDatabaseUrl({ MIGRATION_DATABASE_URL: value, DATABASE_URL: 'postgresql://app:pw@localhost:5433/wagering' }),
    ).toThrow('MIGRATION_DATABASE_URL is required');
  });

  test('rejects URLs that are not PostgreSQL', () => {
    expect(() => migrationDatabaseUrl({ MIGRATION_DATABASE_URL: 'mysql://a:b@localhost/x' })).toThrow('postgres://');
    expect(() => migrationDatabaseUrl({ MIGRATION_DATABASE_URL: 'not a url' })).toThrow('postgres://');
  });
});

describe('downMigrationBlockReason', () => {
  test('allows databases whose name ends in _test', () => {
    expect(downMigrationBlockReason('wagering_test', {})).toBeUndefined();
  });

  test('blocks any other database by default and explains why', () => {
    const reason = downMigrationBlockReason('wagering', {});
    expect(reason).toContain('destroy financial data');
    expect(reason).toContain('MIGRATION_DOWN_CONFIRM_DATABASE=wagering');
  });

  test.each([[undefined], [''], ['  '], ['true'], ['1'], ['yes'], ['wagering_prod'], ['WAGERING']])(
    'a confirmation of %p does not authorize a down on "wagering"',
    (value) => {
      expect(downMigrationBlockReason('wagering', { MIGRATION_DOWN_CONFIRM_DATABASE: value })).toBeDefined();
    },
  );

  test('allows the down only when the confirmation names the exact database', () => {
    expect(downMigrationBlockReason('wagering', { MIGRATION_DOWN_CONFIRM_DATABASE: 'wagering' })).toBeUndefined();
  });
});

describe('assertTestDatabaseHostAllowed', () => {
  test.each([['localhost'], ['127.0.0.1'], ['[::1]']])('accepts the local host %s', (host) => {
    expect(() => assertTestDatabaseHostAllowed('DATABASE_URL', new URL(`postgresql://a:b@${host}:5433/x`), {})).not.toThrow();
  });

  test('rejects a remote host unless the override names it exactly', () => {
    const url = new URL('postgresql://a:b@db.example.com:5432/x');
    expect(() => assertTestDatabaseHostAllowed('DATABASE_URL', url, {})).toThrow('not local');
    expect(() =>
      assertTestDatabaseHostAllowed('DATABASE_URL', url, { ALLOW_NON_LOCAL_TEST_DATABASE_HOST: 'true' }),
    ).toThrow('not local');
    expect(() =>
      assertTestDatabaseHostAllowed('DATABASE_URL', url, { ALLOW_NON_LOCAL_TEST_DATABASE_HOST: 'db.example.com' }),
    ).not.toThrow();
  });
});

describe('provisionDatabase input validation (before any connection)', () => {
  const admin = 'postgresql://root:pw@localhost:5433/wagering';
  const app = 'postgresql://wagering_app:pw@localhost:5433/wagering';
  const owner = 'postgresql://wagering_migrator:pw@localhost:5433/wagering';

  test('rejects the same user for the application and migrations', async () => {
    await expect(provisionDatabase({ adminUrl: admin, applicationUrl: app, migrationUrl: app })).rejects.toThrow(
      'must be distinct',
    );
  });

  test('rejects reusing the admin user or the privilege group as a login user', async () => {
    await expect(
      provisionDatabase({ adminUrl: admin, applicationUrl: 'postgresql://root:pw@localhost:5433/wagering', migrationUrl: owner }),
    ).rejects.toThrow('must be distinct');
    await expect(
      provisionDatabase({
        adminUrl: admin,
        applicationUrl: 'postgresql://wagering_application:pw@localhost:5433/wagering',
        migrationUrl: owner,
      }),
    ).rejects.toThrow('must be distinct');
  });

  test('rejects different databases or servers', async () => {
    await expect(
      provisionDatabase({
        adminUrl: admin,
        applicationUrl: app,
        migrationUrl: 'postgresql://wagering_migrator:pw@localhost:5433/other',
      }),
    ).rejects.toThrow('same database');
    await expect(
      provisionDatabase({
        adminUrl: admin,
        applicationUrl: app,
        migrationUrl: 'postgresql://wagering_migrator:pw@localhost:5434/wagering',
      }),
    ).rejects.toThrow('same server');
  });

  test('rejects unsafe identifiers and missing passwords', async () => {
    await expect(
      provisionDatabase({ adminUrl: admin, applicationUrl: 'postgresql://Bad%22User:pw@localhost:5433/wagering', migrationUrl: owner }),
    ).rejects.toThrow();
    await expect(
      provisionDatabase({ adminUrl: admin, applicationUrl: 'postgresql://wagering_app@localhost:5433/wagering', migrationUrl: owner }),
    ).rejects.toThrow();
  });
});
