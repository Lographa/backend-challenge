import { MikroORM } from '@mikro-orm/postgresql';
import { createLogger } from '../common/observability/logger';
import { createOrmOptions } from './mikro-orm.config';

const COMMANDS = ['up', 'down', 'status'] as const;
type Command = (typeof COMMANDS)[number];

/** Must hold the exact name of the database being reverted; any other value (or none) blocks `down`. */
export const DOWN_CONFIRMATION_VARIABLE = 'MIGRATION_DOWN_CONFIRM_DATABASE';

/** Migrations run only as the schema owner; DATABASE_URL (the application user) is never a fallback. */
export function migrationDatabaseUrl(env: Record<string, string | undefined>): string {
  const value = env.MIGRATION_DATABASE_URL?.trim();
  if (!value) {
    throw new Error('MIGRATION_DATABASE_URL is required; migrations never use DATABASE_URL');
  }
  if (!URL.canParse(value) || !['postgres:', 'postgresql:'].includes(new URL(value).protocol)) {
    throw new Error('MIGRATION_DATABASE_URL must be a postgres:// or postgresql:// URL');
  }
  return value;
}

/**
 * A down migration drops tables, the financial ledger included; it is not a way to recover or
 * correct ledger data. Allowed on "*_test" databases, or when the confirmation variable names
 * the target database exactly.
 */
export function downMigrationBlockReason(
  databaseName: string,
  env: Record<string, string | undefined>,
): string | undefined {
  if (databaseName.endsWith('_test')) {
    return undefined;
  }
  if (env[DOWN_CONFIRMATION_VARIABLE]?.trim() === databaseName) {
    return undefined;
  }
  return (
    `Refusing to run a down migration against "${databaseName}": it can drop tables and destroy ` +
    `financial data, including the ledger, and does not restore it. To proceed anyway, set ` +
    `${DOWN_CONFIRMATION_VARIABLE}=${databaseName}.`
  );
}

async function main(): Promise<void> {
  const logger = createLogger('log');
  const command = (process.argv[2] ?? 'up') as Command;
  if (!COMMANDS.includes(command)) {
    logger.fatal({ message: 'Unknown migration command', command, expected: COMMANDS }, 'Migrations');
    process.exit(1);
  }

  let databaseUrl: string;
  try {
    databaseUrl = migrationDatabaseUrl(process.env);
  } catch (error) {
    logger.fatal({ message: (error as Error).message }, 'Migrations');
    process.exit(1);
  }

  if (command === 'down') {
    const database = decodeURIComponent(new URL(databaseUrl).pathname.slice(1));
    const blocked = downMigrationBlockReason(database, process.env);
    if (blocked !== undefined) {
      logger.fatal({ message: 'Down migration blocked', reason: blocked }, 'Migrations');
      process.exit(1);
    }
  }

  const orm = await MikroORM.init(createOrmOptions({ databaseUrl, databasePoolMax: 2 }));
  try {
    if (command === 'up') {
      const applied = await orm.migrator.up();
      logger.log({ message: 'Migrations applied', applied: applied.map((m) => m.name) }, 'Migrations');
    } else if (command === 'down') {
      const reverted = await orm.migrator.down();
      logger.log({ message: 'Last migration reverted', reverted: reverted.map((m) => m.name) }, 'Migrations');
    } else {
      const executed = await orm.migrator.getExecuted();
      const pending = await orm.migrator.getPending();
      logger.log(
        {
          message: 'Migration status',
          executed: executed.map((m) => m.name),
          pending: pending.map((m) => m.name),
        },
        'Migrations',
      );
    }
  } finally {
    await orm.close();
  }
}

if (import.meta.main) {
  await main();
}
