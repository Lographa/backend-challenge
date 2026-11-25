import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, PostgreSqlDriver } from '@mikro-orm/postgresql';
import type { AppConfig } from '../common/config/env';
import { entitySchemas } from './entities';
import { migrations } from './migrations';

export function createOrmOptions(config: Pick<AppConfig, 'databaseUrl' | 'databasePoolMax'>) {
  return defineConfig({
    driver: PostgreSqlDriver,
    clientUrl: config.databaseUrl,
    pool: { min: 0, max: config.databasePoolMax },
    entities: entitySchemas,
    extensions: [Migrator],
    migrations: {
      migrationsList: migrations,
      transactional: true,
      allOrNothing: true,
      // The schema is hand-written SQL (triggers, partial indexes); there is no snapshot to diff.
      snapshot: false,
    },
    allowGlobalContext: false,
    forceUtcTimezone: true,
    debug: false,
  });
}
