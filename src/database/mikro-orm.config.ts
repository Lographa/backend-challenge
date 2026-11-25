import { defineConfig, PostgreSqlDriver } from '@mikro-orm/postgresql';
import type { AppConfig } from '../common/config/env';

export function createOrmOptions(config: AppConfig) {
  return defineConfig({
    driver: PostgreSqlDriver,
    clientUrl: config.databaseUrl,
    pool: { min: 0, max: config.databasePoolMax },
    entities: [],
    discovery: { warnWhenNoEntities: false },
    allowGlobalContext: false,
    forceUtcTimezone: true,
    debug: false,
  });
}
