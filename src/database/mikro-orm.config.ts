import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, PostgreSqlDriver } from '@mikro-orm/postgresql';
import type { AppConfig } from '../common/config/env';
import { toPostgresApplicationName } from '../common/instance/instance-identity';
import { entitySchemas } from './entities';
import { migrations } from './migrations';

/**
 * Enables PostgreSQL 14+ periodic `epoll(EPOLLRDHUP)` client-disconnect checks (every 250ms) even
 * while a backend is sleeping inside `ProcSleep()` waiting on a heavyweight lock, ensuring locks
 * held by an abruptly crashed (`SIGKILL` / `TerminateProcess`) instance are aborted and released
 * within ~250ms rather than surviving until the awaited lock or OS 2h TCP keepalive expires.
 */
export const PG_CLIENT_CONNECTION_CHECK_OPTIONS = '-c client_connection_check_interval=250';

export function createOrmOptions(
  config: Pick<AppConfig, 'databaseUrl' | 'databasePoolMax'> & { readonly instanceId?: string },
) {
  return defineConfig({
    driver: PostgreSqlDriver,
    clientUrl: config.databaseUrl,
    pool: { min: 0, max: config.databasePoolMax },
    driverOptions: {
      options: PG_CLIENT_CONNECTION_CHECK_OPTIONS,
      keepAlive: true,
      keepAliveInitialDelayMillis: 1_000,
      ...(config.instanceId !== undefined && config.instanceId.length > 0
        ? {
            application_name: toPostgresApplicationName(config.instanceId),
          }
        : {}),
    },
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
