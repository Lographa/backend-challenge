import { MikroORM } from '@mikro-orm/postgresql';
import { AppConfig } from '../common/config/env';
import { createLogger } from '../common/observability/logger';
import { createOrmOptions } from './mikro-orm.config';

const COMMANDS = ['up', 'down', 'status'] as const;
type Command = (typeof COMMANDS)[number];

async function main(): Promise<void> {
  const logger = createLogger('log');
  const command = (process.argv[2] ?? 'up') as Command;
  if (!COMMANDS.includes(command)) {
    logger.fatal({ message: 'Unknown migration command', command, expected: COMMANDS }, 'Migrations');
    process.exit(1);
  }

  const orm = await MikroORM.init(createOrmOptions(AppConfig.load(process.env)));
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

await main();
