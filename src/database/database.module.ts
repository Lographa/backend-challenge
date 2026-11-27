import { MikroOrmModule } from '@mikro-orm/nestjs';
import { MikroORM, PostgreSqlDriver } from '@mikro-orm/postgresql';
import { Module, type OnModuleInit, Optional } from '@nestjs/common';
import { AppConfig } from '../common/config/env';
import { ShutdownService } from '../common/http/shutdown.service';
import { createOrmOptions } from './mikro-orm.config';

const ORM_CLOSE_GUARDED = Symbol('ORM_CLOSE_GUARDED');

@Module({
  imports: [
    MikroOrmModule.forRootAsync({
      driver: PostgreSqlDriver,
      inject: [AppConfig],
      useFactory: (config: AppConfig) => createOrmOptions(config),
    }),
  ],
})
export class DatabaseModule implements OnModuleInit {
  constructor(
    private readonly orm: MikroORM,
    @Optional() private readonly shutdown?: ShutdownService,
  ) {}

  onModuleInit(): void {
    const shutdown = this.shutdown;
    if (shutdown === undefined) {
      return;
    }
    const guardedOrm = this.orm as MikroORM & { [ORM_CLOSE_GUARDED]?: boolean };
    if (guardedOrm[ORM_CLOSE_GUARDED] === true) {
      return;
    }
    guardedOrm[ORM_CLOSE_GUARDED] = true;

    const originalClose = this.orm.close.bind(this.orm);
    let closePromise: Promise<void> | undefined;

    this.orm.close = (force?: boolean): Promise<void> => {
      closePromise ??= (async () => {
        if (shutdown.isShuttingDown() && force !== true) {
          await shutdown.waitForResourceIdle('postgres');
        }
        try {
          await originalClose(force);
        } catch (error) {
          if (shutdown.isShuttingDown()) {
            const reason = error instanceof Error ? error.message : String(error);
            shutdown.markTeardownError(`MikroORM close failed: ${reason}`);
          }
          throw error;
        }
      })();
      return closePromise;
    };
  }
}
