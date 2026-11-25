import { MikroOrmModule } from '@mikro-orm/nestjs';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { Module } from '@nestjs/common';
import { AppConfig } from '../common/config/env';
import { createOrmOptions } from './mikro-orm.config';

@Module({
  imports: [
    MikroOrmModule.forRootAsync({
      driver: PostgreSqlDriver,
      inject: [AppConfig],
      useFactory: (config: AppConfig) => createOrmOptions(config),
    }),
  ],
})
export class DatabaseModule {}
