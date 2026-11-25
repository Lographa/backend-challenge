import { type DynamicModule, Module } from '@nestjs/common';
import { ConfigModule } from './common/config/config.module';
import type { AppConfig } from './common/config/env';
import { HealthModule } from './common/health/health.module';
import { DatabaseModule } from './database/database.module';
import { MessagingModule } from './messaging/messaging.module';

@Module({})
export class AppModule {
  static forRoot(config: AppConfig): DynamicModule {
    return {
      module: AppModule,
      imports: [ConfigModule.forRoot(config), DatabaseModule, MessagingModule, HealthModule],
    };
  }
}
