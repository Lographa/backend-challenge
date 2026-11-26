import { type DynamicModule, type MiddlewareConsumer, Module, type NestModule } from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import { AuthGuard } from './common/auth/auth.guard';
import { ConfigModule } from './common/config/config.module';
import type { AppConfig } from './common/config/env';
import { HealthModule } from './common/health/health.module';
import { correlationMiddleware } from './common/http/correlation';
import { DomainExceptionFilter } from './common/http/domain-exception.filter';
import { DatabaseModule } from './database/database.module';
import { MessagingModule } from './messaging/messaging.module';
import { WageringModule } from './wagering/wagering.module';
import { WalletsModule } from './wallets/wallets.module';

@Module({})
export class AppModule implements NestModule {
  static forRoot(config: AppConfig): DynamicModule {
    return {
      module: AppModule,
      imports: [
        ConfigModule.forRoot(config),
        DatabaseModule,
        MessagingModule,
        HealthModule,
        WalletsModule,
        WageringModule,
      ],
      providers: [
        { provide: APP_FILTER, useClass: DomainExceptionFilter },
        { provide: APP_GUARD, useClass: AuthGuard },
      ],
    };
  }

  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(correlationMiddleware).forRoutes('{*path}');
  }
}
