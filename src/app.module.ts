import {
  type DynamicModule,
  type MiddlewareConsumer,
  Module,
  type NestModule,
  type OnModuleDestroy,
} from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import { AuthGuard } from './common/auth/auth.guard';
import { ConfigModule } from './common/config/config.module';
import type { AppConfig } from './common/config/env';
import { HealthModule } from './common/health/health.module';
import { correlationMiddleware } from './common/http/correlation';
import { DomainExceptionFilter } from './common/http/domain-exception.filter';
import { createHttpDrainMiddleware, ShutdownModule, ShutdownService } from './common/http/shutdown.service';
import { DatabaseModule } from './database/database.module';
import { MessagingModule } from './messaging/messaging.module';
import { WageringModule } from './wagering/wagering.module';
import { WalletsModule } from './wallets/wallets.module';

@Module({})
export class AppModule implements NestModule, OnModuleDestroy {
  constructor(private readonly shutdown: ShutdownService) {}

  static forRoot(config: AppConfig): DynamicModule {
    return {
      module: AppModule,
      imports: [
        ConfigModule.forRoot(config),
        ShutdownModule,
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

  onModuleDestroy(): void {
    // AppModule has distance = 1, so NestJS invokes this before imported worker modules (distance >= 2).
    this.shutdown.beginShutdown('app.close');
  }

  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(correlationMiddleware, createHttpDrainMiddleware(this.shutdown)).forRoutes('{*path}');
  }
}
