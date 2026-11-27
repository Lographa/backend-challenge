import { type DynamicModule, Global, Module, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { AppConfig } from '../config/env';
import { MetricsRegistry } from './metrics';

@Global()
@Module({})
export class ObservabilityModule implements OnModuleInit, OnModuleDestroy {
  constructor(private readonly metrics: MetricsRegistry) {}

  static forRoot(): DynamicModule {
    return {
      module: ObservabilityModule,
      global: true,
      providers: [
        {
          provide: MetricsRegistry,
          useFactory: (config: AppConfig) => new MetricsRegistry(config.instanceId),
          inject: [AppConfig],
        },
      ],
      exports: [MetricsRegistry],
    };
  }

  onModuleInit(): void {
    this.metrics.startPeriodicSnapshot();
  }

  onModuleDestroy(): void {
    this.metrics.stopPeriodicSnapshot();
  }
}
