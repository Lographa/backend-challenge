import 'reflect-metadata';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfig, InvalidEnvironmentError } from './common/config/env';
import { createProcessShutdownController, ShutdownService } from './common/http/shutdown.service';
import { createLogger } from './common/observability/logger';

export function resolveBoundPort(httpServer: unknown, fallbackPort: number): number {
  const candidate = httpServer as Pick<Server, 'address'> | undefined;
  const addr = typeof candidate?.address === 'function' ? candidate.address() : null;
  if (addr !== null && typeof addr === 'object' && 'port' in addr) {
    return (addr as AddressInfo).port;
  }
  return fallbackPort;
}

export async function bootstrap(): Promise<void> {
  let config: AppConfig;
  try {
    config = AppConfig.load(process.env);
  } catch (error) {
    if (error instanceof InvalidEnvironmentError) {
      createLogger('error').fatal({ message: 'Invalid environment', issues: error.issues }, 'Bootstrap');
      process.exit(1);
    }
    throw error;
  }

  const structuredLogger = createLogger(config.logLevel, config.instanceId);
  let earlySignal: 'SIGTERM' | 'SIGINT' | undefined;
  const duplicateEarlySignals: Array<'SIGTERM' | 'SIGINT'> = [];
  let earlyWatchdog: ReturnType<typeof setTimeout> | undefined;

  const recordEarlySignal = (signal: 'SIGTERM' | 'SIGINT'): void => {
    if (earlySignal === undefined) {
      earlySignal = signal;
      earlyWatchdog = setTimeout(() => {
        process.exitCode = 1;
        structuredLogger.error(
          {
            message: 'Shutdown timed out while creating Nest application context; forcing exit',
            signal,
            timeoutMs: config.shutdownTimeoutMs,
          },
          undefined,
          'Bootstrap',
        );
        process.exit(1);
      }, config.shutdownTimeoutMs);
      return;
    }
    duplicateEarlySignals.push(signal);
  };

  const onEarlySigterm = (): void => recordEarlySignal('SIGTERM');
  const onEarlySigint = (): void => recordEarlySignal('SIGINT');
  process.on('SIGTERM', onEarlySigterm);
  process.on('SIGINT', onEarlySigint);

  const detachEarlyListeners = (): void => {
    if (earlyWatchdog !== undefined) {
      clearTimeout(earlyWatchdog);
      earlyWatchdog = undefined;
    }
    process.off('SIGTERM', onEarlySigterm);
    process.off('SIGINT', onEarlySigint);
  };

  let app: Awaited<ReturnType<typeof NestFactory.create>>;
  try {
    app = await NestFactory.create(AppModule.forRoot(config), {
      logger: structuredLogger,
    });
  } catch (error) {
    detachEarlyListeners();
    const reason = error instanceof Error ? error.message : String(error);
    structuredLogger.fatal({ message: 'Failed to create Nest application', reason }, 'Bootstrap');
    process.exit(1);
  }

  const logger = new Logger('Bootstrap');
  const shutdown = app.get(ShutdownService);
  const controller = createProcessShutdownController({
    app,
    shutdown,
    logger,
  });

  // Hand off from early listeners only AFTER controller listeners are attached so no signal gap exists.
  const capturedEarlySignal = earlySignal;
  detachEarlyListeners();

  if (capturedEarlySignal !== undefined) {
    for (const dup of duplicateEarlySignals) {
      logger.warn({
        message: 'Duplicate shutdown signal received during early bootstrap',
        signal: dup,
      });
    }
    await controller.triggerSignal(capturedEarlySignal);
    return;
  }

  const startupTask = (async (): Promise<boolean> => {
    await app.init();
    if (shutdown.isShuttingDown()) {
      logger.warn({
        message: 'Skipping HTTP listen because shutdown started during application initialization',
        reason: shutdown.reason(),
      });
      return false;
    }
    await app.listen(config.port);
    return true;
  })();

  shutdown.setBootstrapBarrier(startupTask);

  try {
    const listened = await startupTask;
    shutdown.setBootstrapBarrier(undefined);
    if (listened && !shutdown.isShuttingDown()) {
      const boundPort = resolveBoundPort(app.getHttpServer(), config.port);
      logger.log({
        message: 'HTTP server listening',
        port: boundPort,
        instanceId: config.instanceId,
      });
    }
  } catch (error) {
    shutdown.setBootstrapBarrier(undefined);
    await controller.shutdownAfterBootstrapFailure(error);
  }
}

void bootstrap();
