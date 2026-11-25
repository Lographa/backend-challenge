import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfig, InvalidEnvironmentError } from './common/config/env';
import { createLogger } from './common/observability/logger';

async function bootstrap(): Promise<void> {
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

  const app = await NestFactory.create(AppModule.forRoot(config), {
    logger: createLogger(config.logLevel),
  });
  app.enableShutdownHooks(['SIGTERM', 'SIGINT']);

  const logger = new Logger('Bootstrap');
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => {
      logger.log({ message: 'Shutdown signal received', signal });
      // Hooks that hang (e.g. a stuck DB pool) must not keep the process alive forever.
      setTimeout(() => {
        logger.error({ message: 'Graceful shutdown timed out', timeoutMs: config.shutdownTimeoutMs });
        process.exit(1);
      }, config.shutdownTimeoutMs).unref();
    });
  }

  await app.listen(config.port);
  logger.log({ message: 'HTTP server listening', port: config.port });
}

void bootstrap();
