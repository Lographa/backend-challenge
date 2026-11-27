import { ConsoleLogger, type ConsoleLoggerOptions, type LogLevel } from '@nestjs/common';

const SEVERITY: LogLevel[] = ['fatal', 'error', 'warn', 'log', 'debug', 'verbose'];

class StructuredConsoleLogger extends ConsoleLogger {
  constructor(
    options: ConsoleLoggerOptions,
    private readonly instanceId?: string,
  ) {
    super(options);
  }

  protected override getJsonLogObject(
    message: unknown,
    options: Parameters<ConsoleLogger['getJsonLogObject']>[1],
  ): ReturnType<ConsoleLogger['getJsonLogObject']> {
    const base = super.getJsonLogObject(message, options);
    if (this.instanceId === undefined) {
      return base;
    }
    return {
      ...base,
      instanceId: this.instanceId,
    } as ReturnType<ConsoleLogger['getJsonLogObject']>;
  }
}

export function createLogger(level: LogLevel, instanceId?: string): ConsoleLogger {
  return new StructuredConsoleLogger(
    {
      json: true,
      colors: false,
      logLevels: SEVERITY.slice(0, SEVERITY.indexOf(level) + 1),
    },
    instanceId,
  );
}
