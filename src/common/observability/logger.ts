import { ConsoleLogger, type LogLevel } from '@nestjs/common';

const SEVERITY: LogLevel[] = ['fatal', 'error', 'warn', 'log', 'debug', 'verbose'];

export function createLogger(level: LogLevel): ConsoleLogger {
  return new ConsoleLogger({
    json: true,
    colors: false,
    logLevels: SEVERITY.slice(0, SEVERITY.indexOf(level) + 1),
  });
}
