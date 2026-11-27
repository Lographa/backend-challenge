import { randomBytes } from 'node:crypto';
import { hostname } from 'node:os';

export const INSTANCE_ID_HEADER = 'x-instance-id';

const INSTANCE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,62}$/;
const PG_APPLICATION_NAME_MAX_LENGTH = 63;

export function isValidInstanceId(value: string): boolean {
  return INSTANCE_ID_PATTERN.test(value);
}

/**
 * Generates a collision-resistant per-process identifier safe for HTTP headers,
 * structured JSON logs, and PostgreSQL `application_name` (`<= 63` ASCII bytes).
 */
export function generateDefaultInstanceId(
  pid: number = process.pid,
  rawHostname: string = safeHostname(),
): string {
  const sanitizedHost = rawHostname
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 24) || 'node';
  const suffix = randomBytes(4).toString('hex');
  return `${sanitizedHost}-${pid}-${suffix}`;
}

export function toPostgresApplicationName(instanceId: string): string {
  const clean = instanceId.replace(/[^A-Za-z0-9._:-]/g, '_');
  return `wagering:${clean}`.slice(0, PG_APPLICATION_NAME_MAX_LENGTH);
}

export function computeJitteredDelayMs(
  baseMs: number,
  randomFn: () => number = Math.random,
  jitterRatio = 0.2,
): number {
  if (baseMs <= 0) {
    return 0;
  }
  const boundedRandom = Math.min(1, Math.max(0, randomFn()));
  const delta = (boundedRandom * 2 - 1) * jitterRatio;
  return Math.max(5, Math.round(baseMs * (1 + delta)));
}

function safeHostname(): string {
  try {
    return hostname();
  } catch {
    return 'node';
  }
}
