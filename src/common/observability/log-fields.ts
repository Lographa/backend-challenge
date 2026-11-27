import { DriverException } from '@mikro-orm/core';
import { DomainError } from '../errors/domain.error';
import { isTransientInfrastructureError } from '../errors/transient-error';

const SAFE_CODE = /^[A-Za-z0-9_]{1,32}$/;
const MAX_CAUSE_DEPTH = 5;

export interface SafeErrorFields {
  errorName: string;
  errorCode?: string;
}

/**
 * Name, and a short code when the error is a domain or driver/transient failure.
 * Never includes `Error.message`: driver messages embed SQL and bind parameters.
 */
export function safeErrorFields(error: unknown): SafeErrorFields {
  if (!(error instanceof Error)) {
    return { errorName: 'UnknownError' };
  }
  if (error instanceof DomainError) {
    return { errorName: error.name, errorCode: error.code };
  }
  const errorCode = firstSafeCode(error);
  if (error instanceof DriverException || isTransientInfrastructureError(error)) {
    return errorCode !== undefined ? { errorName: error.name, errorCode } : { errorName: error.name };
  }
  return { errorName: error.name };
}

/** Drops absent identifiers so a log line does not invent correlation or wallet ids. */
export function definedFields(
  fields: Readonly<Record<string, string | number | boolean | undefined>>,
): Record<string, string | number | boolean> {
  const defined: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined) {
      defined[key] = value;
    }
  }
  return defined;
}

function firstSafeCode(error: Error): string | undefined {
  let current: unknown = error;
  for (let depth = 0; depth < MAX_CAUSE_DEPTH && current instanceof Error; depth += 1) {
    const code = (current as { code?: unknown }).code;
    if (typeof code === 'string' && SAFE_CODE.test(code)) {
      return code;
    }
    current = current.cause;
  }
  return undefined;
}
