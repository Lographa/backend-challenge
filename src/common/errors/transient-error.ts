/** Serialization failure, deadlock, lock not available. */
const TRANSIENT_SQLSTATES = new Set(['40001', '40P01', '55P03']);
/** Connection exception, insufficient resources, operator intervention (shutdown, cancel, statement timeout). */
const TRANSIENT_SQLSTATE_CLASSES = new Set(['08', '53', '57']);
const TRANSIENT_SYSTEM_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'EAI_AGAIN',
]);
const MAX_CAUSE_DEPTH = 5;

/**
 * Whether the failure is in the infrastructure and retrying the same request can succeed.
 * Looks at the SQLSTATE or system error code of the error and of its causes.
 */
export function isTransientInfrastructureError(error: unknown): boolean {
  let current = error;
  for (let depth = 0; depth < MAX_CAUSE_DEPTH && current instanceof Error; depth++) {
    const code = (current as { code?: unknown }).code;
    if (
      typeof code === 'string' &&
      (TRANSIENT_SQLSTATES.has(code) ||
        TRANSIENT_SYSTEM_CODES.has(code) ||
        (code.length === 5 && TRANSIENT_SQLSTATE_CLASSES.has(code.slice(0, 2))))
    ) {
      return true;
    }
    current = current.cause;
  }
  return false;
}
