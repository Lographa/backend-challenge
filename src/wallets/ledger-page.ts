import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';

export const DEFAULT_LEDGER_PAGE_SIZE = 50;
export const MAX_LEDGER_PAGE_SIZE = 200;

/** wallet_version is an integer column. */
const MAX_WALLET_VERSION = 2_147_483_647;
const CURSOR_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
const LIMIT_PATTERN = /^[1-9]\d{0,2}$/;

/**
 * Opaque position in a wallet's ledger: the last wallet version already returned. Versions are
 * contiguous and the ledger is append-only, so paging by version neither skips nor repeats entries.
 */
export function encodeLedgerCursor(walletVersion: number): string {
  return Buffer.from(JSON.stringify({ v: walletVersion })).toString('base64url');
}

/** The wallet version after which the next page starts. */
export function decodeLedgerCursor(cursor: unknown): number {
  const version = typeof cursor === 'string' && CURSOR_PATTERN.test(cursor) ? cursorVersion(cursor) : undefined;
  if (version === undefined) {
    throw new DomainError(FailureCode.InvalidPayload, 'cursor is not a cursor returned by this endpoint');
  }
  return version;
}

export function parseLedgerLimit(limit: unknown): number {
  if (limit === undefined) {
    return DEFAULT_LEDGER_PAGE_SIZE;
  }
  if (typeof limit !== 'string' || !LIMIT_PATTERN.test(limit) || Number(limit) > MAX_LEDGER_PAGE_SIZE) {
    throw new DomainError(FailureCode.InvalidPayload, `limit must be an integer from 1 to ${MAX_LEDGER_PAGE_SIZE}`);
  }
  return Number(limit);
}

function cursorVersion(cursor: string): number | undefined {
  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    return undefined;
  }
  const version = (decoded as { v?: unknown } | null)?.v;
  const valid =
    typeof version === 'number' &&
    Number.isInteger(version) &&
    version >= 1 &&
    version <= MAX_WALLET_VERSION &&
    // Only the exact encoding this module produces, not equivalent JSON with other keys or spacing.
    encodeLedgerCursor(version) === cursor;
  return valid ? version : undefined;
}
