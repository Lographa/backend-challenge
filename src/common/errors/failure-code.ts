/**
 * Stable, machine-readable reasons a provider can act on: fix the payload,
 * retry later, or give up. Values are persisted and published; never rename.
 */
export enum FailureCode {
  InvalidPayload = 'INVALID_PAYLOAD',
  InvalidAmount = 'INVALID_AMOUNT',
  InvalidCurrency = 'INVALID_CURRENCY',
  AmountOutOfRange = 'AMOUNT_OUT_OF_RANGE',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  UnsupportedKind = 'UNSUPPORTED_KIND',
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  NegativeBalanceReversal = 'NEGATIVE_BALANCE_REVERSAL',
  ReferenceRequired = 'REFERENCE_REQUIRED',
  ReferenceNotAllowed = 'REFERENCE_NOT_ALLOWED',
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  ReferenceKindNotAllowed = 'REFERENCE_KIND_NOT_ALLOWED',
  ReferenceScopeMismatch = 'REFERENCE_SCOPE_MISMATCH',
  ReferenceAlreadyReversed = 'REFERENCE_ALREADY_REVERSED',
  AmountMismatch = 'AMOUNT_MISMATCH',
  IdempotencyPayloadConflict = 'IDEMPOTENCY_PAYLOAD_CONFLICT',
  WalletAlreadyExists = 'WALLET_ALREADY_EXISTS',
  WalletNotFound = 'WALLET_NOT_FOUND',
  WalletPlayerMismatch = 'WALLET_PLAYER_MISMATCH',
  TransactionNotFound = 'TRANSACTION_NOT_FOUND',
  PermanentIntegrityError = 'PERMANENT_INTEGRITY_ERROR',
}
