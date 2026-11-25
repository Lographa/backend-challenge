import type { FailureCode } from './failure-code';

/** A business rule was violated; the caller can act on `code`. */
export class DomainError extends Error {
  constructor(
    readonly code: FailureCode,
    message: string,
  ) {
    super(message);
    this.name = 'DomainError';
  }
}

/** A state transition that the code must never attempt — a bug, not a business outcome. */
export class InvalidStateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class InvalidTransactionStateError extends InvalidStateError {}
