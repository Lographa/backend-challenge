import { expect } from 'bun:test';
import { DomainError } from '../../src/common/errors/domain.error';
import type { FailureCode } from '../../src/common/errors/failure-code';

export function expectDomainError(work: () => unknown, code: FailureCode): void {
  let caught: unknown;
  try {
    work();
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(DomainError);
  expect((caught as DomainError).code).toBe(code);
}
