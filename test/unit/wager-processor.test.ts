import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { describe, expect, test } from 'bun:test';
import { DomainError, InvalidStateError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { newId } from '../../src/common/ids';
import { parseWagerRequest } from '../../src/wagering/wager-payload';
import { type SubmitWagerCommand, WagerProcessorService } from '../../src/wagering/wager-processor.service';

const walletId = newId();
const playerId = newId();

function buildCommand(overrides: Record<string, unknown> = {}, idempotencyKey = 'idem-1'): SubmitWagerCommand {
  const parsed = parseWagerRequest(
    {
      providerId: 'provider-a',
      externalTransactionId: 'ext-1',
      playerId,
      walletId,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
      ...overrides,
    },
    idempotencyKey,
  );
  return { ...parsed, correlationId: 'corr-1' };
}

async function expectAsyncDomainError(work: () => Promise<unknown>, code: FailureCode): Promise<void> {
  let caught: unknown;
  try {
    await work();
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(DomainError);
  expect((caught as DomainError).code).toBe(code);
}

describe('WagerProcessorService boundary guards', () => {
  test('processInTransaction refuses an EntityManager that is not in an active transaction', async () => {
    const processor = new WagerProcessorService({} as MikroORM);
    const nonTransactionalEm = { isInTransaction: () => false } as unknown as EntityManager;

    await expect(processor.processInTransaction(nonTransactionalEm, buildCommand())).rejects.toBeInstanceOf(
      InvalidStateError,
    );
  });

  test.each<[string, Record<string, unknown>, FailureCode]>([
    ['OPENING kind from external caller', { kind: 'OPENING' }, FailureCode.UnsupportedKind],
    ['unknown kind', { kind: 'BONUS' }, FailureCode.UnsupportedKind],
    ['reserved internal providerId', { providerId: 'internal' }, FailureCode.InvalidPayload],
    ['zero BET', { kind: 'BET', money: { amount: '0.00', currency: 'BRL' } }, FailureCode.InvalidAmount],
    ['REFUND without reference', { kind: 'REFUND' }, FailureCode.ReferenceRequired],
    ['ROLLBACK without reference', { kind: 'ROLLBACK' }, FailureCode.ReferenceRequired],
    ['BET with reference', { kind: 'BET', referenceExternalTransactionId: 'ref-1' }, FailureCode.ReferenceNotAllowed],
    [
      'self-referencing REFUND',
      { kind: 'REFUND', externalTransactionId: 'same', referenceExternalTransactionId: 'same' },
      FailureCode.ReferenceNotAllowed,
    ],
  ])('process rejects %s (%s) before touching the database', async (_label, overrides, code) => {
    let touchedDb = false;
    const fakeOrm = {
      em: {
        fork: () => {
          touchedDb = true;
          throw new Error('should not touch database');
        },
      },
    } as unknown as MikroORM;
    const processor = new WagerProcessorService(fakeOrm);

    await expectAsyncDomainError(() => processor.process(buildCommand(overrides)), code);
    expect(touchedDb).toBe(false);
  });

  test('getById and getByExternalId validate identifiers before querying', async () => {
    const processor = new WagerProcessorService({} as MikroORM);

    await expectAsyncDomainError(() => processor.getById('not-a-uuid'), FailureCode.InvalidPayload);
    await expectAsyncDomainError(() => processor.getByExternalId('', 'ext-1'), FailureCode.InvalidPayload);
    await expectAsyncDomainError(() => processor.getByExternalId('provider-a', '   '), FailureCode.InvalidPayload);
    await expectAsyncDomainError(
      () => processor.getByExternalId('p'.repeat(101), 'ext-1'),
      FailureCode.InvalidPayload,
    );
    await expectAsyncDomainError(
      () => processor.getByExternalId('provider-a', 'e'.repeat(201)),
      FailureCode.InvalidPayload,
    );
  });
});
