import 'reflect-metadata';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { SQL } from 'bun';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { AppModule } from '../../src/app.module';
import { AppConfig } from '../../src/common/config/env';
import { DomainError } from '../../src/common/errors/domain.error';
import { FailureCode } from '../../src/common/errors/failure-code';
import { canonicalHash } from '../../src/common/hashing/canonical-hash';
import { newId } from '../../src/common/ids';
import { InboxMessageRecord } from '../../src/database/entities';
import { parseWagerRequest } from '../../src/wagering/wager-payload';
import { WagerProcessorService } from '../../src/wagering/wager-processor.service';
import { createTestOrm, createTestSql, resetSchema, testDatabaseUrl } from '../support/database';

type Json = Record<string, any>;

let app: INestApplication;
let baseUrl: string;
let orm: MikroORM;
let db: SQL;
let processor: WagerProcessorService;
const createdWalletIds: string[] = [];

beforeAll(async () => {
  const migrator = await createTestOrm('migrator');
  try {
    await resetSchema(migrator);
  } finally {
    await migrator.close();
  }
  const config = AppConfig.load({ ...process.env, DATABASE_URL: testDatabaseUrl('app') });
  app = await NestFactory.create(AppModule.forRoot(config), { logger: false });
  await app.listen(0);
  baseUrl = await app.getUrl();
  processor = app.get(WagerProcessorService);
  orm = await createTestOrm('app');
  db = createTestSql('app');
});

afterAll(async () => {
  await app?.close();
  await orm?.close();
  await db?.close();
});

async function call(
  method: 'GET' | 'POST',
  path: string,
  body?: unknown,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: Json; headers: Headers }> {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as Json, headers: response.headers };
}

async function openWallet(amount: string, playerId = newId(), currency = 'BRL'): Promise<Json> {
  const response = await call('POST', '/wallets', { playerId, initialBalance: { amount, currency } });
  expect(response.status).toBe(201);
  createdWalletIds.push(response.body.id);
  return response.body;
}

async function submitWager(
  payload: Record<string, unknown>,
  idempotencyKey?: string,
  extraHeaders: Record<string, string> = {},
): Promise<{ status: number; body: Json; headers: Headers }> {
  const key =
    idempotencyKey ??
    `${String(payload.providerId ?? 'provider-a')}:${String(payload.externalTransactionId ?? newId())}`;
  return call('POST', '/wagering/transactions', payload, {
    'idempotency-key': key,
    ...extraHeaders,
  });
}

async function outboxEventsForTransaction(transactionId: string): Promise<Json[]> {
  return db`
    select sequence_number, event_type, aggregate_id, payload
    from outbox_messages
    where aggregate_id = ${transactionId}
       or payload -> 'data' ->> 'transactionId' = ${transactionId}
    order by sequence_number asc
  `;
}

describe('Wagering processor — five kinds, ledger and outbox', () => {
  test('processes BET, WIN (with and without reference), LOSS (positive and 0.00), REFUND and ROLLBACK', async () => {
    const wallet = await openWallet('200.00');
    const providerId = `prov-${newId()}`;

    // 1. BET 50.00 -> 150.00 (version 2)
    const bet = await submitWager(
      {
        providerId,
        externalTransactionId: 'bet-1',
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '50.00', currency: 'BRL' },
      },
      `${providerId}:bet-1`,
      { 'x-correlation-id': 'corr-bet-1' },
    );
    expect(bet.status).toBe(200);
    expect(bet.body).toEqual({
      transactionId: expect.any(String),
      status: 'PROCESSED',
      balance: { amount: '150.00', currency: 'BRL' },
      idempotentReplay: false,
    });

    const betEvents = await outboxEventsForTransaction(bet.body.transactionId);
    expect(betEvents.map((e) => e.event_type)).toEqual([
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
    ]);
    expect(BigInt(betEvents[0]!.sequence_number)).toBeLessThan(BigInt(betEvents[1]!.sequence_number));
    expect(betEvents[0]!.payload.correlationId).toBe('corr-bet-1');
    expect(betEvents[1]!.payload.correlationId).toBe('corr-bet-1');

    // 2. WIN 120.00 referencing bet-1 -> 270.00 (version 3)
    const winWithRef = await submitWager({
      providerId,
      externalTransactionId: 'win-1',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '120.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-1',
    });
    expect(winWithRef.status).toBe(200);
    expect(winWithRef.body.balance).toEqual({ amount: '270.00', currency: 'BRL' });

    const winDetails = await call('GET', `/wagering/transactions/${winWithRef.body.transactionId}`);
    expect(winDetails.status).toBe(200);
    expect(winDetails.body.referenceTransactionId).toBe(bet.body.transactionId);
    expect(winDetails.body.referenceExternalTransactionId).toBe('bet-1');

    // 3. Standalone WIN 30.00 without reference -> 300.00 (version 4)
    const winStandalone = await submitWager({
      providerId,
      externalTransactionId: 'win-standalone',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-2',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '30.00', currency: 'BRL' },
    });
    expect(winStandalone.status).toBe(200);
    expect(winStandalone.body.balance).toEqual({ amount: '300.00', currency: 'BRL' });

    // 4. LOSS (with 50.00 and with 0.00) -> does NOT change wallet balance or version, emits no WalletBalanceChanged
    const lossPositive = await submitWager({
      providerId,
      externalTransactionId: 'loss-1',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'LOSS',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-1',
    });
    expect(lossPositive.status).toBe(200);
    expect(lossPositive.body.balance).toEqual({ amount: '300.00', currency: 'BRL' });

    const lossZero = await submitWager({
      providerId,
      externalTransactionId: 'loss-zero',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-2',
      gameId: 'fortune-chimp',
      kind: 'LOSS',
      money: { amount: '0.00', currency: 'BRL' },
    });
    expect(lossZero.status).toBe(200);
    expect(lossZero.body.balance).toEqual({ amount: '300.00', currency: 'BRL' });

    const lossEvents = await outboxEventsForTransaction(lossPositive.body.transactionId);
    expect(lossEvents.map((e) => e.event_type)).toEqual(['WagerTransactionProcessed']);

    const walletAfterLoss = await call('GET', `/wallets/${wallet.id}`);
    expect(walletAfterLoss.body.version).toBe(4);
    expect(walletAfterLoss.body.balance).toEqual({ amount: '300.00', currency: 'BRL' });

    // 5. REFUND of bet-1 (50.00 CREDIT) -> 350.00 (version 5)
    const refund = await submitWager({
      providerId,
      externalTransactionId: 'refund-bet-1',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-1',
    });
    expect(refund.status).toBe(200);
    expect(refund.body.balance).toEqual({ amount: '350.00', currency: 'BRL' });

    // 6. ROLLBACK of win-1 (120.00 DEBIT) -> 230.00 (version 6)
    const rollbackWin = await submitWager({
      providerId,
      externalTransactionId: 'rollback-win-1',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '120.00', currency: 'BRL' },
      referenceExternalTransactionId: 'win-1',
    });
    expect(rollbackWin.status).toBe(200);
    expect(rollbackWin.body.balance).toEqual({ amount: '230.00', currency: 'BRL' });

    // 7. ROLLBACK of refund-bet-1 (50.00 DEBIT) -> 180.00 (version 7)
    const rollbackRefund = await submitWager({
      providerId,
      externalTransactionId: 'rollback-refund-1',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '50.00', currency: 'BRL' },
      referenceExternalTransactionId: 'refund-bet-1',
    });
    expect(rollbackRefund.status).toBe(200);
    expect(rollbackRefund.body.balance).toEqual({ amount: '180.00', currency: 'BRL' });

    // 8. Second BET + ROLLBACK of BET (40.00 DEBIT -> 140.00, then 40.00 CREDIT -> 180.00, version 9)
    const bet2 = await submitWager({
      providerId,
      externalTransactionId: 'bet-2',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-3',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '40.00', currency: 'BRL' },
    });
    expect(bet2.status).toBe(200);
    expect(bet2.body.balance).toEqual({ amount: '140.00', currency: 'BRL' });

    const rollbackBet2 = await submitWager({
      providerId,
      externalTransactionId: 'rollback-bet-2',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-3',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '40.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-2',
    });
    expect(rollbackBet2.status).toBe(200);
    expect(rollbackBet2.body.balance).toEqual({ amount: '180.00', currency: 'BRL' });

    const finalWallet = await call('GET', `/wallets/${wallet.id}`);
    expect(finalWallet.body).toEqual({
      id: wallet.id,
      playerId: wallet.playerId,
      balance: { amount: '180.00', currency: 'BRL' },
      version: 9,
    });
  });
});

describe('Wagering processor — persistent idempotency, replay and payload conflicts', () => {
  test('PROCESSED replay returns historical observedBalance even after later wallet movements', async () => {
    const wallet = await openWallet('100.00');
    const providerId = `prov-${newId()}`;
    const key = `${providerId}:bet-hist`;

    const first = await submitWager(
      {
        providerId,
        externalTransactionId: 'bet-hist',
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-10',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '25.00', currency: 'BRL' },
      },
      key,
    );
    expect(first.status).toBe(200);
    expect(first.body.balance).toEqual({ amount: '75.00', currency: 'BRL' });
    expect(first.body.idempotentReplay).toBe(false);

    // Move wallet balance from 75.00 to 135.00
    const laterWin = await submitWager({
      providerId,
      externalTransactionId: 'win-later',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-10',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '60.00', currency: 'BRL' },
    });
    expect(laterWin.status).toBe(200);
    expect(laterWin.body.balance).toEqual({ amount: '135.00', currency: 'BRL' });

    const beforeReplayEvents = await outboxEventsForTransaction(first.body.transactionId);

    // Replay first BET with shuffled key order and extra transport metadata
    const replay = await submitWager(
      {
        money: { currency: 'BRL', amount: '25.00' },
        kind: 'BET',
        gameId: 'fortune-chimp',
        roundId: 'round-10',
        walletId: wallet.id,
        playerId: wallet.playerId,
        externalTransactionId: 'bet-hist',
        providerId,
        messageId: 'sqs-envelope-ignored',
      },
      key,
    );
    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({
      transactionId: first.body.transactionId,
      status: 'PROCESSED',
      balance: { amount: '75.00', currency: 'BRL' },
      idempotentReplay: true,
    });

    // Current wallet balance and outbox remain untouched
    const currentWallet = await call('GET', `/wallets/${wallet.id}`);
    expect(currentWallet.body.balance).toEqual({ amount: '135.00', currency: 'BRL' });
    expect(currentWallet.body.version).toBe(3);

    const afterReplayEvents = await outboxEventsForTransaction(first.body.transactionId);
    expect(afterReplayEvents).toHaveLength(beforeReplayEvents.length);
  });

  test('REJECTED transaction is terminal: replay keeps REJECTED and historical observedBalance even after deposit', async () => {
    const wallet = await openWallet('15.00');
    const providerId = `prov-${newId()}`;
    const key = `${providerId}:bet-too-high`;
    const payload = {
      providerId,
      externalTransactionId: 'bet-too-high',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-20',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '50.00', currency: 'BRL' },
    };

    const rejected = await submitWager(payload, key);
    expect(rejected.status).toBe(422);
    expect(rejected.body).toEqual({
      transactionId: expect.any(String),
      status: 'REJECTED',
      failureCode: 'INSUFFICIENT_FUNDS',
      balance: { amount: '15.00', currency: 'BRL' },
      idempotentReplay: false,
    });

    const rejectedEvents = await outboxEventsForTransaction(rejected.body.transactionId);
    expect(rejectedEvents.map((e) => e.event_type)).toEqual(['WagerTransactionRejected']);

    // Credit 100.00 so wallet now has 115.00 (more than enough for 50.00!)
    const win = await submitWager({
      providerId,
      externalTransactionId: 'win-topup',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-21',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '100.00', currency: 'BRL' },
    });
    expect(win.status).toBe(200);
    expect(win.body.balance).toEqual({ amount: '115.00', currency: 'BRL' });

    // Re-submitting the previously rejected BET must NOT reopen it or debit 115.00
    const replay = await submitWager(payload, key);
    expect(replay.status).toBe(422);
    expect(replay.body).toEqual({
      transactionId: rejected.body.transactionId,
      status: 'REJECTED',
      failureCode: 'INSUFFICIENT_FUNDS',
      balance: { amount: '15.00', currency: 'BRL' },
      idempotentReplay: true,
    });

    const afterWallet = await call('GET', `/wallets/${wallet.id}`);
    expect(afterWallet.body.balance).toEqual({ amount: '115.00', currency: 'BRL' });
    expect(afterWallet.body.version).toBe(2);
  });

  test('detects 409 IDEMPOTENCY_PAYLOAD_CONFLICT on altered payload or reused externalTransactionId', async () => {
    const wallet = await openWallet('100.00');
    const providerId = `prov-${newId()}`;
    const key = `${providerId}:bet-conflict`;
    const basePayload = {
      providerId,
      externalTransactionId: 'bet-conflict',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-30',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
    };

    const first = await submitWager(basePayload, key);
    expect(first.status).toBe(200);

    // Same Idempotency-Key, different amount
    const diffAmount = await submitWager({ ...basePayload, money: { amount: '12.00', currency: 'BRL' } }, key);
    expect(diffAmount.status).toBe(409);
    expect(diffAmount.body.code).toBe(FailureCode.IdempotencyPayloadConflict);

    // Same Idempotency-Key, pointing to a non-existent walletId -> still 409 conflict, not 404
    const diffMissingWallet = await submitWager({ ...basePayload, walletId: newId() }, key);
    expect(diffMissingWallet.status).toBe(409);
    expect(diffMissingWallet.body.code).toBe(FailureCode.IdempotencyPayloadConflict);

    // Same (providerId, externalTransactionId) submitted under a brand new Idempotency-Key
    const reusedExternalId = await submitWager(basePayload, `${providerId}:another-key`);
    expect(reusedExternalId.status).toBe(409);
    expect(reusedExternalId.body.code).toBe(FailureCode.IdempotencyPayloadConflict);
  });
});

describe('Wagering processor — PENDING_REFERENCE, single active reversal and rejection rules', () => {
  test('records PENDING_REFERENCE (202) for missing references, replays idempotently, and blocks duplicate reversals', async () => {
    const wallet = await openWallet('80.00');
    const providerId = `prov-${newId()}`;
    const refundPayload = {
      providerId,
      externalTransactionId: 'refund-early',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-40',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-not-arrived-yet',
    };

    const pending = await submitWager(refundPayload, `${providerId}:refund-early`);
    expect(pending.status).toBe(202);
    expect(pending.body).toEqual({
      transactionId: expect.any(String),
      status: 'PENDING_REFERENCE',
      idempotentReplay: false,
    });

    const pendingEvents = await outboxEventsForTransaction(pending.body.transactionId);
    expect(pendingEvents.map((e) => e.event_type)).toEqual(['WagerTransactionPendingReference']);

    const [row] = await db`
      select attempt_count, next_attempt_at
      from wager_transactions
      where id = ${pending.body.transactionId}
    `;
    expect(row!.attempt_count).toBe(1);
    expect(row!.next_attempt_at).not.toBeNull();

    // Idempotent replay of PENDING_REFERENCE does not bump attempt_count or duplicate outbox events
    const replay = await submitWager(refundPayload, `${providerId}:refund-early`);
    expect(replay.status).toBe(202);
    expect(replay.body).toEqual({
      transactionId: pending.body.transactionId,
      status: 'PENDING_REFERENCE',
      idempotentReplay: true,
    });
    const [rowAfter] = await db`select attempt_count from wager_transactions where id = ${pending.body.transactionId}`;
    expect(rowAfter!.attempt_count).toBe(1);

    // A second distinct reversal (ROLLBACK) for the same missing BET is rejected with REFERENCE_ALREADY_REVERSED
    const rivalRollback = await submitWager({
      providerId,
      externalTransactionId: 'rollback-rival',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-40',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-not-arrived-yet',
    });
    expect(rivalRollback.status).toBe(422);
    expect(rivalRollback.body.status).toBe('REJECTED');
    expect(rivalRollback.body.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);

    // WIN and LOSS with explicit missing referenceExternalTransactionId also park in PENDING_REFERENCE
    const winMissingRef = await submitWager({
      providerId,
      externalTransactionId: 'win-early',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-41',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '35.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-41-missing',
    });
    expect(winMissingRef.status).toBe(202);
    expect(winMissingRef.body.status).toBe('PENDING_REFERENCE');

    // Wallet remained untouched throughout all pending/rejected operations
    const walletCheck = await call('GET', `/wallets/${wallet.id}`);
    expect(walletCheck.body.balance).toEqual({ amount: '80.00', currency: 'BRL' });
    expect(walletCheck.body.version).toBe(1);
  });

  test('enforces single reversal, NEGATIVE_BALANCE_REVERSAL, and reference compatibility rules', async () => {
    const wallet = await openWallet('50.00');
    const otherWallet = await openWallet('50.00');
    const providerId = `prov-${newId()}`;

    // BET 20.00 -> 30.00
    await submitWager({
      providerId,
      externalTransactionId: 'bet-a',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-50',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '20.00', currency: 'BRL' },
    });

    // REFUND bet-a -> 50.00
    const refund1 = await submitWager({
      providerId,
      externalTransactionId: 'refund-a-1',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-50',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-a',
    });
    expect(refund1.status).toBe(200);

    // Second REFUND on bet-a -> 422 REFERENCE_ALREADY_REVERSED
    const refund2 = await submitWager({
      providerId,
      externalTransactionId: 'refund-a-2',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-50',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-a',
    });
    expect(refund2.status).toBe(422);
    expect(refund2.body.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);

    // ROLLBACK on already-refunded bet-a -> 422 REFERENCE_ALREADY_REVERSED
    const rollbackAfterRefund = await submitWager({
      providerId,
      externalTransactionId: 'rollback-a-after-refund',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-50',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-a',
    });
    expect(rollbackAfterRefund.status).toBe(422);
    expect(rollbackAfterRefund.body.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);

    // WIN 40.00 -> 90.00, then spend 70.00 via BET -> 20.00, then ROLLBACK of WIN 40.00 -> NEGATIVE_BALANCE_REVERSAL
    await submitWager({
      providerId,
      externalTransactionId: 'win-big',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-51',
      gameId: 'fortune-chimp',
      kind: 'WIN',
      money: { amount: '40.00', currency: 'BRL' },
    });
    await submitWager({
      providerId,
      externalTransactionId: 'bet-spend',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-52',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '70.00', currency: 'BRL' },
    });

    const negativeRollback = await submitWager({
      providerId,
      externalTransactionId: 'rollback-win-big',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-51',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '40.00', currency: 'BRL' },
      referenceExternalTransactionId: 'win-big',
    });
    expect(negativeRollback.status).toBe(422);
    expect(negativeRollback.body.failureCode).toBe(FailureCode.NegativeBalanceReversal);
    expect(negativeRollback.body.balance).toEqual({ amount: '20.00', currency: 'BRL' });

    // REFUND of WIN -> REFERENCE_KIND_NOT_ALLOWED
    const refundOfWin = await submitWager({
      providerId,
      externalTransactionId: 'refund-of-win',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-51',
      gameId: 'fortune-chimp',
      kind: 'REFUND',
      money: { amount: '40.00', currency: 'BRL' },
      referenceExternalTransactionId: 'win-big',
    });
    expect(refundOfWin.status).toBe(422);
    expect(refundOfWin.body.failureCode).toBe(FailureCode.ReferenceKindNotAllowed);

    // AMOUNT_MISMATCH on ROLLBACK of bet-spend (70.00 vs 65.00)
    const wrongAmount = await submitWager({
      providerId,
      externalTransactionId: 'rollback-wrong-amount',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-52',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '65.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-spend',
    });
    expect(wrongAmount.status).toBe(422);
    expect(wrongAmount.body.failureCode).toBe(FailureCode.AmountMismatch);

    // REFERENCE_SCOPE_MISMATCH (wrong roundId and cross-wallet attempt)
    const wrongRound = await submitWager({
      providerId,
      externalTransactionId: 'rollback-wrong-round',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'another-round',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '70.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-spend',
    });
    expect(wrongRound.status).toBe(422);
    expect(wrongRound.body.failureCode).toBe(FailureCode.ReferenceScopeMismatch);

    const wrongWallet = await submitWager({
      providerId,
      externalTransactionId: 'rollback-wrong-wallet',
      playerId: otherWallet.playerId,
      walletId: otherWallet.id,
      roundId: 'round-52',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '70.00', currency: 'BRL' },
      referenceExternalTransactionId: 'bet-spend',
    });
    expect(wrongWallet.status).toBe(422);
    expect(wrongWallet.body.failureCode).toBe(FailureCode.ReferenceScopeMismatch);

    // REFERENCE_NOT_PROCESSED (referencing the rejected refund-a-2)
    const refRejected = await submitWager({
      providerId,
      externalTransactionId: 'rollback-of-rejected',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-50',
      gameId: 'fortune-chimp',
      kind: 'ROLLBACK',
      money: { amount: '20.00', currency: 'BRL' },
      referenceExternalTransactionId: 'refund-a-2',
    });
    expect(refRejected.status).toBe(422);
    expect(refRejected.body.failureCode).toBe(FailureCode.ReferenceNotProcessed);
  });

  test('rejects pre-persistence scope and input errors cleanly (400 / 404 / 422)', async () => {
    const wallet = await openWallet('50.00');
    const providerId = `prov-${newId()}`;

    const missingHeader = await call('POST', '/wagering/transactions', {
      providerId,
      externalTransactionId: 'no-header',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'r1',
      gameId: 'g1',
      kind: 'BET',
      money: { amount: '5.00', currency: 'BRL' },
    });
    expect(missingHeader.status).toBe(400);
    expect(missingHeader.body.code).toBe(FailureCode.InvalidPayload);

    const openingAttempt = await submitWager({
      providerId,
      externalTransactionId: 'open-attempt',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'r1',
      gameId: 'g1',
      kind: 'OPENING',
      money: { amount: '5.00', currency: 'BRL' },
    });
    expect(openingAttempt.status).toBe(400);
    expect(openingAttempt.body.code).toBe(FailureCode.UnsupportedKind);

    const unknownWallet = await submitWager({
      providerId,
      externalTransactionId: 'unknown-wallet',
      playerId: wallet.playerId,
      walletId: newId(),
      roundId: 'r1',
      gameId: 'g1',
      kind: 'BET',
      money: { amount: '5.00', currency: 'BRL' },
    });
    expect(unknownWallet.status).toBe(404);
    expect(unknownWallet.body.code).toBe(FailureCode.WalletNotFound);

    const wrongPlayer = await submitWager({
      providerId,
      externalTransactionId: 'wrong-player',
      playerId: newId(),
      walletId: wallet.id,
      roundId: 'r1',
      gameId: 'g1',
      kind: 'BET',
      money: { amount: '5.00', currency: 'BRL' },
    });
    expect(wrongPlayer.status).toBe(422);
    expect(wrongPlayer.body.code).toBe(FailureCode.WalletPlayerMismatch);

    const wrongCurrency = await submitWager({
      providerId,
      externalTransactionId: 'wrong-currency',
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'r1',
      gameId: 'g1',
      kind: 'BET',
      money: { amount: '5.00', currency: 'USD' },
    });
    expect(wrongCurrency.status).toBe(422);
    expect(wrongCurrency.body.code).toBe(FailureCode.CurrencyMismatch);
  });
});

describe('Wagering processor — transactional composition (Inbox readiness) and query endpoints', () => {
  test('processInTransaction shares caller SQL transaction atomically (commit and rollback)', async () => {
    const wallet = await openWallet('100.00');
    const providerId = `prov-${newId()}`;

    // 1. Atomic commit alongside inbox_messages
    const messageIdCommit = `msg-${newId()}`;
    const parsedCommit = parseWagerRequest(
      {
        providerId,
        externalTransactionId: 'tx-inbox-commit',
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-60',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '30.00', currency: 'BRL' },
      },
      `${providerId}:tx-inbox-commit`,
    );

    const committed = await orm.em.fork().transactional(async (txEm) => {
      await txEm.insert(InboxMessageRecord, {
        consumerName: 'wager-transactions',
        messageId: messageIdCommit,
        payloadHash: canonicalHash({ messageIdCommit }),
        receivedAt: new Date(),
        processedAt: null,
      });
      const res = await processor.processInTransaction(txEm, {
        ...parsedCommit,
        correlationId: 'corr-inbox-1',
        causationId: messageIdCommit,
      });
      await txEm.nativeUpdate(
        InboxMessageRecord,
        { consumerName: 'wager-transactions', messageId: messageIdCommit },
        { processedAt: new Date() },
      );
      return res;
    });
    expect(committed.transaction.observedBalance?.toJSON()).toEqual({ amount: '70.00', currency: 'BRL' });

    // 2. Caller rollback undoes inbox_messages + wallet + wager_transactions + ledger + outbox together
    const messageIdRollback = `msg-${newId()}`;
    const parsedRollback = parseWagerRequest(
      {
        providerId,
        externalTransactionId: 'tx-inbox-rollback',
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-61',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '20.00', currency: 'BRL' },
      },
      `${providerId}:tx-inbox-rollback`,
    );

    let rolledBackTxId = '';
    await expect(
      orm.em.fork().transactional(async (txEm) => {
        await txEm.insert(InboxMessageRecord, {
          consumerName: 'wager-transactions',
          messageId: messageIdRollback,
          payloadHash: canonicalHash({ messageIdRollback }),
          receivedAt: new Date(),
          processedAt: null,
        });
        const res = await processor.processInTransaction(txEm, {
          ...parsedRollback,
          correlationId: 'corr-inbox-2',
        });
        rolledBackTxId = res.transaction.id;
        throw new Error('simulated post-processing failure before commit');
      }),
    ).rejects.toThrow('simulated post-processing failure');

    const walletAfterRollback = await call('GET', `/wallets/${wallet.id}`);
    expect(walletAfterRollback.body.balance).toEqual({ amount: '70.00', currency: 'BRL' });
    expect(walletAfterRollback.body.version).toBe(2);

    const notFound = await call('GET', `/wagering/transactions/${rolledBackTxId}`);
    expect(notFound.status).toBe(404);
    expect(notFound.body.code).toBe(FailureCode.TransactionNotFound);

    const rolledBackEvents = await outboxEventsForTransaction(rolledBackTxId);
    expect(rolledBackEvents).toHaveLength(0);
  });

  test('processInTransaction keeps outer transaction alive when handling payload conflict', async () => {
    const walletA = await openWallet('50.00');
    const walletB = await openWallet('50.00');
    const providerId = `prov-${newId()}`;
    const sharedKey = `${providerId}:shared-key`;

    await submitWager(
      {
        providerId,
        externalTransactionId: 'ext-on-a',
        playerId: walletA.playerId,
        walletId: walletA.id,
        roundId: 'r1',
        gameId: 'g1',
        kind: 'BET',
        money: { amount: '10.00', currency: 'BRL' },
      },
      sharedKey,
    );

    const messageId = `msg-${newId()}`;
    const conflictingOnB = parseWagerRequest(
      {
        providerId,
        externalTransactionId: 'ext-on-b',
        playerId: walletB.playerId,
        walletId: walletB.id,
        roundId: 'r1',
        gameId: 'g1',
        kind: 'BET',
        money: { amount: '10.00', currency: 'BRL' },
      },
      sharedKey,
    );

    await orm.em.fork().transactional(async (txEm) => {
      await txEm.insert(InboxMessageRecord, {
        consumerName: 'wager-transactions',
        messageId,
        payloadHash: canonicalHash({ messageId }),
        receivedAt: new Date(),
        processedAt: null,
      });
      try {
        await processor.processInTransaction(txEm, { ...conflictingOnB, correlationId: 'corr-conflict' });
        throw new Error('expected conflict');
      } catch (err) {
        expect(err).toBeInstanceOf(DomainError);
        expect((err as DomainError).code).toBe(FailureCode.IdempotencyPayloadConflict);
      }
      // Outer SQL transaction is still healthy and can mark the inbox message as processed (terminal business outcome)
      await txEm.nativeUpdate(
        InboxMessageRecord,
        { consumerName: 'wager-transactions', messageId },
        { processedAt: new Date() },
      );
    });

    const [inboxRow] = await db`
      select processed_at from inbox_messages
      where consumer_name = 'wager-transactions' and message_id = ${messageId}
    `;
    expect(inboxRow!.processed_at).not.toBeNull();
  });

  test('GET by internal transactionId and by provider externalTransactionId return consistent views', async () => {
    const wallet = await openWallet('60.00');
    const providerId = `prov-${newId()}`;
    const externalTransactionId = `ext-${newId()}`;

    const created = await submitWager({
      providerId,
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-70',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '15.00', currency: 'BRL' },
    });
    expect(created.status).toBe(200);

    const byId = await call('GET', `/wagering/transactions/${created.body.transactionId}`);
    const byExternal = await call(
      'GET',
      `/providers/${encodeURIComponent(providerId)}/wagering/transactions/${encodeURIComponent(externalTransactionId)}`,
    );

    expect(byId.status).toBe(200);
    expect(byExternal.status).toBe(200);
    expect(byExternal.body).toEqual(byId.body);
    expect(byId.body).toMatchObject({
      id: created.body.transactionId,
      providerId,
      externalTransactionId,
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: 'round-70',
      gameId: 'fortune-chimp',
      kind: 'BET',
      status: 'PROCESSED',
      money: { amount: '15.00', currency: 'BRL' },
      observedBalance: { amount: '45.00', currency: 'BRL' },
      referenceExternalTransactionId: null,
      referenceTransactionId: null,
      failureCode: null,
    });

    const missingExternal = await call('GET', `/providers/${providerId}/wagering/transactions/does-not-exist`);
    expect(missingExternal.status).toBe(404);
    expect(missingExternal.body.code).toBe(FailureCode.TransactionNotFound);
  });

  test('every wallet exercised across the suite reconciles cleanly', async () => {
    expect(createdWalletIds.length).toBeGreaterThan(0);
    for (const walletId of createdWalletIds) {
      const res = await call('POST', `/wallets/${walletId}/reconciliation`);
      expect(res.status).toBe(200);
      expect(res.body.consistent).toBe(true);
      expect(res.body.difference).toEqual({ amount: '0.00', currency: res.body.storedBalance.currency });
    }
  });
});
