import {
  type BatchResultErrorEntry,
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { IsolationLevel } from '@mikro-orm/core';
import { type EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Injectable, Logger, type OnApplicationBootstrap, type OnModuleDestroy, Optional } from '@nestjs/common';
import { AppConfig } from '../common/config/env';
import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { isTransientInfrastructureError, lockConflictSqlState, retryCause } from '../common/errors/transient-error';
import { CooperativeBudgetExhaustedError, ShutdownService } from '../common/http/shutdown.service';
import { definedFields, safeErrorFields } from '../common/observability/log-fields';
import { guardMetrics, MetricsRegistry, recordStatusTransition } from '../common/observability/metrics';
import { computeJitteredDelayMs } from '../common/instance/instance-identity';
import { InboxMessage } from '../common/messaging/inbox-message';
import { readDatabaseClock } from '../database/clock';
import { type ProcessWagerResult, WagerProcessorService } from '../wagering/wager-processor.service';
import { BatchVisibilityLeaseManager } from './sqs-visibility-lease';
import { deriveDlqRouting, type ParsedWagerMessage, parseWagerMessage, WAGER_CONSUMER_NAME } from './wager-message';

const DEFAULT_MAX_MESSAGES = 10;
const MAX_SQS_VISIBILITY_BATCH_SIZE = 10;
const DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 30;
const ERROR_BACKOFF_MS = 250;

/**
 * Business conflicts thrown before/without writing a new wager_transactions row whose outcome is
 * deterministic and permanent (retrying the message would fail identically forever).
 * Notably excludes WALLET_NOT_FOUND, which must roll back the inbox and retry until SQS redrive.
 */
const TERMINAL_BUSINESS_CONFLICT_CODES: ReadonlySet<FailureCode> = new Set([
  FailureCode.IdempotencyPayloadConflict,
  FailureCode.WalletPlayerMismatch,
  FailureCode.CurrencyMismatch,
]);

interface InboxRow {
  consumer_name: string;
  message_id: string;
  payload_hash: string;
  received_at: Date | string;
  processed_at: Date | string | null;
}

type TransactionalOutcome =
  | { readonly kind: 'processed'; readonly result: ProcessWagerResult }
  | { readonly kind: 'business_conflict'; readonly code: FailureCode }
  | { readonly kind: 'duplicate' }
  | { readonly kind: 'inbox_hash_mismatch' };

export type HandleMessageOutcome =
  | { readonly status: 'processed'; readonly messageId: string; readonly result: ProcessWagerResult }
  | { readonly status: 'business_conflict'; readonly messageId: string; readonly failureCode: FailureCode }
  | { readonly status: 'duplicate'; readonly messageId: string }
  | { readonly status: 'dead_lettered'; readonly reason: string; readonly failureCode: FailureCode }
  | { readonly status: 'retry_later'; readonly reason: string };

export interface ConsumerMessageOptions {
  /** Test/diagnostic hook invoked inside the SQL transaction immediately before COMMIT. */
  readonly beforeCommit?: (txEm: EntityManager, messageId: string) => Promise<void> | void;
}

export interface PollOnceOptions extends ConsumerMessageOptions {
  readonly maxNumberOfMessages?: number;
  readonly waitTimeSeconds?: number;
}

export interface PollBatchStats {
  readonly received: number;
  readonly processed: number;
  readonly duplicates: number;
  readonly conflicts: number;
  readonly deadLettered: number;
  readonly retried: number;
  readonly visibilityReleased: number;
}

@Injectable()
export class WagerTransactionsConsumer implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(WagerTransactionsConsumer.name);
  private running = false;
  private stopping = false;
  private loopPromise: Promise<void> | undefined;
  private receiveAbort: AbortController | undefined;
  private sleepWake: (() => void) | undefined;
  private activePollPromise: Promise<PollBatchStats> | undefined;
  private mainQueueUrl: string | undefined;
  private mainQueueVisibilityTimeoutSeconds: number | undefined;
  private dlqUrl: string | undefined;

  constructor(
    private readonly sqs: SQSClient,
    private readonly orm: MikroORM,
    private readonly processor: WagerProcessorService,
    private readonly config: AppConfig,
    @Optional() private readonly shutdown?: ShutdownService,
    @Optional() private readonly metrics?: MetricsRegistry,
  ) {}

  onApplicationBootstrap(): void {
    if (this.config.workersEnabled && this.shutdown?.isShuttingDown() !== true) {
      this.start();
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.shutdown !== undefined) {
      await this.shutdown.runWithinCooperativeBudget(WagerTransactionsConsumer.name, () => this.stop(), [
        'postgres',
        'sqs',
      ]);
      return;
    }
    await this.stop();
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.stopping = false;
    this.running = true;
    this.loopPromise = this.runLoop();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.running = false;
    this.receiveAbort?.abort();
    this.sleepWake?.();
    await this.loopPromise;
    this.loopPromise = undefined;
    await this.activePollPromise;
    this.stopping = false;
  }

  /** Resets cached queue URLs (useful if queues are recreated or reconfigured). */
  clearQueueUrlCache(): void {
    this.mainQueueUrl = undefined;
    this.mainQueueVisibilityTimeoutSeconds = undefined;
    this.dlqUrl = undefined;
  }

  async pollOnce(options: PollOnceOptions = {}): Promise<PollBatchStats> {
    if (this.activePollPromise !== undefined) {
      return this.activePollPromise;
    }
    const work = this.trackConsumerOperation('WagerTransactionsConsumer.pollOnce', undefined, () =>
      this.executePoll(options),
    );
    this.activePollPromise = work;
    try {
      return await work;
    } finally {
      if (this.activePollPromise === work) {
        this.activePollPromise = undefined;
      }
    }
  }

  /**
   * Processes a single SQS message from `wager-transactions.fifo`:
   * - Validates envelope & payload; forwards contract poison to DLQ before ACK.
   * - Maintains a visibility lease heartbeat while the SQL transaction is in flight.
   * - Runs Inbox + WagerProcessorService.processInTransaction in one READ COMMITTED transaction.
   * - Calls DeleteMessage strictly AFTER PostgreSQL commits.
   */
  async handleMessage(message: Message, options: ConsumerMessageOptions = {}): Promise<HandleMessageOutcome> {
    const { outcome } = await this.trackConsumerOperation(
      'WagerTransactionsConsumer.handleMessage',
      { sqsMessageId: message.MessageId },
      async () => {
        if (message.ReceiptHandle === undefined || this.stopping || this.shutdown?.isShuttingDown() === true) {
          return this.processReceivedMessage(message, options);
        }

        const receivedAtMono = performance.now();
        let queueUrl: string;
        try {
          queueUrl = await this.getMainQueueUrl();
        } catch (error) {
          if (error instanceof CooperativeBudgetExhaustedError) {
            return {
              outcome: { status: 'retry_later' as const, reason: error.message },
              acknowledged: false,
            };
          }
          throw error;
        }

        const visibilityTimeoutSeconds = await this.getMainQueueVisibilityTimeoutSeconds(queueUrl);
        const leaseManager = new BatchVisibilityLeaseManager({
          sqs: this.sqs,
          queueUrl,
          messages: [message],
          visibilityTimeoutSeconds,
          receivedAtMono,
          defaultCallTimeoutMs: this.config.sqsCallTimeoutMs,
          logger: this.logger,
          ...(this.shutdown !== undefined ? { shutdown: this.shutdown } : {}),
          isStopping: () => this.stopping || this.shutdown?.isShuttingDown() === true,
          isGroupBlocked: () => false,
        });
        if (!leaseManager.claimForExecution(0)) {
          return {
            outcome: {
              status: 'retry_later' as const,
              reason: 'SQS visibility timeout elapsed before message execution started',
            },
            acknowledged: false,
          };
        }
        leaseManager.start();

        try {
          return await this.processReceivedMessage(message, options, () => leaseManager.markCompleted(0), queueUrl);
        } finally {
          leaseManager.markCompleted(0);
          await leaseManager.stop();
        }
      },
    );
    return outcome;
  }

  private async processReceivedMessage(
    message: Message,
    options: ConsumerMessageOptions,
    onBeforeAck?: () => void,
    resolvedQueueUrl?: string,
  ): Promise<{ readonly outcome: HandleMessageOutcome; readonly acknowledged: boolean }> {
    let queueUrl: string;
    if (resolvedQueueUrl !== undefined) {
      queueUrl = resolvedQueueUrl;
    } else {
      try {
        queueUrl = await this.getMainQueueUrl();
      } catch (error) {
        if (error instanceof CooperativeBudgetExhaustedError) {
          return {
            outcome: { status: 'retry_later', reason: error.message },
            acknowledged: false,
          };
        }
        throw error;
      }
    }

    let parsed: ParsedWagerMessage;
    try {
      parsed = parseWagerMessage(message.Body, message.Attributes?.MessageGroupId);
    } catch (error) {
      const code = error instanceof DomainError ? error.code : FailureCode.InvalidPayload;
      const reason = error instanceof Error ? error.message : String(error);
      onBeforeAck?.();
      return this.forwardPoisonToDlqAndAck(queueUrl, message, code, reason);
    }

    // Validate Inbox domain invariants before checking out a DB connection.
    InboxMessage.receive({
      consumerName: WAGER_CONSUMER_NAME,
      messageId: parsed.messageId,
      payloadHash: parsed.inboxPayloadHash,
      receivedAt: new Date(),
    });

    let txOutcome: TransactionalOutcome;
    const started = performance.now();
    try {
      txOutcome = await this.orm.em.fork().transactional(
        (txEm) => this.executeInTransaction(txEm, parsed, options),
        { isolationLevel: IsolationLevel.READ_COMMITTED },
      );
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      guardMetrics(() => {
        this.metrics?.observe('wager_processing_duration_ms', performance.now() - started, {
          channel: 'sqs',
          outcome: 'error',
        });
        this.metrics?.increment('wager_retries_total', { channel: 'sqs', cause: retryCause(error) });
        const sqlstate = lockConflictSqlState(error);
        if (sqlstate !== undefined) {
          this.metrics?.increment('wager_lock_conflicts_total', { sqlstate });
        }
      });
      try {
        this.logger.warn(
          'Wager SQS message transaction rolled back; leaving message unacknowledged for retry',
          definedFields({
            correlationId: parsed.command.correlationId,
            messageId: parsed.messageId,
            walletId: parsed.command.walletId,
            providerId: parsed.command.providerId,
            transient: isTransientInfrastructureError(error),
            ...safeErrorFields(error),
          }),
        );
      } catch {
        // A log failure must not acknowledge the message or hide the rollback.
      }
      return { outcome: { status: 'retry_later', reason }, acknowledged: false };
    }

    guardMetrics(() => {
      this.metrics?.observe('wager_processing_duration_ms', performance.now() - started, {
        channel: 'sqs',
        outcome: 'completed',
      });
      this.recordCommittedSqsOutcome(txOutcome);
    });

    onBeforeAck?.();

    if (txOutcome.kind === 'inbox_hash_mismatch') {
      return this.forwardPoisonToDlqAndAck(
        queueUrl,
        message,
        FailureCode.IdempotencyPayloadConflict,
        `messageId "${parsed.messageId}" was already processed with a different payloadHash`,
      );
    }

    // PostgreSQL transaction is now durably committed; safe to ACK SQS.
    const acknowledged = await this.acknowledgeAfterCommit(queueUrl, message.ReceiptHandle, parsed.messageId);

    if (txOutcome.kind === 'duplicate') {
      return { outcome: { status: 'duplicate', messageId: parsed.messageId }, acknowledged };
    }
    if (txOutcome.kind === 'business_conflict') {
      return {
        outcome: { status: 'business_conflict', messageId: parsed.messageId, failureCode: txOutcome.code },
        acknowledged,
      };
    }
    return {
      outcome: { status: 'processed', messageId: parsed.messageId, result: txOutcome.result },
      acknowledged,
    };
  }

  private async executeInTransaction(
    txEm: EntityManager,
    parsed: ParsedWagerMessage,
    options: ConsumerMessageOptions,
  ): Promise<TransactionalOutcome> {
    const dbNow = await readDatabaseClock(txEm);

    // 1. Global lock hierarchy step 1: Inbox row lock (consumer_name, message_id).
    await txEm.execute(
      `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       values (?, ?, ?, ?, null)
       on conflict (consumer_name, message_id) do nothing`,
      [WAGER_CONSUMER_NAME, parsed.messageId, parsed.inboxPayloadHash, dbNow],
    );

    const rows = await txEm.execute<InboxRow[]>(
      `select consumer_name, message_id, payload_hash, received_at, processed_at
       from inbox_messages
       where consumer_name = ? and message_id = ?
       for update`,
      [WAGER_CONSUMER_NAME, parsed.messageId],
    );

    const row = rows[0];
    if (row === undefined) {
      throw new Error(`Inbox row ${WAGER_CONSUMER_NAME}/${parsed.messageId} missing after upsert`);
    }

    const inbox = InboxMessage.rehydrate({
      consumerName: row.consumer_name,
      messageId: row.message_id,
      payloadHash: row.payload_hash,
      receivedAt: toDate(row.received_at),
      processedAt: row.processed_at === null ? null : toDate(row.processed_at),
    });

    if (inbox.payloadHash !== parsed.inboxPayloadHash) {
      return { kind: 'inbox_hash_mismatch' };
    }

    if (inbox.isProcessed()) {
      return { kind: 'duplicate' };
    }

    // 2. Global lock hierarchy steps 2..5: Wallet -> WagerTransaction -> Ledger -> Outbox.
    let processResult: ProcessWagerResult | undefined;
    let conflictCode: FailureCode | undefined;
    try {
      processResult = await this.processor.processInTransaction(txEm, parsed.command, { now: dbNow });
    } catch (error) {
      if (error instanceof DomainError && TERMINAL_BUSINESS_CONFLICT_CODES.has(error.code)) {
        conflictCode = error.code;
      } else {
        throw error;
      }
    }

    // 3. Mark inbox processed in the exact same SQL transaction.
    const commitStamp = await readDatabaseClock(txEm);
    const processedAt = new Date(Math.max(commitStamp.getTime(), inbox.receivedAt.getTime()));
    inbox.markProcessed(processedAt);

    await txEm.execute(
      `update inbox_messages
       set processed_at = ?
       where consumer_name = ? and message_id = ?`,
      [processedAt, WAGER_CONSUMER_NAME, parsed.messageId],
    );

    if (options.beforeCommit !== undefined) {
      await options.beforeCommit(txEm, parsed.messageId);
    }

    if (conflictCode !== undefined) {
      return { kind: 'business_conflict', code: conflictCode };
    }
    if (processResult === undefined) {
      throw new Error('Unreachable: missing processResult without conflictCode');
    }
    return { kind: 'processed', result: processResult };
  }

  private async executePoll(options: PollOnceOptions): Promise<PollBatchStats> {
    if (this.stopping || this.shutdown?.isShuttingDown() === true) {
      return emptyPollStats();
    }

    let queueUrl: string;
    try {
      queueUrl = await this.getMainQueueUrl();
    } catch (error) {
      if (
        this.stopping ||
        this.shutdown?.isShuttingDown() === true ||
        error instanceof CooperativeBudgetExhaustedError
      ) {
        return emptyPollStats();
      }
      throw error;
    }

    if (this.stopping || this.shutdown?.isShuttingDown() === true) {
      return emptyPollStats();
    }

    const maxNumberOfMessages = options.maxNumberOfMessages ?? DEFAULT_MAX_MESSAGES;
    const waitTimeSeconds = options.waitTimeSeconds ?? this.config.sqsWaitTimeSeconds;
    const receiveTimeoutMs = waitTimeSeconds * 1_000 + this.config.sqsCallTimeoutMs;

    const stopController = new AbortController();
    const timeoutSignal = AbortSignal.timeout(receiveTimeoutMs);
    const signals: AbortSignal[] = [stopController.signal, timeoutSignal];
    if (this.shutdown !== undefined) {
      signals.push(this.shutdown.shutdownSignal);
    }
    const combinedSignal = AbortSignal.any(signals);
    this.receiveAbort = stopController;

    let messages: Message[];
    let receivedAtMono = performance.now();
    try {
      const response = await this.sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: maxNumberOfMessages,
          WaitTimeSeconds: waitTimeSeconds,
          AttributeNames: ['All'],
          MessageSystemAttributeNames: ['All'],
          MessageAttributeNames: ['All'],
        }),
        { abortSignal: combinedSignal },
      );
      receivedAtMono = performance.now();
      messages = response.Messages ?? [];
    } catch (error) {
      if (stopController.signal.aborted || this.stopping || this.shutdown?.isShuttingDown() === true) {
        return emptyPollStats();
      }
      throw error;
    } finally {
      if (this.receiveAbort === stopController) {
        this.receiveAbort = undefined;
      }
    }

    if (messages.length === 0) {
      return emptyPollStats();
    }

    const visibilityTimeoutSeconds = await this.getMainQueueVisibilityTimeoutSeconds(queueUrl);

    let processed = 0;
    let duplicates = 0;
    let conflicts = 0;
    let deadLettered = 0;
    let retried = 0;
    let visibilityReleased = 0;
    // Groups whose uncommitted head failed in this batch: later messages in the same group must not run.
    const blockedGroups = new Set<string>();
    // Groups with an earlier message still in-flight in SQS (failed OR DeleteMessage failed):
    // later messages of that group must NOT have VisibilityTimeout reset to 0 ahead of their predecessor.
    const inFlightPredecessorGroups = new Set<string>();

    const leaseManager = new BatchVisibilityLeaseManager({
      sqs: this.sqs,
      queueUrl,
      messages,
      visibilityTimeoutSeconds,
      receivedAtMono,
      defaultCallTimeoutMs: this.config.sqsCallTimeoutMs,
      logger: this.logger,
      ...(this.shutdown !== undefined ? { shutdown: this.shutdown } : {}),
      isStopping: () => this.stopping || this.shutdown?.isShuttingDown() === true,
      isGroupBlocked: (groupId) => blockedGroups.has(groupId) || inFlightPredecessorGroups.has(groupId),
    });
    leaseManager.start();

    try {
      let inFlightMessageOnShutdown = 0;
      for (let index = 0; index < messages.length; index += 1) {
        const message = messages[index];
        if (message === undefined) {
          continue;
        }

        // On SIGTERM/stop, halt lease renewals first, then batch-release only unblocked, still-owned groups.
        if (this.stopping || this.shutdown?.isShuttingDown() === true) {
          leaseManager.markReleasedFrom(index);
          await leaseManager.stop();
          const withheldGroups = new Set<string>([
            ...inFlightPredecessorGroups,
            ...leaseManager.lostGroupIds(),
          ]);
          const releaseOutcome = await this.releaseRemainingVisibility(
            queueUrl,
            messages.slice(index),
            withheldGroups,
          );
          visibilityReleased += releaseOutcome.released;
          retried += releaseOutcome.withheld;
          this.logger.log({
            message: 'SQS consumer shutdown drain summary',
            batchSize: messages.length,
            inFlightMessagesAtSignal: inFlightMessageOnShutdown,
            processedInBatch: processed + duplicates + conflicts + deadLettered,
            visibilityReleased,
            withheldBehindPredecessor: retried,
            leaseStates: leaseManager.leaseStateCounts(),
          });
          break;
        }

        const groupId = message.Attributes?.MessageGroupId;
        if (groupId !== undefined && blockedGroups.has(groupId)) {
          // Preserve strict per-wallet FIFO order when an earlier message in the same group failed.
          retried += 1;
          continue;
        }

        if (!leaseManager.claimForExecution(index)) {
          // Another process may already own this group because SQS visibility expired before execution started.
          retried += 1;
          if (groupId !== undefined) {
            blockedGroups.add(groupId);
            inFlightPredecessorGroups.add(groupId);
          }
          continue;
        }

        const stoppingBeforeMessage = this.stopping || this.shutdown?.isShuttingDown() === true;
        let outcome: HandleMessageOutcome;
        let acknowledged: boolean;
        try {
          ({ outcome, acknowledged } = await this.processReceivedMessage(
            message,
            options,
            () => leaseManager.markCompleted(index),
            queueUrl,
          ));
        } finally {
          if (!stoppingBeforeMessage && (this.stopping || this.shutdown?.isShuttingDown() === true)) {
            inFlightMessageOnShutdown += 1;
          }
          leaseManager.markCompleted(index);
        }

        if (!acknowledged && groupId !== undefined) {
          inFlightPredecessorGroups.add(groupId);
        }

        switch (outcome.status) {
          case 'processed':
            processed += 1;
            break;
          case 'duplicate':
            duplicates += 1;
            break;
          case 'business_conflict':
            conflicts += 1;
            break;
          case 'dead_lettered':
            deadLettered += 1;
            break;
          case 'retry_later':
            retried += 1;
            if (groupId !== undefined) {
              blockedGroups.add(groupId);
              inFlightPredecessorGroups.add(groupId);
            }
            break;
        }

        if (index === messages.length - 1 && (this.stopping || this.shutdown?.isShuttingDown() === true)) {
          this.logger.log({
            message: 'SQS consumer shutdown drain summary',
            batchSize: messages.length,
            inFlightMessagesAtSignal: inFlightMessageOnShutdown,
            processedInBatch: processed + duplicates + conflicts + deadLettered,
            visibilityReleased,
            withheldBehindPredecessor: retried,
            leaseStates: leaseManager.leaseStateCounts(),
          });
        }
      }
    } finally {
      await leaseManager.stop();
    }

    return {
      received: messages.length,
      processed,
      duplicates,
      conflicts,
      deadLettered,
      retried,
      visibilityReleased,
    };
  }

  /**
   * Sends contract-invalid messages to `wager-transactions-dlq.fifo` and deletes from the main
   * queue ONLY after SQS confirms the DLQ write (`200 OK`). If the DLQ write fails, leaves the
   * message unacknowledged so native SQS RedrivePolicy remains the safety net.
   */
  private async forwardPoisonToDlqAndAck(
    mainQueueUrl: string,
    message: Message,
    failureCode: FailureCode,
    reason: string,
  ): Promise<{ readonly outcome: HandleMessageOutcome; readonly acknowledged: boolean }> {
    try {
      const dlqUrl = await this.getDlqUrl();
      const { messageGroupId, messageDeduplicationId } = deriveDlqRouting(message);
      const budget = this.acquireSqsBudget('WagerTransactionsConsumer.forwardPoisonToDlq');
      if (budget.exhausted) {
        throw new CooperativeBudgetExhaustedError('WagerTransactionsConsumer.forwardPoisonToDlq');
      }

      try {
        await this.sqs.send(
          new SendMessageCommand({
            QueueUrl: dlqUrl,
            MessageBody: message.Body ?? '',
            MessageGroupId: messageGroupId,
            MessageDeduplicationId: messageDeduplicationId,
            MessageAttributes: {
              failureCode: { DataType: 'String', StringValue: failureCode },
              failureReason: { DataType: 'String', StringValue: reason.slice(0, 256) },
              ...(message.MessageId !== undefined
                ? { sourceSqsMessageId: { DataType: 'String', StringValue: message.MessageId } }
                : {}),
            },
          }),
          { abortSignal: budget.signal },
        );
      } finally {
        budget.dispose();
      }
    } catch (error) {
      const dlqReason = error instanceof Error ? error.message : String(error);
      try {
        this.logger.error('Failed to forward poison message to DLQ; leaving unacknowledged for native redrive', {
          sqsMessageId: message.MessageId,
          failureCode,
          ...safeErrorFields(error),
        });
      } catch {
        // Logging must not turn a failed DLQ send into an acknowledgement.
      }
      return { outcome: { status: 'retry_later', reason: dlqReason }, acknowledged: false };
    }

    const receiptHandle = message.ReceiptHandle;
    const acknowledged = await this.acknowledgeAfterCommit(
      mainQueueUrl,
      receiptHandle,
      message.MessageId ?? 'poison',
    );
    // A missing receipt handle returns acknowledged without executing DeleteMessage.
    // That is not a completed forward for the metric or for the ack log.
    if (receiptHandle !== undefined && acknowledged) {
      guardMetrics(() => {
        this.metrics?.increment('wager_dlq_forwards_completed_total');
      });
      try {
        this.logger.warn('Poison message forwarded to DLQ and acknowledged', {
          sqsMessageId: message.MessageId,
          failureCode,
          reason,
        });
      } catch {
        // The DLQ write and source delete already finished.
      }
    }
    return { outcome: { status: 'dead_lettered', reason, failureCode }, acknowledged };
  }

  private recordCommittedSqsOutcome(txOutcome: TransactionalOutcome): void {
    if (txOutcome.kind === 'duplicate') {
      this.metrics?.increment('wager_duplicates_total', { cause: 'inbox' });
      return;
    }
    if (txOutcome.kind === 'inbox_hash_mismatch') {
      this.metrics?.increment('wager_hash_conflicts_total', { cause: 'inbox_payload' });
      return;
    }
    if (txOutcome.kind === 'business_conflict') {
      if (txOutcome.code === FailureCode.IdempotencyPayloadConflict) {
        this.metrics?.increment('wager_hash_conflicts_total', { cause: 'idempotency' });
      }
      return;
    }
    if (txOutcome.result.idempotentReplay) {
      this.metrics?.increment('wager_duplicates_total', { cause: 'idempotency_replay' });
      return;
    }
    recordStatusTransition(this.metrics, txOutcome.result.transaction.status);
  }

  private async acknowledgeAfterCommit(
    queueUrl: string,
    receiptHandle: string | undefined,
    messageId: string,
  ): Promise<boolean> {
    if (receiptHandle === undefined) {
      return true;
    }
    const budget = this.acquireSqsBudget('WagerTransactionsConsumer.acknowledgeAfterCommit');
    if (budget.exhausted) {
      this.logger.warn({
        message: 'Skipping SQS DeleteMessage after commit because cooperative shutdown budget is exhausted; redelivery will be deduplicated by inbox',
        messageId,
      });
      return false;
    }
    try {
      await this.sqs.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receiptHandle,
        }),
        { abortSignal: budget.signal },
      );
      return true;
    } catch (error) {
      // Transaction is already committed; redelivery will hit inbox.isProcessed() and ACK cleanly.
      try {
        this.logger.warn('DeleteMessage failed after commit; redelivery will be deduplicated by inbox', {
          messageId,
          ...safeErrorFields(error),
        });
      } catch {
        // A log failure must still leave the message unacknowledged.
      }
      return false;
    } finally {
      budget.dispose();
    }
  }

  /**
   * Releases visibility (`VisibilityTimeout: 0`) of unstarted messages in FIFO waves:
   * - Groups remaining messages by `MessageGroupId` preserving arrival order.
   * - Sends at most ONE message (the current head) per `MessageGroupId` per wave, chunked at
   *   `<= 10` entries per `ChangeMessageVisibilityBatchCommand`, so two messages of the same
   *   group can never be in the same SQS batch where a partial failure could invert FIFO order.
   * - Shares a single non-resetting timeout budget bounded by `sqsCallTimeoutMs` and
   *   `shutdownTimeoutMs` across all waves.
   */
  private async releaseRemainingVisibility(
    queueUrl: string,
    remaining: readonly Message[],
    inFlightPredecessorGroups: ReadonlySet<string>,
  ): Promise<{ readonly released: number; readonly withheld: number }> {
    interface GroupReleaseQueue {
      readonly groupId: string | undefined;
      readonly messages: Message[];
    }

    const blockedGroups = new Set<string>(inFlightPredecessorGroups);
    const queues: GroupReleaseQueue[] = [];
    const queuesByGroup = new Map<string, GroupReleaseQueue>();
    let withheld = 0;

    for (const message of remaining) {
      const groupId = message.Attributes?.MessageGroupId;
      if (groupId !== undefined && blockedGroups.has(groupId)) {
        withheld += 1;
        this.logger.warn({
          message:
            'Withholding SQS visibility release during shutdown to preserve FIFO order behind in-flight predecessor',
          sqsMessageId: message.MessageId,
          messageGroupId: groupId,
        });
        continue;
      }

      if (groupId !== undefined) {
        let groupQueue = queuesByGroup.get(groupId);
        if (groupQueue === undefined) {
          groupQueue = { groupId, messages: [] };
          queuesByGroup.set(groupId, groupQueue);
          queues.push(groupQueue);
        }
        groupQueue.messages.push(message);
      } else {
        queues.push({ groupId: undefined, messages: [message] });
      }
    }

    const drainRemainingQueuesAsWithheld = (reason: string): void => {
      let drained = 0;
      for (const queue of queues) {
        if (queue.messages.length === 0) {
          continue;
        }
        if (queue.groupId !== undefined) {
          blockedGroups.add(queue.groupId);
        }
        drained += queue.messages.length;
        queue.messages.length = 0;
      }
      if (drained > 0) {
        withheld += drained;
        this.logger.warn({
          message: 'Shutdown visibility release budget exhausted; withholding unconfirmed SQS messages',
          withheldCount: drained,
          reason,
        });
      }
    };

    const standaloneBudgetMs = Math.min(
      this.config.sqsCallTimeoutMs,
      Math.max(100, Math.floor(this.config.shutdownTimeoutMs * 0.8)),
    );
    const releaseBudgetMs =
      this.shutdown?.isShuttingDown() === true
        ? Math.min(standaloneBudgetMs, this.shutdown.remainingCooperativeMs())
        : standaloneBudgetMs;

    if (releaseBudgetMs <= 0) {
      drainRemainingQueuesAsWithheld('Cooperative shutdown budget exhausted before SQS visibility release');
      return { released: 0, withheld };
    }

    let budgetSignal: AbortSignal | undefined;
    let released = 0;

    for (;;) {
      if (budgetSignal?.aborted) {
        const abortReason =
          budgetSignal.reason instanceof Error ? budgetSignal.reason.message : 'Shutdown visibility budget elapsed';
        drainRemainingQueuesAsWithheld(abortReason);
        break;
      }

      const waveHeads: Array<{ readonly queue: GroupReleaseQueue; readonly message: Message }> = [];
      for (const queue of queues) {
        const head = queue.messages[0];
        if (head === undefined) {
          continue;
        }
        if (queue.groupId !== undefined && blockedGroups.has(queue.groupId)) {
          withheld += queue.messages.length;
          queue.messages.length = 0;
          continue;
        }
        if (head.ReceiptHandle === undefined) {
          if (queue.groupId !== undefined) {
            blockedGroups.add(queue.groupId);
          }
          const blockedInGroup = queue.messages.length;
          withheld += blockedInGroup;
          queue.messages.length = 0;
          this.logger.warn({
            message:
              'Withholding SQS visibility release because head ReceiptHandle is missing; blocking group tail',
            sqsMessageId: head.MessageId,
            messageGroupId: queue.groupId,
            withheldInGroup: blockedInGroup,
          });
          continue;
        }
        waveHeads.push({ queue, message: head });
      }

      if (waveHeads.length === 0) {
        break;
      }

      budgetSignal ??= AbortSignal.timeout(releaseBudgetMs);

      for (let offset = 0; offset < waveHeads.length; offset += MAX_SQS_VISIBILITY_BATCH_SIZE) {
        if (budgetSignal.aborted) {
          break;
        }

        const chunk = waveHeads.slice(offset, offset + MAX_SQS_VISIBILITY_BATCH_SIZE);
        try {
          const response = await this.sqs.send(
            new ChangeMessageVisibilityBatchCommand({
              QueueUrl: queueUrl,
              Entries: chunk.map((item, idx) => ({
                Id: String(idx),
                ReceiptHandle: item.message.ReceiptHandle!,
                VisibilityTimeout: 0,
              })),
            }),
            { abortSignal: budgetSignal },
          );

          const succeededIndices = new Set<number>();
          for (const entry of response.Successful ?? []) {
            const parsedIdx = entry.Id !== undefined ? Number(entry.Id) : Number.NaN;
            if (Number.isInteger(parsedIdx) && parsedIdx >= 0 && parsedIdx < chunk.length) {
              succeededIndices.add(parsedIdx);
            }
          }

          const failuresByIndex = new Map<number, BatchResultErrorEntry>();
          for (const failure of response.Failed ?? []) {
            const parsedIdx = failure.Id !== undefined ? Number(failure.Id) : Number.NaN;
            if (Number.isInteger(parsedIdx) && parsedIdx >= 0 && parsedIdx < chunk.length) {
              failuresByIndex.set(parsedIdx, failure);
            }
          }

          for (let idx = 0; idx < chunk.length; idx += 1) {
            const item = chunk[idx]!;
            if (succeededIndices.has(idx) && !failuresByIndex.has(idx)) {
              item.queue.messages.shift();
              released += 1;
              continue;
            }

            const failure = failuresByIndex.get(idx);
            if (item.queue.groupId !== undefined) {
              blockedGroups.add(item.queue.groupId);
            }
            const withheldInGroup = item.queue.messages.length;
            withheld += withheldInGroup;
            item.queue.messages.length = 0;
            this.logger.warn({
              message: 'Failed to release SQS message visibility in shutdown wave; blocking group tail',
              sqsMessageId: item.message.MessageId,
              messageGroupId: item.queue.groupId,
              withheldInGroup,
              code: failure?.Code ?? 'AmbiguousBatchResponse',
              senderFault: failure?.SenderFault,
              reason: failure?.Message ?? 'Entry was not confirmed in ChangeMessageVisibilityBatch Successful set',
            });
          }
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error);
          let withheldInChunk = 0;
          for (const item of chunk) {
            if (item.queue.groupId !== undefined) {
              blockedGroups.add(item.queue.groupId);
            }
            withheldInChunk += item.queue.messages.length;
            item.queue.messages.length = 0;
          }
          withheld += withheldInChunk;
          this.logger.warn({
            message: 'Failed to execute SQS ChangeMessageVisibilityBatch during shutdown; blocking chunk groups',
            batchSize: chunk.length,
            withheldCount: withheldInChunk,
            timedOut: budgetSignal.aborted,
            reason,
          });
          if (budgetSignal.aborted) {
            drainRemainingQueuesAsWithheld(reason);
            return { released, withheld };
          }
        }
      }
    }

    return { released, withheld };
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.pollOnce();
      } catch (error) {
        this.mainQueueUrl = undefined;
        this.mainQueueVisibilityTimeoutSeconds = undefined;
        this.dlqUrl = undefined;
        this.logger.error({
          message: 'SQS consumer poll cycle failed',
          reason: error instanceof Error ? error.message : String(error),
        });
        if (this.running) {
          await this.sleep(computeJitteredDelayMs(ERROR_BACKOFF_MS));
        }
      }
    }
  }

  private async getMainQueueUrl(): Promise<string> {
    if (this.mainQueueUrl === undefined) {
      const budget = this.acquireSqsBudget('WagerTransactionsConsumer.getMainQueueUrl');
      if (budget.exhausted) {
        throw new CooperativeBudgetExhaustedError('WagerTransactionsConsumer.getMainQueueUrl');
      }
      let response;
      try {
        response = await this.sqs.send(
          new GetQueueUrlCommand({ QueueName: this.config.wagerTransactionsQueue }),
          { abortSignal: budget.signal },
        );
      } finally {
        budget.dispose();
      }
      if (!response.QueueUrl) {
        throw new Error(`Queue URL missing for ${this.config.wagerTransactionsQueue}`);
      }
      this.mainQueueUrl = response.QueueUrl;
    }
    return this.mainQueueUrl;
  }

  private async getMainQueueVisibilityTimeoutSeconds(queueUrl: string): Promise<number> {
    if (this.mainQueueVisibilityTimeoutSeconds !== undefined) {
      return this.mainQueueVisibilityTimeoutSeconds;
    }
    if (this.stopping || this.shutdown?.isShuttingDown() === true) {
      return DEFAULT_VISIBILITY_TIMEOUT_SECONDS;
    }
    const budget = this.acquireSqsBudget('WagerTransactionsConsumer.getMainQueueVisibilityTimeout');
    if (budget.exhausted) {
      return DEFAULT_VISIBILITY_TIMEOUT_SECONDS;
    }
    try {
      const response = await this.sqs.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: ['VisibilityTimeout'],
        }),
        { abortSignal: budget.signal },
      );
      const raw = response.Attributes?.VisibilityTimeout;
      if (raw !== undefined && /^\d+$/.test(raw)) {
        this.mainQueueVisibilityTimeoutSeconds = Number(raw);
        return this.mainQueueVisibilityTimeoutSeconds;
      }
    } catch (error) {
      this.logger.warn({
        message: 'Failed to read SQS VisibilityTimeout attribute; using default visibility lease',
        defaultSeconds: DEFAULT_VISIBILITY_TIMEOUT_SECONDS,
        reason: error instanceof Error ? error.message : String(error),
      });
    } finally {
      budget.dispose();
    }
    this.mainQueueVisibilityTimeoutSeconds = DEFAULT_VISIBILITY_TIMEOUT_SECONDS;
    return this.mainQueueVisibilityTimeoutSeconds;
  }

  private async getDlqUrl(): Promise<string> {
    if (this.dlqUrl === undefined) {
      const budget = this.acquireSqsBudget('WagerTransactionsConsumer.getDlqUrl');
      if (budget.exhausted) {
        throw new CooperativeBudgetExhaustedError('WagerTransactionsConsumer.getDlqUrl');
      }
      let response;
      try {
        response = await this.sqs.send(
          new GetQueueUrlCommand({ QueueName: this.config.wagerTransactionsDlq }),
          { abortSignal: budget.signal },
        );
      } finally {
        budget.dispose();
      }
      if (!response.QueueUrl) {
        throw new Error(`Queue URL missing for ${this.config.wagerTransactionsDlq}`);
      }
      this.dlqUrl = response.QueueUrl;
    }
    return this.dlqUrl;
  }

  private acquireSqsBudget(operation: string) {
    if (this.shutdown !== undefined) {
      return this.shutdown.acquireSqsCallBudget(this.config.sqsCallTimeoutMs, operation);
    }
    return {
      exhausted: false as const,
      budgetMs: this.config.sqsCallTimeoutMs,
      signal: AbortSignal.timeout(this.config.sqsCallTimeoutMs),
      dispose: () => undefined,
    };
  }

  private trackConsumerOperation<T>(
    name: string,
    context: Readonly<Record<string, string | undefined>> | undefined,
    work: () => Promise<T>,
  ): Promise<T> {
    if (this.shutdown !== undefined) {
      return this.shutdown.trackCriticalOperation(
        {
          kind: 'worker',
          name,
          resources: ['postgres', 'sqs'],
          context,
        },
        work,
      );
    }
    return work();
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.sleepWake = undefined;
        resolve();
      }, ms);
      this.sleepWake = () => {
        clearTimeout(timer);
        this.sleepWake = undefined;
        resolve();
      };
    });
  }
}

function toDate(value: Date | string): Date {
  return value instanceof Date ? value : new Date(value);
}

function emptyPollStats(): PollBatchStats {
  return {
    received: 0,
    processed: 0,
    duplicates: 0,
    conflicts: 0,
    deadLettered: 0,
    retried: 0,
    visibilityReleased: 0,
  };
}
