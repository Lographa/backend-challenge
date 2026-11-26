import {
  type BatchResultErrorEntry,
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageCommand,
  GetQueueUrlCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { IsolationLevel } from '@mikro-orm/core';
import { type EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { Injectable, Logger, type OnApplicationBootstrap, type OnModuleDestroy } from '@nestjs/common';
import { AppConfig } from '../common/config/env';
import { DomainError } from '../common/errors/domain.error';
import { FailureCode } from '../common/errors/failure-code';
import { isTransientInfrastructureError } from '../common/errors/transient-error';
import { InboxMessage } from '../common/messaging/inbox-message';
import { type ProcessWagerResult, WagerProcessorService } from '../wagering/wager-processor.service';
import { deriveDlqRouting, type ParsedWagerMessage, parseWagerMessage, WAGER_CONSUMER_NAME } from './wager-message';

const DEFAULT_MAX_MESSAGES = 10;
const MAX_SQS_VISIBILITY_BATCH_SIZE = 10;
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
  private dlqUrl: string | undefined;

  constructor(
    private readonly sqs: SQSClient,
    private readonly orm: MikroORM,
    private readonly processor: WagerProcessorService,
    private readonly config: AppConfig,
  ) {}

  onApplicationBootstrap(): void {
    if (this.config.workersEnabled) {
      this.start();
    }
  }

  async onModuleDestroy(): Promise<void> {
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
    this.dlqUrl = undefined;
  }

  async pollOnce(options: PollOnceOptions = {}): Promise<PollBatchStats> {
    if (this.activePollPromise !== undefined) {
      return this.activePollPromise;
    }
    const work = this.executePoll(options);
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
   * - Runs Inbox + WagerProcessorService.processInTransaction in one READ COMMITTED transaction.
   * - Calls DeleteMessage strictly AFTER PostgreSQL commits.
   */
  async handleMessage(message: Message, options: ConsumerMessageOptions = {}): Promise<HandleMessageOutcome> {
    const { outcome } = await this.processReceivedMessage(message, options);
    return outcome;
  }

  private async processReceivedMessage(
    message: Message,
    options: ConsumerMessageOptions,
  ): Promise<{ readonly outcome: HandleMessageOutcome; readonly acknowledged: boolean }> {
    const queueUrl = await this.getMainQueueUrl();

    let parsed: ParsedWagerMessage;
    try {
      parsed = parseWagerMessage(message.Body, message.Attributes?.MessageGroupId);
    } catch (error) {
      const code = error instanceof DomainError ? error.code : FailureCode.InvalidPayload;
      const reason = error instanceof Error ? error.message : String(error);
      return this.forwardPoisonToDlqAndAck(queueUrl, message, code, reason);
    }

    const receivedAt = new Date();
    // Validate Inbox domain invariants before checking out a DB connection.
    InboxMessage.receive({
      consumerName: WAGER_CONSUMER_NAME,
      messageId: parsed.messageId,
      payloadHash: parsed.inboxPayloadHash,
      receivedAt,
    });

    let txOutcome: TransactionalOutcome;
    try {
      txOutcome = await this.orm.em.fork().transactional(
        (txEm) => this.executeInTransaction(txEm, parsed, receivedAt, options),
        { isolationLevel: IsolationLevel.READ_COMMITTED },
      );
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.logger.warn({
        message: 'Wager SQS message transaction rolled back; leaving message unacknowledged for retry',
        messageId: parsed.messageId,
        walletId: parsed.command.walletId,
        transient: isTransientInfrastructureError(error),
        code: error instanceof DomainError ? error.code : undefined,
        reason,
      });
      return { outcome: { status: 'retry_later', reason }, acknowledged: false };
    }

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
    receivedAt: Date,
    options: ConsumerMessageOptions,
  ): Promise<TransactionalOutcome> {
    // 1. Global lock hierarchy step 1: Inbox row lock (consumer_name, message_id).
    await txEm.execute(
      `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       values (?, ?, ?, ?, null)
       on conflict (consumer_name, message_id) do nothing`,
      [WAGER_CONSUMER_NAME, parsed.messageId, parsed.inboxPayloadHash, receivedAt],
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
      processResult = await this.processor.processInTransaction(txEm, parsed.command);
    } catch (error) {
      if (error instanceof DomainError && TERMINAL_BUSINESS_CONFLICT_CODES.has(error.code)) {
        conflictCode = error.code;
      } else {
        throw error;
      }
    }

    // 3. Mark inbox processed in the exact same SQL transaction.
    const processedAt = new Date(Math.max(Date.now(), inbox.receivedAt.getTime()));
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
    const queueUrl = await this.getMainQueueUrl();
    if (this.stopping) {
      return emptyPollStats();
    }

    const maxNumberOfMessages = options.maxNumberOfMessages ?? DEFAULT_MAX_MESSAGES;
    const waitTimeSeconds = options.waitTimeSeconds ?? this.config.sqsWaitTimeSeconds;
    const receiveTimeoutMs = waitTimeSeconds * 1_000 + this.config.sqsCallTimeoutMs;

    const stopController = new AbortController();
    const timeoutSignal = AbortSignal.timeout(receiveTimeoutMs);
    const combinedSignal = AbortSignal.any([stopController.signal, timeoutSignal]);
    this.receiveAbort = stopController;

    let messages: Message[];
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
      messages = response.Messages ?? [];
    } catch (error) {
      if (stopController.signal.aborted || this.stopping) {
        return emptyPollStats();
      }
      throw error;
    } finally {
      if (this.receiveAbort === stopController) {
        this.receiveAbort = undefined;
      }
    }

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

    for (let index = 0; index < messages.length; index += 1) {
      const message = messages[index];
      if (message === undefined) {
        continue;
      }

      // On SIGTERM/stop, do not start remaining messages; batch-release only unblocked groups.
      if (this.stopping) {
        const releaseOutcome = await this.releaseRemainingVisibility(
          queueUrl,
          messages.slice(index),
          inFlightPredecessorGroups,
        );
        visibilityReleased += releaseOutcome.released;
        retried += releaseOutcome.withheld;
        break;
      }

      const groupId = message.Attributes?.MessageGroupId;
      if (groupId !== undefined && blockedGroups.has(groupId)) {
        // Preserve strict per-wallet FIFO order when an earlier message in the same group failed.
        retried += 1;
        continue;
      }

      const { outcome, acknowledged } = await this.processReceivedMessage(message, options);
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
        { abortSignal: AbortSignal.timeout(this.config.sqsCallTimeoutMs) },
      );
    } catch (error) {
      const dlqReason = error instanceof Error ? error.message : String(error);
      this.logger.error({
        message: 'Failed to forward poison message to DLQ; leaving unacknowledged for native redrive',
        sqsMessageId: message.MessageId,
        failureCode,
        reason: dlqReason,
      });
      return { outcome: { status: 'retry_later', reason: dlqReason }, acknowledged: false };
    }

    const acknowledged = await this.acknowledgeAfterCommit(
      mainQueueUrl,
      message.ReceiptHandle,
      message.MessageId ?? 'poison',
    );
    this.logger.warn({
      message: 'Poison message forwarded to DLQ and acknowledged',
      sqsMessageId: message.MessageId,
      failureCode,
      reason,
    });
    return { outcome: { status: 'dead_lettered', reason, failureCode }, acknowledged };
  }

  private async acknowledgeAfterCommit(
    queueUrl: string,
    receiptHandle: string | undefined,
    messageId: string,
  ): Promise<boolean> {
    if (receiptHandle === undefined) {
      return true;
    }
    try {
      await this.sqs.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receiptHandle,
        }),
        { abortSignal: AbortSignal.timeout(this.config.sqsCallTimeoutMs) },
      );
      return true;
    } catch (error) {
      // Transaction is already committed; redelivery will hit inbox.isProcessed() and ACK cleanly.
      this.logger.warn({
        message: 'DeleteMessage failed after commit; redelivery will be deduplicated by inbox',
        messageId,
        reason: error instanceof Error ? error.message : String(error),
      });
      return false;
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

    const releaseBudgetMs = Math.min(
      this.config.sqsCallTimeoutMs,
      Math.max(100, Math.floor(this.config.shutdownTimeoutMs * 0.8)),
    );
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
        this.dlqUrl = undefined;
        this.logger.error({
          message: 'SQS consumer poll cycle failed',
          reason: error instanceof Error ? error.message : String(error),
        });
        if (this.running) {
          await this.sleep(ERROR_BACKOFF_MS);
        }
      }
    }
  }

  private async getMainQueueUrl(): Promise<string> {
    if (this.mainQueueUrl === undefined) {
      const response = await this.sqs.send(
        new GetQueueUrlCommand({ QueueName: this.config.wagerTransactionsQueue }),
        { abortSignal: AbortSignal.timeout(this.config.sqsCallTimeoutMs) },
      );
      if (!response.QueueUrl) {
        throw new Error(`Queue URL missing for ${this.config.wagerTransactionsQueue}`);
      }
      this.mainQueueUrl = response.QueueUrl;
    }
    return this.mainQueueUrl;
  }

  private async getDlqUrl(): Promise<string> {
    if (this.dlqUrl === undefined) {
      const response = await this.sqs.send(
        new GetQueueUrlCommand({ QueueName: this.config.wagerTransactionsDlq }),
        { abortSignal: AbortSignal.timeout(this.config.sqsCallTimeoutMs) },
      );
      if (!response.QueueUrl) {
        throw new Error(`Queue URL missing for ${this.config.wagerTransactionsDlq}`);
      }
      this.dlqUrl = response.QueueUrl;
    }
    return this.dlqUrl;
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
