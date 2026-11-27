import { SQSClient } from '@aws-sdk/client-sqs';
import { Module, type OnApplicationShutdown, Optional } from '@nestjs/common';
import { ShutdownService } from '../common/http/shutdown.service';
import { WageringModule } from '../wagering/wagering.module';
import { OutboxPublisherService } from './outbox-publisher.service';
import { sqsClientProvider } from './sqs.provider';
import { WagerTransactionsConsumer } from './wager-transactions.consumer';

@Module({
  imports: [WageringModule],
  providers: [sqsClientProvider, WagerTransactionsConsumer, OutboxPublisherService],
  exports: [SQSClient, WagerTransactionsConsumer, OutboxPublisherService],
})
export class MessagingModule implements OnApplicationShutdown {
  private destroyed = false;

  constructor(
    private readonly sqs: SQSClient,
    @Optional() private readonly shutdown?: ShutdownService,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    if (this.destroyed) {
      return;
    }
    if (this.shutdown !== undefined) {
      await this.shutdown.waitForResourceIdle('sqs');
    }
    this.destroyed = true;
    try {
      this.sqs.destroy();
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.shutdown?.markTeardownError(`SQSClient destroy failed: ${reason}`);
      throw error;
    }
  }
}
