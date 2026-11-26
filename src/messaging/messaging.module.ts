import { SQSClient } from '@aws-sdk/client-sqs';
import { Module, type OnApplicationShutdown } from '@nestjs/common';
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
  constructor(private readonly sqs: SQSClient) {}

  onApplicationShutdown(): void {
    this.sqs.destroy();
  }
}
