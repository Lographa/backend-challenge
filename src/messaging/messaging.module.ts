import { SQSClient } from '@aws-sdk/client-sqs';
import { Module, type OnApplicationShutdown } from '@nestjs/common';
import { sqsClientProvider } from './sqs.provider';

@Module({
  providers: [sqsClientProvider],
  exports: [SQSClient],
})
export class MessagingModule implements OnApplicationShutdown {
  constructor(private readonly sqs: SQSClient) {}

  onApplicationShutdown(): void {
    this.sqs.destroy();
  }
}
