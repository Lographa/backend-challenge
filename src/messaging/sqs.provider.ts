import { SQSClient } from '@aws-sdk/client-sqs';
import type { Provider } from '@nestjs/common';
import { AppConfig } from '../common/config/env';

export const sqsClientProvider: Provider = {
  provide: SQSClient,
  inject: [AppConfig],
  useFactory: (config: AppConfig) =>
    new SQSClient({
      region: config.awsRegion,
      ...(config.awsEndpointUrl ? { endpoint: config.awsEndpointUrl } : {}),
    }),
};
