#!/bin/bash
set -euo pipefail

# Visibility timeout must exceed the worst-case processing time of one message;
# maxReceiveCount is the retry budget before a message lands in the DLQ.
VISIBILITY_TIMEOUT=30
MAX_RECEIVE_COUNT=5

awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,MessageRetentionPeriod=1209600

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url "$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "{\"FifoQueue\":\"true\",\"VisibilityTimeout\":\"${VISIBILITY_TIMEOUT}\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE_COUNT}\\\"}\"}"

awslocal sqs create-queue \
  --queue-name wager-integration-events.fifo \
  --attributes FifoQueue=true

echo "SQS queues ready"
