import { InvalidStateError } from '../errors/domain.error';
import { PAYLOAD_HASH_PATTERN } from '../hashing/canonical-hash';

export interface InboxMessageState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt: Date | null;
}

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
}

const MAX_KEY_LENGTH = 255;

export class InboxMessage {
  private constructor(
    readonly messageId: string,
    readonly consumerName: string,
    readonly payloadHash: string,
    readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  static receive(props: ReceiveInboxProps): InboxMessage {
    for (const [field, value] of [
      ['messageId', props.messageId],
      ['consumerName', props.consumerName],
    ] as const) {
      if (value.length === 0 || value.length > MAX_KEY_LENGTH) {
        throw new RangeError(`${field} must have between 1 and ${MAX_KEY_LENGTH} characters`);
      }
    }
    if (!PAYLOAD_HASH_PATTERN.test(props.payloadHash)) {
      throw new RangeError('payloadHash must be a lowercase SHA-256 hex digest');
    }
    return new InboxMessage(props.messageId, props.consumerName, props.payloadHash, props.receivedAt);
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      state.receivedAt,
      state.processedAt ?? undefined,
    );
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined;
  }

  markProcessed(at: Date): void {
    if (this.isProcessed()) {
      throw new InvalidStateError(`Inbox message ${this.messageId} is already processed`);
    }
    this._processedAt = at;
  }

  toState(): InboxMessageState {
    return {
      messageId: this.messageId,
      consumerName: this.consumerName,
      payloadHash: this.payloadHash,
      receivedAt: this.receivedAt,
      processedAt: this._processedAt ?? null,
    };
  }
}
