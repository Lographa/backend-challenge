import type { IncomingMessage, ServerResponse } from 'node:http';
import { createParamDecorator, type ExecutionContext } from '@nestjs/common';
import { newId } from '../ids';

export const CORRELATION_ID_HEADER = 'x-correlation-id';

/** Restricted so a caller-supplied id is safe to log and to echo in a header. */
const CORRELATION_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

const correlationIds = new WeakMap<IncomingMessage, string>();

/**
 * The request's correlation id: the caller's X-Correlation-Id when it is well formed, otherwise
 * a new one. Assigned once per request and echoed in the response header.
 */
export function assignCorrelationId(request: IncomingMessage, response: ServerResponse): string {
  let correlationId = correlationIds.get(request);
  if (correlationId === undefined) {
    const supplied = request.headers[CORRELATION_ID_HEADER];
    correlationId = typeof supplied === 'string' && CORRELATION_ID_PATTERN.test(supplied) ? supplied : newId();
    correlationIds.set(request, correlationId);
  }
  if (!response.headersSent) {
    response.setHeader(CORRELATION_ID_HEADER, correlationId);
  }
  return correlationId;
}

export function correlationMiddleware(request: IncomingMessage, response: ServerResponse, next: () => void): void {
  assignCorrelationId(request, response);
  next();
}

/** Controller parameter with the request's correlation id. */
export const CorrelationId = createParamDecorator((_data: unknown, context: ExecutionContext): string => {
  const http = context.switchToHttp();
  return assignCorrelationId(http.getRequest<IncomingMessage>(), http.getResponse<ServerResponse>());
});
