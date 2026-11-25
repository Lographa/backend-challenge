import { type EntityProperty, type Platform, Type } from '@mikro-orm/core';

// PostgreSQL silently rounds extra decimals to the column scale, so they are refused here.
const DECIMAL_PATTERN = /^-?\d+(\.\d{1,2})?$/;

/**
 * numeric(19,2) carried as a decimal string end to end. A JS number in either
 * direction means precision may already be lost, so it is rejected loudly
 * instead of being coerced.
 */
export class DecimalType extends Type<string | null, string | null> {
  override convertToDatabaseValue(value: string | null): string | null {
    return assertDecimalString(value, 'write', this.prop);
  }

  override convertToJSValue(value: string | null): string | null {
    return assertDecimalString(value, 'read', this.prop);
  }

  override compareAsType(): string {
    return 'string';
  }

  override ensureComparable(): boolean {
    return false;
  }

  override getColumnType(_prop: EntityProperty, _platform: Platform): string {
    return 'numeric(19,2)';
  }
}

function assertDecimalString(
  value: unknown,
  direction: 'read' | 'write',
  prop: EntityProperty | undefined,
): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const column = prop?.name ?? 'numeric column';
  if (typeof value !== 'string') {
    throw new TypeError(
      `${column}: refusing to ${direction} a ${typeof value} as money; use a decimal string`,
    );
  }
  if (!DECIMAL_PATTERN.test(value)) {
    throw new TypeError(`${column}: "${value}" is not a plain decimal string with at most 2 decimals`);
  }
  return value;
}
