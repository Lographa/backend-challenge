import type { MigrationObject } from '@mikro-orm/core';
import { Migration20261125140000InitialSchema } from './Migration20261125140000InitialSchema';

/** Explicit, ordered list: no filesystem discovery, so it behaves the same under Bun and in a bundle. */
export const migrations: MigrationObject[] = [
  { name: 'Migration20261125140000InitialSchema', class: Migration20261125140000InitialSchema },
];
