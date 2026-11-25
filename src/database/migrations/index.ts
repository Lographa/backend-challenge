import type { MigrationObject } from '@mikro-orm/core';
import { Migration20261125140000InitialSchema } from './Migration20261125140000InitialSchema';
import { Migration20261125180000LedgerHeadIntegrity } from './Migration20261125180000LedgerHeadIntegrity';
import { Migration20261125180100ApplicationPrivileges } from './Migration20261125180100ApplicationPrivileges';

/** Explicit, ordered list: no filesystem discovery, so it behaves the same under Bun and in a bundle. */
export const migrations: MigrationObject[] = [
  { name: 'Migration20261125140000InitialSchema', class: Migration20261125140000InitialSchema },
  { name: 'Migration20261125180000LedgerHeadIntegrity', class: Migration20261125180000LedgerHeadIntegrity },
  { name: 'Migration20261125180100ApplicationPrivileges', class: Migration20261125180100ApplicationPrivileges },
];
