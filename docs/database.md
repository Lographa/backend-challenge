# Database roles, provisioning and destructive commands

## Roles

| Variable | Role | Used by | Can |
| --- | --- | --- | --- |
| `DATABASE_ADMIN_URL` | `POSTGRES_USER` from `docker-compose.yml` (superuser) | `bun run db:provision`, integration test setup | everything; never used by the application |
| `MIGRATION_DATABASE_URL` | e.g. `wagering_migrator` | `bun run db:migrate*` only | owns the `public` schema and every table, sequence and function in it |
| `DATABASE_URL` | e.g. `wagering_app` | the application | the privileges of `wagering_application`, below |

`wagering_application` is a `NOLOGIN` group role that holds the application's table privileges; the
application user inherits them and nothing else. Neither owns any object.

| Table | Application privileges |
| --- | --- |
| `wallet_ledger_entries` | `SELECT`, `INSERT` |
| `wallets` | `SELECT`, `INSERT`, `UPDATE (balance_amount, version, updated_at)` |
| `wager_transactions` | `SELECT`, `INSERT`, `UPDATE (reference_transaction_id, status, failure_code, observed_balance_amount, attempt_count, next_attempt_at, processed_at)` |
| `inbox_messages` | `SELECT`, `INSERT`, `UPDATE (processed_at)` |
| `outbox_messages` | `SELECT`, `INSERT`, `UPDATE (attempts, next_attempt_at, published_at)` |
| `mikro_orm_migrations` | none |

Apart from the owner (and the superuser), no role has `DELETE`, `TRUNCATE`, `TRIGGER` or `REFERENCES` on these tables. `PUBLIC` has no
privileges on the database (no `CONNECT`, no `TEMP`), the schema (no `USAGE`, no `CREATE`), the tables or the
trigger functions. The trigger functions are `SECURITY INVOKER` with `search_path = public, pg_temp`.

Table grants live in the `ApplicationPrivileges` migration, so new tables need their grants added in the
migration that creates them. The migration user never grants `EXECUTE` on new functions to `PUBLIC`
(default privileges).

The migration user, as owner, can still `ALTER TABLE ... DISABLE TRIGGER`. Only migrations should use it,
and its credentials must not reach the running application.

## Provisioning

```sh
bun run infra:up        # start PostgreSQL and LocalStack (keeps existing volumes)
bun run db:provision    # create/update the two users and privileges (DATABASE_ADMIN_URL, DATABASE_URL, MIGRATION_DATABASE_URL)
bun run db:migrate      # apply migrations as the migration user
```

`db:provision` is idempotent and runs against new or existing volumes; Docker init scripts only run when
the volume is first created, so they are not used. It creates missing roles, resets their attributes and
passwords from the URLs, transfers ownership of the `public` schema and its objects to the migration user,
and sets database and schema privileges. It never creates or drops databases, tables or rows.

PostgreSQL roles are global to the server: the users exist for every database on it, including
`wagering_test`, which the integration tests provision with the same URLs.

## Destructive commands

- `bun run infra:down` stops the containers and keeps the data.
- `bun run infra:destroy-volumes` runs `docker compose down -v` and **deletes all PostgreSQL and LocalStack
  data**. There is no undo.
- `bun run db:migrate:down` reverts the last migration. Reverting the initial migration **drops every table,
  the financial ledger included**. A down migration is not a way to recover or correct ledger data; the
  ledger is append-only and is corrected only by new compensating entries. Down refuses databases whose name
  does not end in `_test` unless `MIGRATION_DOWN_CONFIRM_DATABASE` holds the exact database name. A missing,
  empty or other value (such as `true`) never authorizes it.
- `bun run db:migrate:status` creates the `mikro_orm_migrations` table if it is missing.

## Tests

Integration tests use `<database>_test` on the same server, derived from the three URLs (which must name the
same database). Before creating it, the setup refuses hosts other than `localhost`, `127.0.0.1` and `::1`
unless `ALLOW_NON_LOCAL_TEST_DATABASE_HOST` names the host exactly.
