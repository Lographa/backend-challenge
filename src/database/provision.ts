import { SQL } from 'bun';
import { createLogger } from '../common/observability/logger';

/** NOLOGIN role that receives the application's table privileges in migrations; it owns nothing. */
export const APPLICATION_ROLE = 'wagering_application';

const IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/;
const POSTGRES_PROTOCOLS = ['postgres:', 'postgresql:'];
const RESTRICTED = 'nosuperuser nocreatedb nocreaterole noreplication nobypassrls';

interface RoleCredentials {
  user: string;
  password: string;
  database: string;
  server: string;
}

export interface ProvisionUrls {
  /** Superuser of the PostgreSQL server (the Docker bootstrap user); used only here. */
  adminUrl: string;
  /** DATABASE_URL: the application's login user. */
  applicationUrl: string;
  /** MIGRATION_DATABASE_URL: owner of the schema and of every object in it. */
  migrationUrl: string;
}

/**
 * Idempotently creates the roles and applies database- and schema-level privileges on the
 * database named in the URLs. Never creates or drops databases, tables or rows; it can run
 * any number of times against a database that already holds data.
 */
export async function provisionDatabase(urls: ProvisionUrls): Promise<void> {
  const application = roleCredentials('DATABASE_URL', urls.applicationUrl);
  const migrator = roleCredentials('MIGRATION_DATABASE_URL', urls.migrationUrl);
  const admin = parsePostgresUrl('DATABASE_ADMIN_URL', urls.adminUrl);

  if (application.server !== migrator.server || admin.host !== application.server) {
    throw new Error('DATABASE_ADMIN_URL, DATABASE_URL and MIGRATION_DATABASE_URL must point to the same server');
  }
  if (application.database !== migrator.database) {
    throw new Error('DATABASE_URL and MIGRATION_DATABASE_URL must point to the same database');
  }
  const reserved = new Set([decodeURIComponent(admin.username), APPLICATION_ROLE]);
  if (application.user === migrator.user || reserved.has(application.user) || reserved.has(migrator.user)) {
    throw new Error(
      `The application and migration users must be distinct and differ from the admin user and from ${APPLICATION_ROLE}`,
    );
  }

  admin.pathname = `/${application.database}`;
  const sql = new SQL(admin.toString());
  try {
    const [current] = await sql`select rolsuper from pg_roles where rolname = current_user`;
    if (current?.rolsuper !== true) {
      throw new Error('DATABASE_ADMIN_URL must connect as a superuser to create roles and transfer ownership');
    }
    await sql.begin(async (tx) => {
      for (const statement of provisioningStatements(application, migrator)) {
        await tx.unsafe(statement);
      }
      await assertApplicationRoleIsolated(tx, application.user);
    });
  } finally {
    await sql.close();
  }
}

function provisioningStatements(application: RoleCredentials, migrator: RoleCredentials): string[] {
  const group = ident(APPLICATION_ROLE);
  const app = ident(application.user);
  const owner = ident(migrator.user);
  const database = ident(application.database);
  return [
    createRoleIfMissing(APPLICATION_ROLE),
    createRoleIfMissing(migrator.user),
    createRoleIfMissing(application.user),
    `alter role ${group} with nologin ${RESTRICTED} noinherit`,
    `alter role ${owner} with login ${RESTRICTED} noinherit password ${literal(migrator.password)}`,
    `alter role ${app} with login ${RESTRICTED} inherit password ${literal(application.password)}`,
    `grant ${group} to ${app}`,

    // No CONNECT or TEMPORARY for everyone else; temporary tables could shadow the tables that
    // trigger functions read.
    `revoke all on database ${database} from public`,
    `grant connect on database ${database} to ${group}, ${owner}`,

    `alter schema public owner to ${owner}`,
    `revoke all on schema public from public`,
    `grant usage on schema public to ${group}`,
    transferOwnershipTo(migrator.user),

    // Table privileges are granted explicitly by each migration; functions stop being executable by PUBLIC.
    `alter default privileges for role ${owner} revoke execute on functions from public`,
  ];
}

/** Objects created earlier by another role (e.g. the bootstrap superuser) move to the schema owner. */
function transferOwnershipTo(user: string): string {
  const name = literal(user);
  return `
    do $$
    declare
      target oid := (select oid from pg_roles where rolname = ${name});
      item record;
    begin
      for item in
        select c.oid::regclass as object from pg_class c
        where c.relnamespace = 'public'::regnamespace and c.relowner <> target
          and c.relkind in ('r', 'p', 'v', 'm', 'f')
      loop
        execute format('alter table %s owner to %I', item.object, ${name});
      end loop;
      -- sequences owned by a column follow their table
      for item in
        select c.oid::regclass as object from pg_class c
        where c.relnamespace = 'public'::regnamespace and c.relowner <> target and c.relkind = 'S'
          and not exists (select 1 from pg_depend d where d.objid = c.oid and d.deptype in ('a', 'i'))
      loop
        execute format('alter sequence %s owner to %I', item.object, ${name});
      end loop;
      for item in
        select p.oid::regprocedure as routine from pg_proc p
        where p.pronamespace = 'public'::regnamespace and p.proowner <> target
          and not exists (select 1 from pg_depend d where d.objid = p.oid and d.deptype = 'e')
      loop
        execute format('alter routine %s owner to %I', item.routine, ${name});
      end loop;
    end
    $$`;
}

async function assertApplicationRoleIsolated(tx: SQL, user: string): Promise<void> {
  const memberships = await tx`
    select r.rolname from pg_auth_members m
    join pg_roles r on r.oid = m.roleid
    where m.member = (select oid from pg_roles where rolname = ${user}) and r.rolname <> ${APPLICATION_ROLE}`;
  const groupMemberships = await tx`
    select 1 from pg_auth_members m
    where m.member = (select oid from pg_roles where rolname = ${APPLICATION_ROLE})`;
  const owned = await tx`
    select 1 from pg_class c join pg_roles r on r.oid = c.relowner
    where r.rolname in (${user}, ${APPLICATION_ROLE})
    union all
    select 1 from pg_namespace n join pg_roles r on r.oid = n.nspowner
    where r.rolname in (${user}, ${APPLICATION_ROLE})`;
  if (memberships.length > 0 || groupMemberships.length > 0 || owned.length > 0) {
    throw new Error(
      `${user} must only be a member of ${APPLICATION_ROLE}, and neither may own objects in this database`,
    );
  }
}

function createRoleIfMissing(name: string): string {
  return `
    do $$
    begin
      if not exists (select 1 from pg_roles where rolname = ${literal(name)}) then
        create role ${ident(name)};
      end if;
    end
    $$`;
}

function roleCredentials(variable: string, value: string): RoleCredentials {
  const url = parsePostgresUrl(variable, value);
  const user = decodeURIComponent(url.username);
  const password = decodeURIComponent(url.password);
  const database = decodeURIComponent(url.pathname.replace(/^\//, ''));
  if (!IDENTIFIER.test(user) || !IDENTIFIER.test(database)) {
    throw new Error(`${variable}: user and database must match ${IDENTIFIER}`);
  }
  if (password.length === 0 || password.includes('\0')) {
    throw new Error(`${variable}: a non-empty password is required`);
  }
  return { user, password, database, server: url.host };
}

function parsePostgresUrl(variable: string, value: string): URL {
  if (!URL.canParse(value) || !POSTGRES_PROTOCOLS.includes(new URL(value).protocol)) {
    throw new Error(`${variable} must be a postgres:// or postgresql:// URL`);
  }
  return new URL(value);
}

/** Only called with names already matching IDENTIFIER. */
function ident(name: string): string {
  return `"${name}"`;
}

function literal(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required (see .env.example)`);
  }
  return value;
}

if (import.meta.main) {
  const logger = createLogger('log');
  try {
    const urls = {
      adminUrl: requiredEnv('DATABASE_ADMIN_URL'),
      applicationUrl: requiredEnv('DATABASE_URL'),
      migrationUrl: requiredEnv('MIGRATION_DATABASE_URL'),
    };
    await provisionDatabase(urls);
    logger.log(
      {
        message: 'Database roles provisioned',
        database: new URL(urls.applicationUrl).pathname.slice(1),
        applicationRole: APPLICATION_ROLE,
      },
      'Provisioning',
    );
  } catch (error) {
    logger.fatal({ message: 'Provisioning failed', error: (error as Error).message }, 'Provisioning');
    process.exit(1);
  }
}
