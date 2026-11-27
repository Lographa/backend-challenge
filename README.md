# Wagering Processor

Serviço financeiro que processa apostas de vários provedores, com saldo materializado, ledger imutável e entrega assíncrona at-least-once.

O objetivo é manter o saldo igual ao ledger quando a mesma operação chega mais de uma vez, fora de ordem, ou em instâncias simultâneas, e quando o processo cai antes ou depois do commit.

## Documentação

- [Enunciado original](CHALLENGE.md)
- [Arquitetura](ARCHITECTURE.md)
- [Papéis do PostgreSQL e comandos destrutivos](docs/database.md)

## Tecnologias

- Bun 1.x e TypeScript
- NestJS
- MikroORM e PostgreSQL 17 (`postgres:17-alpine`)
- Amazon SQS via LocalStack 4.10
- Docker Compose
- decimal.js para valores monetários

As versões de imagem estão em `docker-compose.yml`. As versões das bibliotecas estão em `package.json`.

## Pré-requisitos

- Bun 1.x
- Docker com o comando `docker compose`

Não é necessário um PostgreSQL instalado na máquina. O Compose publica o banco na porta `5433`.

## Instalação

Na raiz do repositório:

```sh
bun install
```

`bun install` é o comando do runtime para instalar as dependências declaradas em `package.json`. Não existe um script `install` no projeto.

## Variáveis de ambiente

Copie o exemplo e ajuste só o que for necessário:

```sh
copy .env.example .env
```

No PowerShell, `copy` preserva o arquivo de exemplo. Em um shell Unix, o equivalente é `cp .env.example .env`.

As credenciais do exemplo são só para desenvolvimento local. Não as reutilize fora desta máquina.

| Variável | Função |
|---|---|
| `PORT` | Porta HTTP. `0` pede uma porta efêmera ao sistema. |
| `LOG_LEVEL` | Nível do logger. |
| `INSTANCE_ID` | Identificador do processo. Opcional; se ausente, é gerado. |
| `POSTGRES_USER` | Superusuário criado no primeiro volume do Postgres. |
| `POSTGRES_PASSWORD` | Senha desse superusuário. |
| `POSTGRES_DB` | Nome do banco criado no primeiro volume. |
| `POSTGRES_PORT` | Porta publicada do Postgres. |
| `DATABASE_ADMIN_URL` | Superusuário. Usado por `db:provision` e pela preparação dos testes. A aplicação não usa esta URL. |
| `MIGRATION_DATABASE_URL` | Dono do schema. Usado só por `db:migrate`, `db:migrate:down` e `db:migrate:status`. |
| `DATABASE_URL` | Usuário da aplicação. |
| `DATABASE_POOL_MAX` | Tamanho máximo do pool. |
| `MIGRATION_DOWN_CONFIRM_DATABASE` | Deixe sem valor. Autoriza `db:migrate:down` fora de um banco cujo nome termina em `_test` somente quando o valor é o nome exato do banco. |
| `ALLOW_NON_LOCAL_TEST_DATABASE_HOST` | Deixe sem valor. Os testes de integração recusam host que não seja local, salvo se este valor for exatamente esse host. |
| `AWS_REGION` | Região lida pelo SDK. No LocalStack o valor de exemplo é suficiente. |
| `AWS_ENDPOINT_URL` | Endpoint do LocalStack. |
| `AWS_ACCESS_KEY_ID` | Credencial fictícia aceita pelo LocalStack. |
| `AWS_SECRET_ACCESS_KEY` | Credencial fictícia aceita pelo LocalStack. |
| `SQS_WAGER_TRANSACTIONS_QUEUE` | Fila FIFO de transações de aposta. |
| `SQS_WAGER_TRANSACTIONS_DLQ` | Fila de mensagens rejeitadas pelo contrato. |
| `SQS_INTEGRATION_EVENTS_QUEUE` | Fila FIFO dos eventos da outbox. |
| `HEALTH_CHECK_TIMEOUT_MS` | Orçamento de cada checagem de readiness. |
| `SHUTDOWN_TIMEOUT_MS` | Tempo máximo do desligamento coordenado. |
| `WORKERS_ENABLED` | Liga o consumidor SQS, o publicador da outbox e o worker de referência pendente. |
| `SQS_WAIT_TIME_SECONDS` | Long poll do consumidor. |
| `SQS_CALL_TIMEOUT_MS` | Tempo máximo de uma chamada SQS. |
| `OUTBOX_POLL_INTERVAL_MS` | Intervalo do publicador da outbox. |
| `PENDING_REFERENCE_POLL_INTERVAL_MS` | Intervalo do worker de referência pendente. |
| `PENDING_REFERENCE_TTL_MS` | Tempo até rejeitar uma referência que não chegou. |
| `PENDING_REFERENCE_MAX_ATTEMPTS` | Tentativas automáticas dessa espera. |

`POSTGRES_USER`, `POSTGRES_PASSWORD` e `POSTGRES_DB` valem na criação do volume. Mudá-los depois exige recriar o volume. Os papéis `DATABASE_ADMIN_URL`, `MIGRATION_DATABASE_URL` e `DATABASE_URL` estão explicados em [docs/database.md](docs/database.md).

## Infraestrutura

Sobe PostgreSQL e LocalStack e espera os containers ficarem saudáveis. Volumes já existentes são mantidos.

```sh
bun run infra:up
```

Para só parar os containers, sem apagar dados:

```sh
bun run infra:down
```

### Comandos destrutivos

`bun run infra:destroy-volumes` executa `docker compose down -v` e apaga os volumes de PostgreSQL e LocalStack. Não há como desfazer.

`bun run db:migrate:down` reverte a última migration. Reverter a migration inicial derruba as tabelas, inclusive o ledger. Não serve para corrigir saldo.

As condições e as recusas desses comandos estão em [docs/database.md](docs/database.md).

## Banco de dados

Com a infraestrutura no ar e o `.env` carregado:

```sh
bun run db:provision
bun run db:migrate
```

`db:provision` cria ou atualiza os usuários a partir das três URLs. Não cria nem apaga banco, tabela ou linha.

`db:migrate` aplica as migrations com o usuário de migration.

Para consultar o estado:

```sh
bun run db:migrate:status
```

## Aplicação

```sh
bun run start
```

Durante o desenvolvimento, com recarga:

```sh
bun run dev
```

Com `WORKERS_ENABLED=true`, o processo também consome a fila de apostas, publica a outbox e reprocessa referências pendentes. Health:

- `GET /health/live`
- `GET /health/ready`

## Testes

```sh
bun run test:unit
bun run test:integration
bun run test
```

`test` executa a suíte do Bun no repositório. `test:unit` limita a `test/unit`. `test:integration` limita a `test/integration`.

Os testes de integração usam PostgreSQL e LocalStack reais. O banco de teste é derivado das URLs, com o sufixo `_test`, e a preparação recusa host não local. Detalhes em [docs/database.md](docs/database.md).

Verificação de tipos, sem emitir JavaScript:

```sh
bun run typecheck
```

## Scripts

| Script | Comando |
|---|---|
| `start` | `bun run src/main.ts` |
| `dev` | `bun --watch run src/main.ts` |
| `typecheck` | `tsc --noEmit` |
| `test` | `bun test` |
| `test:unit` | `bun test test/unit` |
| `test:integration` | `bun test test/integration` |
| `db:provision` | `bun run src/database/provision.ts` |
| `db:migrate` | `bun run src/database/migrate.ts up` |
| `db:migrate:down` | `bun run src/database/migrate.ts down` |
| `db:migrate:status` | `bun run src/database/migrate.ts status` |
| `infra:up` | `docker compose up -d --wait` |
| `infra:down` | `docker compose down` |
| `infra:destroy-volumes` | `docker compose down -v` |

Todos saem de `package.json`. Não há script de carga.
