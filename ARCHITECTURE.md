# Arquitetura

Este documento descreve o que o processo faz hoje, por que essas escolhas foram feitas e o que elas garantem. O enunciado está em [CHALLENGE.md](CHALLENGE.md). A operação local está em [README.md](README.md). Os papéis do PostgreSQL estão em [docs/database.md](docs/database.md).

Três classificações aparecem ao longo do texto.

- **Implementado:** há código de produção que executa o comportamento.
- **Não implementado:** o enunciado aceita a ausência, ou o tipo existe sem um caminho que o produza.
- **Possibilidade futura:** extensão explícita, sem código que a realize.

## 1. Visão geral

Um processo NestJS atende HTTP e, quando `WORKERS_ENABLED=true`, roda três ciclos no mesmo processo: o consumidor da fila de apostas, o publicador da outbox e o worker de referências pendentes. Várias instâncias podem correr ao mesmo tempo. A correção não depende de haver uma só.

O saldo vive na linha da wallet. Cada movimento também vira uma linha imutável no ledger. A aposta, o ledger e os eventos de integração entram na mesma transação PostgreSQL. A publicação SQS acontece depois, noutro ciclo, lendo a outbox. O ack da mensagem de aposta acontece depois do commit.

A unidade de concorrência é a linha da wallet. Wallets diferentes avançam em paralelo. Não há lock advisory global.

```
HTTP ou SQS
    -> transação PostgreSQL (inbox, quando vem da fila; wallet; aposta; ledger; outbox)
    -> commit
    -> DeleteMessage da aposta, ou ciclo separado de publicação da outbox
```

Arquivos de entrada: `src/main.ts`, `src/app.module.ts`, `src/wagering/wager-processor.service.ts`, `src/messaging/wager-transactions.consumer.ts`, `src/messaging/outbox-publisher.service.ts`, `src/wagering/pending-reference.worker.ts`.

## 2. Persistência, MikroORM, PostgreSQL, Money e transações

**Implementado.** O ORM é MikroORM sobre PostgreSQL. Prisma não é usado. A justificativa é o `EntityManager.transactional()`, o `LockMode` e um fork por transação, para o identity map de uma requisição não vazar para outra.

Dinheiro não atravessa o processo como `number`. `Money` (`src/common/money/money.ts`) guarda um `Decimal` do decimal.js, com escala 2, arredondamento half-even e teto de `numeric(19,2)`. O contrato aceita string decimal não negativa. `DecimalType` (`src/database/decimal.type.ts`) recusa qualquer valor que não seja string decimal na ida e na volta do driver. Se o driver devolver um `number`, a leitura falha em vez de seguir com precisão já perdida.

As colunas monetárias são `numeric(19,2)`. Constraints e triggers do schema impedem saldo negativo, ledger mutável e saldo da wallet divergente do último lançamento durante a operação normal. A aplicação não é a única barreira. As migrations estão em `src/database/migrations/`. O usuário da aplicação não tem `DELETE`, `TRUNCATE` nem `TRIGGER`. O detalhe dos papéis está em [docs/database.md](docs/database.md).

`em.fork().transactional()` abre a transação, executa o callback e o `flush`, e só então envia `COMMIT`. Se o callback ou o commit falha, a promise rejeita e o que foi contado ou publicado depois desse `await` não roda. Isolamento das transações de escrita: `READ COMMITTED`. A reconciliação usa `REPEATABLE READ` somente leitura, para um movimento commitado no meio da leitura não parecer divergência.

**Não implementado.** Partidas dobradas. O ledger é de lançamento simples: um débito ou um crédito por movimento, com saldo anterior e posterior.

## 3. Consistência financeira e lock pessimista

**Implementado.** `WagerProcessorService` bloqueia a wallet com `LockMode.PESSIMISTIC_WRITE` e `refresh: true` antes de decidir o movimento (`src/wagering/wager-processor.service.ts`). O refresh existe porque o identity map do MikroORM poderia devolver o saldo lido antes do lock.

Na fila, a ordem de lock é a linha do inbox e depois a wallet. A ordem fixa evita deadlock entre duas mensagens que tomariam as duas linhas em ordens diferentes.

`BET` debita. `WIN` credita. `LOSS` não move saldo. `REFUND` credita o valor integral de uma `BET` já processada, uma vez. `ROLLBACK` inverte uma transação já processada, uma vez, do mesmo provedor, jogador, wallet, moeda e rodada. Valor diferente do original é `AMOUNT_MISMATCH`. Reversão que deixaria o saldo negativo é `NEGATIVE_BALANCE_REVERSAL`, distinto de `INSUFFICIENT_FUNDS`. `OPENING` é interno: só a abertura de wallet com saldo inicial positivo o grava, já `PROCESSED`, e a API rejeita esse kind.

O saldo observado fica na própria transação. Um replay devolve esse saldo, não uma leitura nova da wallet. Sem isso, um crédito posterior mudaria a resposta de uma aposta já fechada.

A reconciliação (`src/wallets/wallet-reconciliation.ts`, `src/wallets/wallets.service.ts`) soma o ledger e compara com o saldo gravado. Ela não corrige a wallet. Divergência volta no corpo HTTP, inclusive `difference`, e é contada. O log não leva valor monetário.

**Trade-off.** A reconciliação carrega todos os lançamentos da wallet na mesma leitura. Isso torna o resultado fiel àquele snapshot e custa memória e tempo quando o ledger é longo. Não há paginação nessa leitura.

## 4. Idempotência, inbox e payloadHash

**Implementado.** A chave de idempotência vem do header `Idempotency-Key`. O serviço não inventa uma chave quando o header falta: a requisição é `INVALID_PAYLOAD`. A sugestão `{providerId}:{externalTransactionId}` do enunciado é convenção de quem chama, não um default do servidor.

`payloadHash` é SHA-256 em hexadecimal de um JSON canônico (`src/common/hashing/canonical-hash.ts`, `src/wagering/wager-payload.ts`). As chaves do objeto são ordenadas. Entram só campos de negócio: provedor, id externo, jogador, wallet, rodada, jogo, kind, dinheiro normalizado em duas casas e a referência externa, quando existe. Não entram `Idempotency-Key`, `X-Correlation-Id`, `messageId` nem `occurredAt`. `"25"` e `"25.00"` produzem o mesmo digest porque `Money` normaliza a escala antes do hash.

A mesma chave com o mesmo hash devolve o resultado original e `idempotentReplay: true`. A mesma chave com outro hash é `IDEMPOTENCY_PAYLOAD_CONFLICT`. Isso é conflito, não replay.

No SQS, o inbox (`inbox_messages`) deduplica pelo par fixo `consumer_name = wager-transactions` e `messageId`. O nome não inclui a instância. Se incluísse, uma reentrega noutra instância aplicaria o débito de novo. O hash do inbox cobre o tipo da mensagem, a chave de idempotência e o `payloadHash` de negócio (`src/messaging/wager-message.ts`). Mensagem já processada com o mesmo hash é duplicata e recebe ack. O mesmo `messageId` com outro hash vai para a DLQ.

Cache em memória não é a garantia. A unicidade está no banco.

## 5. Outbox, SQS, retries, DLQ e confirmação

**Implementado.** Eventos de integração são inseridos na `outbox_messages` dentro da transação financeira (`src/database/outbox.ts`). Nenhum `SendMessage` ocorre antes do commit. O publicador (`src/messaging/outbox-publisher.service.ts`) lê, em outra transação, no máximo uma cabeça não publicada por `aggregate_id`, com `FOR UPDATE OF o SKIP LOCKED`, e envia um lote de até 10. O grupo FIFO é o `aggregateId`, que nas apostas é a wallet. A ordem entre eventos da mesma wallet permanece. Wallets diferentes não se bloqueiam.

Falha de envio chama `scheduleRetry`: incrementa `attempts` e grava `next_attempt_at` com backoff exponencial a partir de 1s, teto de 5 minutos (`src/common/messaging/outbox-message.ts`). Na décima tentativa automática a mensagem fica em quarentena, com `next_attempt_at` em `9999-12-31T23:59:59.999Z`. Ela continua pendente e segura os eventos seguintes da mesma wallet. `requeueQuarantinedAggregate` só move essa data; não diminui `attempts`. A métrica de retry da outbox conta `failed - quarantined` depois que a transação que persistiu o backoff resolve. Quarentena não conta como retry. Lote vazio, orçamento de shutdown esgotado antes do envio e rollback não contam.

O consumidor só chama `DeleteMessage` depois do commit (`acknowledgeAfterCommit` em `src/messaging/wager-transactions.consumer.ts`). Falha desse delete deixa a mensagem para reentrega. O inbox então a reconhece como duplicata. O log desse caminho registra o nome do erro, não `error.message`.

Mensagem com payload inválido, ou `messageId` já visto com outro hash, é enviada a `wager-transactions-dlq.fifo`. O delete da fila principal só ocorre depois do `SendMessage` confirmado. Se o envio à DLQ falha, a mensagem fica sem ack e a redrive nativa da SQS continua sendo a rede de segurança. Códigos permanentes que não mudariam se a mensagem voltasse — `IDEMPOTENCY_PAYLOAD_CONFLICT`, `WALLET_PLAYER_MISMATCH`, `CURRENCY_MISMATCH` — gravam o inbox e recebem ack. `WALLET_NOT_FOUND` e `PERMANENT_INTEGRITY_ERROR` não entram nesse conjunto: a transação desfaz e a mensagem fica para retry.

`PENDING_REFERENCE` recebe ack depois de gravada. Quem reprocessa é o worker, não uma nova entrega SQS. Isso evita dois aplicadores da mesma reversão.

A métrica `wager_dlq_forwards_completed_total` sobe só quando o envio à DLQ teve sucesso e um `DeleteMessage` com `receiptHandle` presente também teve sucesso. Não é profundidade da DLQ nem contagem da redrive nativa.

**Trade-off.** Publicar fora da transação financeira admite um evento commitado ainda não visível na SQS. O atraso aparece no gauge de lag. A alternativa, publicar dentro da transação, perderia o evento ou o saldo quando um dos dois lados falhasse.

## 6. Transições de estado e failureCode

**Implementado.** Os status estão em `src/wagering/wager-transaction.ts`.

| Status | Papel |
|---|---|
| `PENDING` | Só existe dentro da transação que ainda está decidindo. Não é resposta HTTP. |
| `PENDING_REFERENCE` | A referência ainda não chegou. Persistido. O worker é quem continua. |
| `PROCESSED` | Terminal. Saldo observado gravado. |
| `REJECTED` | Terminal. Regra de negócio recusou. Há `failureCode` e saldo observado. Não gera ledger. |
| `FAILED` | Terminal no modelo. Ver a limitação abaixo. |

Transições de domínio: a transação em decisão vai para `PROCESSED`, `REJECTED` ou `PENDING_REFERENCE`. `PENDING_REFERENCE` pode ir a `PROCESSED` ou `REJECTED` quando a referência aparece ou o prazo acaba. `PROCESSED`, `REJECTED` e `FAILED` recusam nova transição com erro de programação, não com um `failureCode` de negócio.

`failureCode` é o enum `FailureCode` (`src/common/errors/failure-code.ts`). O provedor distingue, pelo código, se corrige o payload, se desiste ou se reenvia. Códigos de payload inválido (`INVALID_PAYLOAD`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `UNSUPPORTED_KIND`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`) pedem correção. `INSUFFICIENT_FUNDS` e `NEGATIVE_BALANCE_REVERSAL` são recusas de negócio diferentes. Conflito de hash é `IDEMPOTENCY_PAYLOAD_CONFLICT`. Referência ausente até esgotar a espera é `REFERENCE_NOT_FOUND`.

**Não implementado como fluxo.** `WagerTransaction.fail()` existe e leva a `FAILED`, mas nenhum chamador de produção o invoca. O status está no modelo e nas colunas. As rotas atuais não o produzem. A métrica de transição não tem série `FAILED` nem `PENDING`.

## 7. Worker PENDING_REFERENCE

**Implementado.** `PendingReferenceWorker` (`src/wagering/pending-reference.worker.ts`) varre transações `PENDING_REFERENCE` com `next_attempt_at` vencido. Cada tentativa abre a própria transação, bloqueia a wallet e a transação, e ou resolve, ou reagenda, ou rejeita.

O backoff é 1s, 2s, 4s, 8s, com teto de 15s (`src/wagering/wager-payload.ts`). O padrão de `PENDING_REFERENCE_MAX_ATTEMPTS` é 10. O padrão de `PENDING_REFERENCE_TTL_MS` é 60s. Esgotado o número de tentativas ou o TTL, o resultado é `REJECTED` com `REFERENCE_NOT_FOUND`.

Só uma reagenda já commitada conta como retry de negócio na métrica. Primeira ida a `PENDING_REFERENCE` é transição de status, não retry. Exceção que desfaz a tentativa não conta retry. Resolução para `PROCESSED` ou `REJECTED` conta transição de status.

**Limitação.** O worker é o único reprocessador. Se `WORKERS_ENABLED=false`, a pendência permanece até um processo com o worker ligado.

## 8. API HTTP, erros e autenticação

**Implementado.**

| Método | Caminho | Resultado |
|---|---|---|
| `POST` | `/wallets` | Abre wallet. |
| `GET` | `/wallets/:walletId` | Lê a wallet. |
| `GET` | `/wallets/:walletId/ledger` | Ledger paginado por versão. |
| `POST` | `/wallets/:walletId/reconciliation` | Relatório somente leitura. |
| `POST` | `/wagering/transactions` | Processa aposta. Exige `Idempotency-Key`. |
| `GET` | `/wagering/transactions/:transactionId` | Lê por id. |
| `GET` | `/providers/:providerId/wagering/transactions/:externalTransactionId` | Lê pela chave externa. |
| `GET` | `/health/live` | Processo vivo. Não consulta dependências. |
| `GET` | `/health/ready` | Postgres e SQS, dentro de `HEALTH_CHECK_TIMEOUT_MS`. |

`POST /wagering/transactions` responde `200` para `PROCESSED`, `202` para `PENDING_REFERENCE` e `422` para `REJECTED`. O corpo de `PROCESSED` e `REJECTED` traz o saldo observado na época. Replay repete esse corpo com `idempotentReplay: true`.

Erros que não são resultado de negócio passam por `DomainExceptionFilter` (`src/common/http/domain-exception.filter.ts`):

| Situação | HTTP | Código |
|---|---|---|
| Payload inválido | 400 | `FailureCode` correspondente |
| Alvo inexistente | 404 | `WALLET_NOT_FOUND` ou `TRANSACTION_NOT_FOUND` |
| Conflito com o que já foi gravado | 409 | `IDEMPOTENCY_PAYLOAD_CONFLICT` ou `WALLET_ALREADY_EXISTS` |
| Regra de negócio | 422 | por exemplo `INSUFFICIENT_FUNDS` |
| Dependência indisponível, inclusive deadlock e lock não disponível | 503 | `SERVICE_UNAVAILABLE` |
| Falha inesperada | 500 | `INTERNAL_ERROR` ou `PERMANENT_INTEGRITY_ERROR` |

O corpo de erro é `{ code, message, correlationId }`. HTTP 4xx de negócio não ganha log novo. HTTP 500 e 503 logam o nome do erro e, quando cabe, um código curto. Não logam `Error.message`, porque a mensagem do driver inclui SQL.

**Não implementado.** Autenticação. `AuthGuard` (`src/common/auth/auth.guard.ts`) aceita toda requisição. Rotas marcadas com `@Public()` continuariam abertas se um verificador real substituísse `authenticate`. Hoje health é público e o restante também passa, porque `authenticate` retorna `true`. Mensagens da fila são canal interno. A identidade do provedor dentro da mensagem continua sujeita às regras de domínio, como moeda e dono da wallet.

**Possibilidade futura.** Um Identity Provider externo, OIDC, verificado dentro de `authenticate`. Health permaneceria público. Não há Keycloak, Zitadel nem tabela de usuários. Não há `ProviderIdentityPort` separado: o ponto de extensão é o próprio `AuthGuard`.

## 9. Logs estruturados e nove métricas

**Implementado.** O logger é o `ConsoleLogger` do Nest com JSON e `flattenParams: true` (`src/common/observability/logger.ts`). Campos passados depois do texto da mensagem sobem para o objeto JSON. Cada linha leva `instanceId`.

Quando o valor já está em memória, o log pode incluir `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId`. Campo ausente é omitido. Não se faz consulta extra para preenchê-los. `sqsMessageId` é o id da mensagem SQS e não é copiado para `messageId`. O log de outbox usa `walletId` com o `aggregateId` já carregado.

Não entram corpo HTTP, corpo SQS, payload da outbox, objeto `Money`, senha, connection string nem `Error.message`. `safeErrorFields` (`src/common/observability/log-fields.ts`) registra o nome e, para erro de domínio ou falha transitória, um código curto.

O registro de métricas é um objeto em memória por processo (`src/common/observability/metrics.ts`). `snapshot()` devolve `instanceId`, `scope: "process"`, contadores, gauges, histogramas e `instrumentationFailures`. Esse último conta falha do próprio registro. Não é uma das nove métricas e não é rótulo. Nome ou rótulo fora do conjunto fechado é recusado. Identificadores de wallet, transação, mensagem, correlação, provedor, idempotência ou evento não são rótulos. Falha do registro não escapa para a transação financeira.

Não há rota `/metrics`. Um snapshot JSON `metrics_snapshot` é emitido a cada 60s, com o timer dessassociado do processo para não segurar o encerramento. O desligamento gracioso emite mais um, com `emission: "shutdown"`, depois de fechar a aplicação. O watchdog de timeout duro sai sem esse snapshot final.

| Métrica | Tipo | O que conta |
|---|---|---|
| `wager_transaction_status_transitions_total` | contador, rótulo `status` | Uma transição commitada para `PENDING_REFERENCE`, `PROCESSED` ou `REJECTED`. |
| `wager_duplicates_total` | contador, rótulo `cause` | `inbox` ou `idempotency_replay`. |
| `wager_hash_conflicts_total` | contador, rótulo `cause` | `inbox_payload` ou `idempotency`. |
| `wager_retries_total` | contador, rótulos `channel` e `cause` | Retry persistido ou deixado para reentrega. Canais `sqs`, `outbox`, `pending_reference`. Causas `business`, `infrastructure`, `unexpected`. |
| `wager_dlq_forwards_completed_total` | contador sem rótulo | Envio à DLQ e `DeleteMessage` da origem, os dois confirmados. |
| `wager_lock_conflicts_total` | contador, rótulo `sqlstate` | `40P01` ou `55P03`, inclusive em `cause`. `40001` é retry de infraestrutura, não lock. |
| `wager_outbox_lag_ms` | gauge | Idade, em ms, do `occurred_at` mais antigo ainda não publicado. Sem linha, o valor é 0. |
| `wager_processing_duration_ms` | histograma | Duração da transação SQL. Canais `http`, `sqs`, `pending_reference`. Resultados `completed` ou `error`. Baldes em ms: 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000. Acima de 5000 vai para overflow. |
| `wallet_reconciliation_mismatch_total` | contador sem rótulo | Uma leitura de reconciliação com `consistent: false`. |

Contadores de processos diferentes não formam um total global. Somar gauges de lag de várias instâncias conta o mesmo atraso mais de uma vez. O gauge de lag é amostrado no máximo uma vez por segundo, fora do `SELECT … FOR UPDATE` do publicador, e só se o registro de métricas foi injetado. A consulta não cria índice. O índice parcial existente não elimina a ordenação por `occurred_at`.

**Não implementado.** OpenTelemetry, Prometheus, dashboard e endpoint público de métricas. O `AuthGuard` aceita todo mundo, então um endpoint de métricas ficaria público. Por isso a exposição ficou no log.

**Possibilidade futura.** Exportar o mesmo snapshot para um coletor, com agregação feita fora do processo. Isso não muda o significado das séries.

## 10. Shutdown, recuperação e reconciliação

**Implementado.** `ShutdownService` (`src/common/http/shutdown.service.ts`) marca o desligamento, drena HTTP e operações críticas dentro de `SHUTDOWN_TIMEOUT_MS` e para o timer de métricas sem zerar contadores. Operação nova durante o dreno recebe `503`. O ack e a publicação que ainda cabem no orçamento terminam. O que não cabe fica para a reentrega ou para o próximo ciclo da outbox.

O inbox torna a reentrega depois do commit e antes do ack idempotente. O ledger e os triggers tornam um replay que passasse do inbox incapaz de reescrever o saldo sem um lançamento correspondente.

A reconciliação é o diagnóstico, não o reparo. Correção de saldo, se um dia for necessária, é um novo lançamento compensatório, não um `UPDATE` direto e não um `db:migrate:down`.

**Limitação do teste de divergência.** O trigger `wallets_balance_matches_ledger` impede saldo diferente do ledger. O teste de integração que observa a métrica desliga esse trigger e `wallets_guard` só em `wagering_test`, mede, e volta a ligá-los. A aplicação em execução não tem privilégio de desligar trigger.

## 11. Decisões e trade-offs

| Decisão | Motivo | Consequência |
|---|---|---|
| Lock pessimista da linha da wallet | Perder atualização com leitura-cálculo-gravação é falha eliminatória | Wallets quentes serializam. Wallets distintas seguem em paralelo |
| Inbox com nome fixo | Redelivery em outra instância não pode reaplicar o débito | Todas as instâncias compartilham a mesma deduplicação |
| Ack só depois do commit | Crash entre commit e ack é recuperável; crash antes do ack sem commit não perde nem duplica | A mensagem pode reaparecer. O inbox absorve a cópia |
| Outbox fora do envio SQS | Evento e saldo commitam juntos | Há janela em que o evento existe só no Postgres. O lag mede essa janela |
| FIFO por wallet, não uma fila global | O enunciado proíbe lock global | HTTP não passa pela fila. Duas requisições HTTP na mesma wallet disputam o lock do banco |
| Métricas só em memória | Não há autenticação para proteger `/metrics` | Cada processo vê só a si. Não há série histórica depois do reinício |
| `FAILED` no modelo sem chamador | O enunciado pede o estado terminal | Nenhum fluxo atual grava `FAILED` |
| Auth como guarda que aceita todos | Autenticação não pontua e não deve atrasar a correção financeira | Qualquer cliente na rede local chama a API |

## 12. Limitações conhecidas

Estas limitações estão no código. Não foram corrigidas pela documentação.

1. Mensagem envenenada sem `receiptHandle`. `acknowledgeAfterCommit` retorna `true` quando não há handle, sem executar `DeleteMessage`. `forwardPoisonToDlqAndAck` não incrementa `wager_dlq_forwards_completed_total` e não emite o log de encaminhamento confirmado nesse caso. O retorno da mensagem, porém, continua `acknowledged: true` com status `dead_lettered`. A mensagem permanece na fila e a SQS a reentrega quando a visibilidade acaba. O lote local não bloqueia o grupo FIFO, porque trata o resultado como concluído. Uma mensagem recebida de fato pela SQS traz `receiptHandle`. O caminho existe para uma mensagem sem esse campo. Código: `src/messaging/wager-transactions.consumer.ts`.

2. Logger do orçamento esgotado. No `catch` de falha do `DeleteMessage`, uma exceção do logger é absorvida e o método retorna `false`. O aviso emitido quando o orçamento cooperativo já está esgotado, antes dessa tentativa de delete, não está dentro de um `try`. Se esse `logger.warn` lançar, a exceção sai de `acknowledgeAfterCommit` depois do commit financeiro. O saldo commitado não é desfeito. Código: `src/messaging/wager-transactions.consumer.ts`, `acknowledgeAfterCommit`.

3. `rowCount` do retry da outbox. Cada cabeça não publicada chama `scheduleRetry` e um `UPDATE` de `attempts` e `next_attempt_at`. `failed` sobe uma vez por essa linha. O código não lê quantas linhas o `UPDATE` afetou. A linha foi bloqueada com `FOR UPDATE` na mesma transação, e o trigger `outbox_messages_guard` ou aceita a alteração ou aborta a transação. Se abortar, a métrica não sobe, porque a promise da transação rejeita. Não há verificação explícita de `rowCount`. Código: `src/messaging/outbox-publisher.service.ts`.

Outras limitações, também presentes:

- Métricas morrem com o processo e não se somam entre instâncias.
- `WagerTransaction.fail()` não tem chamador. `FAILED` não é produzido.
- A reconciliação lê o ledger inteiro da wallet.
- O gauge de lag pode ordenar uma varredura grande. O custo com muitos eventos não publicados não foi medido em produção, e não foi criado índice novo.
- Não há OpenTelemetry, dashboard, teste de carga nem Identity Provider.
- `bun run db:migrate:down` e `bun run infra:destroy-volumes` destroem dados. Não corrigem ledger.
