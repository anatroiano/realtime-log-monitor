# Monitoramento de Logs

## Objetivo

Projeto de portfólio para monitorar logs em tempo real e praticar Java, Kafka, Kafka Streams e Grafana. Três microsserviços recebem logs de aplicações simuladas, agregam erros por serviço e minuto e registram incidentes.

## Fluxo

Aplicações → Coletor → Kafka → Processador → Kafka → Serviço de alertas → PostgreSQL → Grafana

Os três serviços expõem métricas operacionais via Actuator. O Prometheus coleta essas métricas e o Grafana consulta o Prometheus e o PostgreSQL. O painel operacional acompanha o próprio pipeline. O painel de incidentes mostra as agregações e os alertas produzidos a partir dos logs.

## Microsserviços

| Serviço | Entrada e saída | Responsabilidade |
| --- | --- | --- |
| `log-collector-service` | `POST /logs` → `logs.raw` | Receber lotes JSON, validar antes de publicar e enviar um registro por log ao Kafka com `serviceName` como chave. Responder `202` após o broker confirmar todos os envios do lote. O cliente conserva os mesmos `logId` em retentativas. |
| `log-processor-service` | `logs.raw` → `logs.metrics` e `alerts.triggered` | Usar Kafka Streams para deduplicar logs e contar entradas por serviço em janelas fixas de um minuto, com base no timestamp original. Contar `ERROR` e `FATAL` como erros e gerar um alerta quando a contagem atingir 51 na janela. Publicar as métricas agrupadas por intervalo, com o snapshot mais recente de cada janela. |
| `alert-service` | `logs.metrics` e `alerts.triggered` → PostgreSQL e `GET /alerts` | Atualizar as contagens por serviço e janela, registrar incidentes, disponibilizar o histórico e enviar notificações via webhook. |

## Contratos Kafka

Os tópicos trocam eventos JSON. Use timestamps em UTC. As métricas da janela são totais acumulados e podem ser publicadas mais de uma vez à medida que chegam logs.

| Tópico | Chave | Campos principais |
| --- | --- | --- |
| `logs.raw` | `serviceName` | `logId`, `timestamp`, `serviceName`, `host`, `level`, `message` |
| `logs.metrics` | `serviceName + windowStart` | `serviceName`, `windowStart`, `windowEnd`, `totalLogs`, `errorCount` |
| `alerts.triggered` | `alertId` | `alertId`, `serviceName`, `ruleId`, `windowStart`, `windowEnd`, `errorCount` |

O `alertId` é estável para a combinação de serviço, regra e início da janela. Defina a serialização das chaves compostas junto com os contratos. Trate deduplicação de reentregas e cooldown como regras separadas. A primeira impede que o mesmo alerta seja registrado duas vezes. A segunda limita notificações de alertas distintos em janelas consecutivas.

## Persistência e propriedade dos dados

- **Coletor** usa o Kafka para publicar os logs brutos. O tópico `logs.raw` retém os registros pelo período configurado no broker.
- **Processador** usa state stores locais gerenciados pelo Kafka Streams para agregações e deduplicação. Mantenha o changelog habilitado para permitir a restauração desse estado a partir do Kafka.
- **Serviço de alertas** é o único serviço Java com PostgreSQL. Use Spring Data JPA para entidades e consultas e Flyway para versionar as tabelas. Grave agregações com upsert por `(service_name, window_start)` e imponha unicidade de `alert_id` no histórico. Faça esses upserts com `INSERT ... ON CONFLICT` nativo no repositório Spring Data, sem consulta de existência seguida de inserção. Aplique um snapshot de janela só quando `total_logs` for maior que o gravado, para que reentregas antigas não façam a contagem regredir. Confirme o consumo somente após persistir os dados.
- **Grafana** usa uma conta PostgreSQL de leitura para consultar agregações e incidentes. O Prometheus mantém separadamente as métricas operacionais coletadas dos serviços.

## Geração dos projetos

Gere os três projetos em Java 21, Maven e empacotamento Jar. Use a mesma versão estável do Spring Boot nos três. Coloque cada projeto em um diretório com o nome do serviço na raiz do repositório.

| Projeto | Dependências no Spring Initializr |
| --- | --- |
| `log-collector-service` | Spring Web, Validation, Spring for Apache Kafka, Spring Boot Actuator, Prometheus |
| `log-processor-service` | Spring for Apache Kafka, Spring for Apache Kafka Streams, Spring Web, Spring Boot Actuator, Prometheus |
| `alert-service` | Spring Web, Validation, Spring for Apache Kafka, Spring Data JPA, PostgreSQL Driver, Flyway Migration, Spring Boot Actuator, Prometheus |

No processador, Spring Web serve ao endpoint HTTP do Actuator. Se a opção de Kafka Streams estiver ausente no Initializr para a versão escolhida, adicione `org.apache.kafka:kafka-streams` ao POM do processador. No serviço de alertas, confira se o POM inclui `org.flywaydb:flyway-database-postgresql` e adicione a dependência se necessário. Exponha `health` e `prometheus` no Actuator para a execução local.

## Regras de trabalho para agentes

- Responda em português. Use inglês em nomes de código, branches e mensagens de commit.
- Não execute compilação, testes ou comandos Maven sem autorização explícita. Ao terminar, informe quais comandos rodar, por exemplo `docker compose run --rm tests` para todos os testes ou `docker compose run --rm tests -pl alert-service clean test -Pkafka-integration` para um serviço.
- Escreva e atualize os testes afetados pela mudança e diga claramente que eles não foram executados.
- Não crie commits nem troque de branch sem pedido. Preserve alterações existentes e o estado de staging.
- Antes de implementar uma etapa, leia o código do serviço afetado e reutilize o que já existe nele.
- Registre em `docs/adr` decisões de arquitetura que não fiquem óbvias pelo código ou por este arquivo.

## Práticas de desenvolvimento

- Use inglês em nomes de classes, métodos, variáveis, pacotes, testes, mensagens técnicas e campos novos de API ou eventos. Preserve nomes já publicados em contratos.
- Escreva código sem comentários explicativos. Prefira nomes precisos, métodos pequenos e fluxo direto. Registre decisões e justificativas na documentação.
- Reutilize código existente dentro de cada serviço. Extraia lógica quando representar a mesma regra de negócio e precisar evoluir em conjunto. Mantenha os serviços independentes e os contratos de integração explícitos.
- Use padrões quando resolverem um problema concreto. Separe a lógica de negócio de HTTP, Kafka e SQL. Prefira as convenções do Spring e a menor solução que preserve os contratos.
- Valide entradas nas fronteiras. Mantenha configurações específicas do ambiente fora do código e carregue segredos por configuração externa. Defina cada segredo uma única vez no `.env` da raiz e repasse-o aos containers pelo `docker-compose.yml`; configuração que não é segredo fica no próprio `docker-compose.yml`. Use logs estruturados com identificadores úteis e omita credenciais, dados sensíveis e o conteúdo integral dos logs recebidos.

## Padrões de código Java

- Use injeção por construtor e um `Clock` injetável para decisões temporais, como cooldown. Use o mesmo `Instant` na validação e no registro de uma operação.
- Organize os pacotes de cada serviço por papel: `config`, `controller`, `domain` (entidades JPA), `dto`, `event` (contratos Kafka), `consumer` (listeners Kafka), `client` (clientes HTTP externos), `scheduling` (tarefas agendadas), `exception`, `repository` e `service`. Crie apenas os pacotes que o serviço usar.
- Services coordenam regras e transações. Listeners Kafka, controllers e schedulers apenas convertem a entrada e disparam o caso de uso.
- Valide entradas com Jakarta Bean Validation e `@Valid` nos DTOs e eventos recebidos. Trate entradas inválidas com exceções específicas e respostas HTTP consistentes no handler de exceções do serviço. Não capture `NullPointerException` como regra de negócio.
- Nomeie dependências injetadas, campos e variáveis pelo tipo e papel, sem abreviar ou pluralizar: `alertService`, `alertRepository`, `meterRegistry`, `kafkaTemplate`.
- Prefira código declarativo a checagens `if (x == null)`. Use `Optional.ofNullable` com `map`, `flatMap`, `filter`, `orElse`, `orElseGet` e `orElseThrow` com exceção explícita, e streams quando a regra percorre coleções ou cadeias. Não use `Optional.get` sem tratamento nem transforme campos JPA ou parâmetros em `Optional` por estilo.
- Endpoints paginados recebem `@ParameterObject Pageable pageable`, sem `@PageableDefault`, e retornam `ResponseEntity<Page<DTO>>`, mapeando a entidade para o DTO com `Page.map`. Filtros opcionais entram como `@RequestParam(required = false)` validados.
- Não substitua dados obrigatórios ausentes por valores fictícios nem ignore registros inválidos em silêncio. Mantenha opcionais os campos opcionais do contrato.
- Use Lombok para código repetitivo: `@RequiredArgsConstructor` para injeção por construtor e `@Slf4j` para logs. Nas entidades JPA, use `@Getter`, `@NoArgsConstructor(access = AccessLevel.PROTECTED)`, `@AllArgsConstructor` quando houver criação pelo código e `@EqualsAndHashCode(of = "<id>")`. Não use `@Data` nem `@Setter` em entidades. Mantenha construtor explícito quando ele recebe `@Value` ou valida argumentos, e em classes instanciadas fora do Spring que montam estado.
- Concentre contadores do Micrometer em um componente de métricas do serviço, como `AlertMetrics`, e injete esse componente nos services.
- Construtores de entidades apenas atribuem dados. Aproveite o dirty checking do JPA para entidades gerenciadas e evite `save` redundante.
- Trate como duplicidade apenas o conflito da chave esperada. Outras falhas de banco devem propagar. Não capture qualquer `DataIntegrityViolationException` como registro já processado.
- Ao usar MDC em listeners, restaure o contexto anterior ao final, inclusive em erro. Não use `MDC.clear`.

## Migrações de banco de dados

- Mantenha as migrations em `src/main/resources/db/migration` com nomes em inglês no formato `V1__description.sql`. Não edite migrations já aplicadas; crie uma nova versão.
- Use `spring.jpa.hibernate.ddl-auto: validate` na aplicação e nos testes. O Hibernate não cria nem altera o schema.
- Mantenha `clean-disabled: true` e o baseline automático desabilitado. Não use `IF NOT EXISTS` nas migrations iniciais para esconder divergências de schema.
- A conta de leitura do Grafana é criada pelo script de inicialização em `infra/postgres/init`, que só roda com o volume vazio. Ela recebe `SELECT` por default privileges nas tabelas criadas pelo dono do banco, e as sessões dela são somente leitura.

## Testes

- Crie ou atualize testes unitários e de integração quando houver mudança de comportamento. Use sufixo `Tests` para unitários e `IT` para integração.
- Use `TopologyTestDriver` para a topologia do Kafka Streams, `@EmbeddedKafka` para integração com Kafka e Testcontainers com PostgreSQL para persistência.
- Use as migrations reais nos testes de persistência. Em `@DataJpaTest` do Spring Boot 4, importe `FlywayAutoConfiguration` explicitamente.
- Use relógio fixo, cubra ausência de dados e limites de janela e use `flush`/`clear` quando precisar verificar a persistência real.

## Confiabilidade com Kafka

- **Produção.** Use produtor idempotente, `acks=all` e timeout de entrega explícito. Limite o tamanho dos lotes HTTP. Se a publicação falhar no meio do lote, preserve os `logId` no reenvio para deduplicar os registros já aceitos.
- **Idempotência de ponta a ponta.** Projete os consumidores para reentregas. No processador, deduplique por `(serviceName, logId)` com retenção alinhada à janela esperada de retentativas. No serviço de alertas, use unicidade de `alertId` e grave os totais da janela com upsert, sem somar o mesmo snapshot de novo. Confirme o offset após o commit da transação no PostgreSQL. A garantia de processamento do Kafka Streams tem escopo Kafka. PostgreSQL e webhooks precisam de tratamento próprio.
- **Falhas e retentativas.** Retente falhas transitórias com limite e espera progressiva. Encaminhe registros inválidos ou falhas permanentes de um registro a uma DLT quando houver caminho seguro para isolar o problema. Preserve tópico, partição, offset e motivo, respeitando as mesmas regras de proteção de dados do tópico original. Confirme o offset só depois de a DLT aceitar o registro. Se a infraestrutura estiver indisponível, preserve o backlog e sinalize a falha. Avalie o impacto de tópicos de retry na ordem por chave.
- **Partições e janelas.** Preserve a chave definida no contrato e conte com ordem de leitura dentro da partição. Use o horário original do evento para agregação. Defina tolerância para atrasos, registre métricas de eventos descartados após esse prazo e preserve a evolução das contagens por chave.
- **Retenção e replay.** Configure retenção e política de limpeza por tópico. Dimensione `logs.raw` pelo volume e pelo intervalo que precisa ser reprocessado. Defina também retenção dos tópicos de saída e da DLT, se usada. Mantenha o changelog das state stores habilitado. Faça replay histórico com estado e saídas isolados ou com um procedimento explícito de reconstrução. Desative notificações externas durante o replay.
- **Notificações.** Ao implementar webhooks, grave a entrega pendente na mesma transação do incidente e envie em segundo plano. Use uma chave de idempotência quando o destino oferecer suporte. Separe deduplicação de reentrega do cooldown entre incidentes.
- **Verificação.** Cubra os caminhos afetados pela mudança. Para ingestão e processamento, confira lote parcialmente publicado, duplicatas, virada de janela, eventos atrasados e o 51º erro. Para persistência, confira reentrega após o commit e falha antes do commit. Acompanhe lag, erros de publicação, retentativas e volume na DLT quando ela existir.

## Ordem de implementação

1. Subir Kafka local e fazer o coletor publicar em `logs.raw`.
2. Validar as contagens de um minuto e o alerta no 51º erro com um lote pequeno.
3. Persistir métricas e incidentes e ligar o Grafana ao PostgreSQL e ao Prometheus.
4. Rodar uma carga de um milhão de logs e medir o atraso e a recuperação após uma pausa no processador. Reentregar um alerta e conferir que ele aparece uma vez no histórico.
5. Notificar incidentes via webhook com outbox, retentativas, chave de idempotência e cooldown por serviço. Esta é a última etapa do MVP.

Ao implementar uma etapa, preserve os contratos Kafka e mantenha cada serviço responsável pelo próprio estado. Os tópicos `logs.enriched` e `logs.errors` ficam para uma etapa em que houver consumidores para eles.