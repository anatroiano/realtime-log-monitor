# Arquitetura

## Tópicos Kafka

| Tópico             | Chave                      | Conteúdo                                                                                       |
|--------------------|----------------------------|------------------------------------------------------------------------------------------------|
| `logs.raw`         | `serviceName`              | Log recebido: `logId`, `timestamp`, `serviceName`, `host`, `level`, `message`                  |
| `logs.metrics`     | serviço + início da janela | Totais acumulados da janela: `totalLogs`, `errorCount`                                         |
| `alerts.triggered` | `alertId`                  | Incidente: `alertId`, `serviceName`, `ruleId`, `windowStart`, `windowEnd`, `errorCount`        |
| `*.dlt`            | mesma do tópico de origem  | Registros inválidos ou que falharam de forma permanente, com tópico, partição, offset e motivo |

## Confiabilidade

- **Do envio ao Kafka:** o produtor do coletor é idempotente e usa `acks=all`. Se um lote falhar e for reenviado com os
  mesmos `logId`, o processador descarta os logs que já tinham sido aceitos.
- **No processador:** o Kafka Streams roda com `exactly_once_v2`, e as contagens e a deduplicação ficam em state stores
  com changelog no Kafka. Se o processador parar, ele retoma de onde estava sem perder nem contar duas vezes.
- **No alert-service:**
    - As contagens são gravadas com upsert e só avançam, então um snapshot antigo reentregue não faz a contagem
      regredir.
    - Cada incidente é único pelo `alertId`, então um alerta reentregue não duplica o histórico.
    - O offset do Kafka só é confirmado depois do commit no PostgreSQL.
    - Falhas pontuais vão para a DLT depois de algumas tentativas. Se o banco ficar fora do ar, o consumo pausa em vez
      de descartar mensagens.
- **Webhooks:**
    - A notificação é gravada na mesma transação do incidente (outbox) e enviada em segundo plano.
    - Falhas são retentadas com espera crescente, e cada envio leva o `alertId` no cabeçalho `Idempotency-Key`.
    - Um cooldown por serviço limita notificações seguidas do mesmo serviço.
