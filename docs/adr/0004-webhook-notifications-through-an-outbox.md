# Notificações de webhook saem de um outbox no PostgreSQL

Quando um alerta novo é registrado, o `alert-service` grava a entrega pendente em `webhook_deliveries` na mesma
transação. Um agendador busca as entregas vencidas com `FOR UPDATE SKIP LOCKED`, envia o `POST` e atualiza o status na
mesma transação. Se o serviço cair entre o commit do alerta e o envio, a entrega continua pendente e sai na próxima
execução. Uma reentrega do alerta não cria outra entrega, porque ela só é criada quando o alerta é inserido.

A entrega pode chegar mais de uma vez: se a resposta do destino se perder, o envio é repetido. Por isso cada requisição
leva o cabeçalho `Idempotency-Key` com o `alertId`.

## Retentativas

- Respostas 2xx encerram a entrega como `SENT`.
- Respostas 5xx, 408, 429 e falhas de conexão são retentadas com espera exponencial, a partir de
  `alerts.webhook.initial-backoff` e limitada a `alerts.webhook.max-backoff`, até `alerts.webhook.max-attempts`.
- As demais respostas, inclusive redirecionamentos, marcam a entrega como `FAILED`, porque repetir não muda o resultado.

## Cooldown

O cooldown é separado da deduplicação. A tabela `notification_cooldowns` guarda o último horário de notificação por
serviço, e um único `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE last_notified_at <= início do cooldown` decide e
reserva a notificação de forma atômica, sem corrida entre consumidores. Alertas suprimidos continuam no histórico, só
não geram entrega. O horário usado é o do registro do alerta, vindo do `Clock`.

## Consequências

- O envio acontece com a linha bloqueada, então os timeouts de conexão e leitura precisam ser curtos. Um destino lento
  atrasa o lote, mas não bloqueia o consumo do Kafka.
- `alerts.webhook.enabled` vem desligado por padrão e deve continuar assim em replays. Com ele desligado, alertas não
  criam entregas e o agendador não é registrado.
