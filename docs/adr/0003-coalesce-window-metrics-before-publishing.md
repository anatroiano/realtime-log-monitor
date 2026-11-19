# Snapshots de janela são agrupados antes da publicação

O processador publicava em `logs.metrics` um snapshot a cada log aceito. Na carga de um milhão de logs, isso gerou um
milhão de upserts no `alert-service`, que grava cada snapshot em uma transação própria e ficou limitado a cerca de 330
gravações por segundo. Quase todos esses snapshots ficavam obsoletos logo depois, substituídos pelo seguinte da mesma
janela.

O processador passa a guardar o snapshot mais recente de cada serviço e janela na state store `pending-metrics` e a
publicá-los a cada `processor.metrics-flush-interval` (1 s por padrão), usando uma pontuação por tempo de relógio. A
store tem changelog, então snapshots pendentes sobrevivem a uma reinicialização. Os alertas continuam saindo de cada
atualização da agregação, sem esse agrupamento, para registrar a contagem exata do 51º erro.

O cache de registros do Kafka Streams faria um agrupamento parecido, mas descartaria a atualização em que o limite é
atingido, e o seu efeito não aparece no `TopologyTestDriver`, que esvazia o cache a cada registro.

## Consequências

- O volume de `logs.metrics` passa a depender do número de janelas ativas por intervalo, e não do número de logs.
- A contagem em `window_counts` e no Grafana pode atrasar até um intervalo em relação ao alerta.
- O contrato não muda: snapshots continuam sendo totais acumulados, e `total_logs` continua crescendo entre publicações
  da mesma janela, como a ADR 0001 pressupõe.
