# Snapshots de janela só avançam a contagem

O processador publica em `logs.metrics` totais acumulados por serviço e janela, um snapshot a cada log aceito. O
`alert-service` grava esses totais com
`INSERT ... ON CONFLICT (service_name, window_start) DO UPDATE ... WHERE EXCLUDED.total_logs > window_counts.total_logs`:
o snapshot substitui o anterior, nunca é somado, e só é aplicado quando tem mais logs que o registro atual. Assim, a
reentrega de um snapshot antigo depois de um mais novo não faz a contagem regredir, e um snapshot repetido não altera a
linha.

## Consequências

- `total_logs` cresce estritamente dentro de uma janela no processador, então serve como versão do snapshot. Se essa
  regra mudar no processador, este critério precisa mudar junto.
- Um replay com estado isolado volta a emitir contagens a partir de 1 e não sobrescreve os totais já gravados. Para
  reconstruir as contagens a partir de um replay, apague as linhas das janelas afetadas antes de reprocessar.
