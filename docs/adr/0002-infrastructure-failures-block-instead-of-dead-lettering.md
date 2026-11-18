# Falhas de infraestrutura bloqueiam a partição em vez de ir para a DLT

O `alert-service` separa falhas do registro de falhas de infraestrutura. Registros que não convertem, não passam no Bean
Validation ou continuam falhando após 4 retentativas com espera progressiva seguem para `<tópico>.dlt`, com tópico,
partição, offset e exceção originais nos cabeçalhos. O offset só é confirmado depois que a DLT aceita o registro. Falhas
de banco indisponível ou transitório (`TransientDataAccessException`, `DataAccessResourceFailureException`,
`RecoverableDataAccessException`, `CannotCreateTransactionException`) são retentadas sem limite, com espera de até 30 s,
para preservar o backlog no Kafka em vez de esvaziá-lo na DLT durante uma queda do PostgreSQL.

## Consequências

- Durante uma queda do banco, as partições afetadas param e o lag cresce. O painel operacional mostra isso pelo lag do
  consumidor e pelas falhas do listener.
- Não usamos tópicos de retry, para não quebrar a ordem por chave dos snapshots de janela.
