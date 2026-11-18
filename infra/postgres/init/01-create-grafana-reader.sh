#!/bin/sh
set -eu

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=owner="$POSTGRES_USER" \
  --set=database="$POSTGRES_DB" \
  --set=reader="$GRAFANA_DB_USER" \
  --set=reader_password="$GRAFANA_DB_PASSWORD" <<'SQL'
CREATE ROLE :"reader" LOGIN PASSWORD :'reader_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;
ALTER ROLE :"reader" SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE :"database" TO :"reader";
GRANT USAGE ON SCHEMA public TO :"reader";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA public GRANT SELECT ON TABLES TO :"reader";
SQL
