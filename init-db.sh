#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE superset;
    GRANT ALL PRIVILEGES ON DATABASE superset TO $POSTGRES_USER;
EOSQL
