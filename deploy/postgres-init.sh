#!/bin/sh
# Runs once, on first boot of an empty data directory. POSTGRES_DB created
# `postgres`; the two service databases are created here so that no service can
# reach the other's tables even by accident (spec D3).
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
    CREATE DATABASE oe_accounts;
    CREATE DATABASE oe_notifications;
SQL
