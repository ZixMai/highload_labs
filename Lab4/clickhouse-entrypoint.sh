#!/bin/sh
set -eu

/entrypoint.sh "$@" &
clickhouse_pid=$!

forward_shutdown() {
  kill "$clickhouse_pid" 2>/dev/null || true
  wait "$clickhouse_pid" || true
}

trap forward_shutdown INT TERM

echo "Waiting for ClickHouse to become available..."
attempt=0
until clickhouse-client \
  --host 127.0.0.1 \
  --user admin \
  --password "${ADMIN_PASSWORD}" \
  --query "SELECT 1" >/dev/null 2>&1
do
  attempt=$((attempt + 1))
  if ! kill -0 "$clickhouse_pid" 2>/dev/null; then
    wait "$clickhouse_pid"
    exit 1
  fi
  if [ "$attempt" -ge 30 ]; then
    echo "ClickHouse did not become reachable inside the container in time. Last connection attempt:"
    clickhouse-client \
      --host 127.0.0.1 \
      --user admin \
      --password "${ADMIN_PASSWORD}" \
      --query "SELECT 1"
    exit 1
  fi
  sleep 2
done

echo "Applying ClickHouse schema and seed data..."
clickhouse-client \
  --host 127.0.0.1 \
  --user admin \
  --password "${ADMIN_PASSWORD}" \
  --multiquery < /init/clickhouse-init.sql

echo "ClickHouse initialization completed."

wait "$clickhouse_pid"
