#!/bin/bash
# 03_metabase_restore.sh
set -e

echo ">>> Restoring metabase app db..."
pg_restore \
  -U "$POSTGRES_USER" \
  -d postgres \
  -C --no-owner --no-acl \
  /docker-entrypoint-initdb.d/metabase_app.dump

echo ">>> All done."
