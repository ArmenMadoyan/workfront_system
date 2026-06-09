#!/usr/bin/env bash
# Snapshot of the pipeline's health under load.
set -e
DB="${DATABASE_URL:-postgresql://amadoyan@localhost:5432/workfront_core}"
PSQL_DB="${DB#*//}"; PSQL_DB="postgresql://${PSQL_DB}"

echo "=== kafka consumer lag (worker-fleet) ==="
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group worker-fleet 2>/dev/null | awk 'NR==1 || $5+0>0 {print $1,$3,$5,$6}' | column -t || echo "(group not active)"

echo "=== replication slot lag ==="
psql "$PSQL_DB" -c "select slot_name, active,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as behind
  from pg_replication_slots;" 2>&1

echo "=== outbox backlog (rows not yet pruned) ==="
psql "$PSQL_DB" -tc "select event_type, count(*) from outbox group by event_type;" 2>&1

echo "=== top app processes ==="
ps -Ao pcpu,comm | grep -E "python|postgres" | sort -rn | head -6