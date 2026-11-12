#!/usr/bin/env bash
# Create and start a local equivalent of infra/compose.yml's PostgreSQL service.
# Usage: tools/init-local-postgres.sh [data-directory]
# Optional environment: PGPORT (default: 5432).
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
PG_BIN=/opt/homebrew/opt/postgresql@18/bin
DATA_DIR=${1:-"$HOME/.local/share/ergo/postgres18"}
PORT=${PGPORT:-5432}

if (( $# > 1 )); then
    echo "Usage: $0 [data-directory]" >&2
    exit 1
fi
if [[ $EUID -eq 0 ]]; then
    echo "Run this script as your normal user, not root." >&2
    exit 1
fi
for tool in initdb pg_ctl psql pg_config; do
    if [[ ! -x "$PG_BIN/$tool" ]]; then
        echo "Missing $PG_BIN/$tool; install postgresql@18 with Homebrew." >&2
        exit 1
    fi
done
if [[ -e "$DATA_DIR" ]] && [[ ! -d "$DATA_DIR" || -n "$(ls -A -- "$DATA_DIR")" ]]; then
    echo "Refusing to overwrite nonempty data directory: $DATA_DIR" >&2
    exit 1
fi

# The init SQL creates a logical slot using this output plugin. Build outside
# the source tree so Docker/Linux build artifacts cannot be reused on macOS.
BUILD_DIR=$(mktemp -d)
STARTED=0
cleanup() {
    status=$?
    trap - EXIT
    if (( status != 0 && STARTED )); then
        "$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast -w stop || true
    fi
    rm -rf -- "$BUILD_DIR"
    exit "$status"
}
trap cleanup EXIT
cp "$ROOT/ergo_pgoutput/Makefile" "$ROOT/ergo_pgoutput/ergo_pgoutput.c" "$BUILD_DIR/"
make -C "$BUILD_DIR" PG_CONFIG="$PG_BIN/pg_config"
make -C "$BUILD_DIR" PG_CONFIG="$PG_BIN/pg_config" install

mkdir -p -- "$DATA_DIR"
DATA_DIR=$(cd -- "$DATA_DIR" && pwd)
"$PG_BIN/initdb" -D "$DATA_DIR" -U postgres --encoding=UTF8 \
    --locale=en_US.UTF-8 --auth-local=trust --auth-host=trust

cp "$ROOT/infra/postgres/postgresql.conf" "$DATA_DIR/postgresql.conf"
cp "$ROOT/infra/postgres/pg_hba.conf" "$DATA_DIR/pg_hba.conf"
mkdir -p "$DATA_DIR/certs" "$DATA_DIR/socket"
cp "$ROOT/infra/postgres/certs/"{server.crt,server.key,ca.crt} "$DATA_DIR/certs/"
chmod 600 "$DATA_DIR/certs/server.key"

# Escape paths for PostgreSQL configuration string literals.
CONFIG_DIR=${DATA_DIR//\'/\'\'}
cat >> "$DATA_DIR/postgresql.conf" <<EOF

# Local equivalents of the compose service's command-line settings.
wal_level = logical
ssl = on
ssl_cert_file = '$CONFIG_DIR/certs/server.crt'
ssl_key_file = '$CONFIG_DIR/certs/server.key'
ssl_ca_file = '$CONFIG_DIR/certs/ca.crt'
unix_socket_directories = '$CONFIG_DIR/socket'
EOF

"$PG_BIN/pg_ctl" -D "$DATA_DIR" -l "$DATA_DIR/server.log" \
    -o "-p $PORT" -w start
STARTED=1
"$PG_BIN/psql" -X -h "$DATA_DIR/socket" -p "$PORT" -U postgres -d postgres \
    -v ON_ERROR_STOP=1 -f "$ROOT/infra/postgres/init-scripts/init.sql"
"$PG_BIN/pg_ctl" -D "$DATA_DIR" -m fast -w stop

printf '\nPostgreSQL initialized and stopped\n' "$PORT"
