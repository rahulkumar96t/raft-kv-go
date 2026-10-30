#!/usr/bin/env bash
# Start, inspect and stop a local 3-node raftkv cluster.
#   scripts/cluster.sh start | status | kill <id> | restart <id> | stop | clean
# Extra node flags can be passed in RAFTKV_ARGS, e.g. RAFTKV_ARGS="--snapshot-bytes 4096".
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
RUN_DIR="${RUN_DIR:-data}"
BIN="bin/raftkv"

start_node() {
  local id=$1
  # shellcheck disable=SC2086 # RAFTKV_ARGS is intentionally word-split
  "$BIN" --id "$id" --cluster "$CLUSTER" --data "$RUN_DIR/node-$id" ${RAFTKV_ARGS:-} >>"$RUN_DIR/node-$id.log" 2>&1 &
  echo $! >"$RUN_DIR/node-$id.pid"
  echo "node $id: pid $!, http://127.0.0.1:700$id, log $RUN_DIR/node-$id.log"
}

stop_node() {
  local pidfile="$RUN_DIR/node-$1.pid"
  if [[ -f "$pidfile" ]]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
    echo "node $1 stopped"
  fi
}

case "${1:-start}" in
  start)
    mkdir -p bin "$RUN_DIR"
    go build -o "$BIN" ./cmd/raftkv
    for id in 1 2 3; do start_node "$id"; done
    ;;
  status)
    for id in 1 2 3; do
      printf 'node %s: ' "$id"
      curl -fsS "http://127.0.0.1:700$id/status" 2>/dev/null || echo "down"
    done
    ;;
  kill) stop_node "${2:?node id}" ;;
  restart) start_node "${2:?node id}" ;;
  stop) for id in 1 2 3; do stop_node "$id"; done ;;
  clean)
    for id in 1 2 3; do stop_node "$id"; done
    rm -rf "$RUN_DIR"
    ;;
  *)
    echo "usage: $0 start|status|kill <id>|restart <id>|stop|clean" >&2
    exit 2
    ;;
esac
