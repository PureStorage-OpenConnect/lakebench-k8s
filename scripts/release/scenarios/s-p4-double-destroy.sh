#!/usr/bin/env bash
# S-P4-DoubleDestroy
#
# Two `destroy A --yes` start a second apart. Exactly one deletes the
# namespace; the other converges without touching anything the first has
# removed: it exits 0 having waited for the other's delete, 3 for a
# refusal (destroy.redeployed or lease.held), 1 when the namespace vanished
# under it (a concurrent destroy), or 6 while the namespace is still
# terminating. Run by `harness.py scenario S-P4`.

set -euo pipefail

CFG_A="${LB_CONFIG_A:?LB_CONFIG_A must point to config A}"
LOG_DIR="${LB_UAT_LOG_DIR:?LB_UAT_LOG_DIR must name a log directory}"
mkdir -p "$LOG_DIR"
source "$(dirname "$0")/lib-checks.sh"
OP0=$(operator_restarts)

NS_A=$(cfg_name "$CFG_A")

echo "S-P4: deploying A..."
lb_run "$LOG_DIR/s-p4-deploy.log" deploy --yes "$CFG_A"
[ "$LB_RC" -eq 0 ] || { echo "FAIL: deploy A exited $LB_RC ($LB_PATHS)"; exit 1; }

echo "S-P4: firing two destroys of A in parallel..."
destroy_bg() {  # destroy_bg N
  local rc=0
  LB_EXIT_PATH_FILE="$LOG_DIR/s-p4-destroy-$1.path" lakebench destroy "$CFG_A" --yes \
    >"$LOG_DIR/s-p4-destroy-$1.log" 2>&1 || rc=$?
  echo "$rc" >"$LOG_DIR/s-p4-destroy-$1.rc"
}
destroy_bg 1 &
D1=$!
sleep 1
destroy_bg 2 &
D2=$!
wait $D1
wait $D2

if ns_present "$NS_A"; then
  echo "FAIL: namespace $NS_A still exists after two destroy passes"
  exit 1
fi
if grep -qE 'Traceback|Unhandled|panic' "$LOG_DIR/s-p4-destroy-1.log" "$LOG_DIR/s-p4-destroy-2.log"; then
  echo "FAIL: destroy produced an unhandled error (see $LOG_DIR/s-p4-destroy-*.log)"
  exit 1
fi

assert_buckets "$CFG_A" absent
assert_watched "$NS_A" no
assert_operator_healthy "$OP0"

# The winner exits 0 and logs "Namespace X deleted"; a loser that waited for
# it logs "Namespace X deleted (deletion started by another run)".
winner() { [ "$(cat "$LOG_DIR/s-p4-destroy-$1.rc")" -eq 0 ] && grep "Namespace $NS_A deleted" "$LOG_DIR/s-p4-destroy-$1.log" | grep -qv "started by another run"; }
n_deleted=0
for i in 1 2; do winner "$i" && n_deleted=$((n_deleted + 1)); done
if [ "$n_deleted" -ne 1 ]; then echo "FAIL: $n_deleted destroys claimed the namespace delete (want 1)"; exit 1; fi
for i in 1 2; do
  winner "$i" && continue
  rc=$(cat "$LOG_DIR/s-p4-destroy-$i.rc")
  LB_PATHS=" $(awk '{for (j = 2; j <= NF; j++) if ($j != "-") print $j}' "$LOG_DIR/s-p4-destroy-$i.path" 2>/dev/null | sort -u | tr '\n' ' ')"
  log="$LOG_DIR/s-p4-destroy-$i.log"
  if [ "$rc" -eq 0 ]; then
    grep -q "started by another run" "$log" || { echo "FAIL: destroy $i exited 0 without waiting for the other delete"; exit 1; }
  elif [ "$rc" -eq "$LB_EXIT_REFUSED" ]; then
    has_path destroy.redeployed || has_path lease.held || { echo "FAIL: destroy $i refused with ($LB_PATHS)"; exit 1; }
  elif [ "$rc" -eq "$LB_EXIT_FAILED" ]; then
    grep -qE "concurrent destroy|Stopped before touching buckets" "$log" || { echo "FAIL: destroy $i exited 1 without a concurrent-destroy reason"; exit 1; }
  elif [ "$rc" -eq "$LB_EXIT_INCOMPLETE" ]; then
    has_path destroy.namespace_terminating || { echo "FAIL: destroy $i exited 6 with ($LB_PATHS)"; exit 1; }
  else
    echo "FAIL: destroy $i exited with an unexpected code $rc"
    exit 1
  fi
done

echo "PASS: S-P4 -- double destroy converged cleanly, no unhandled errors"
