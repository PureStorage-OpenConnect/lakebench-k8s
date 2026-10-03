#!/usr/bin/env bash
# S-P3-DestroyDuringGenerate
#
# A reaches its datagen step; while A is still generating, destroy B.
# Assert A finishes cleanly and B's destroy touches none of A's namespaced
# resources or A's buckets, then destroy A with data in it. Run by
# `harness.py scenario S-P3`.

set -euo pipefail

CFG_A="${LB_CONFIG_A:?LB_CONFIG_A must point to config A}"
CFG_B="${LB_CONFIG_B:?LB_CONFIG_B must point to config B}"
LOG_DIR="${LB_UAT_LOG_DIR:?LB_UAT_LOG_DIR must name a log directory}"
mkdir -p "$LOG_DIR"
source "$(dirname "$0")/lib-checks.sh"
OP0=$(operator_restarts)

NS_A=$(cfg_name "$CFG_A")
NS_B=$(cfg_name "$CFG_B")

echo "S-P3: deploying A and B in parallel..."
lakebench deploy --yes "$CFG_A" >"$LOG_DIR/s-p3-a-deploy.log" 2>&1 &
DEP_A=$!
lakebench deploy --yes "$CFG_B" >"$LOG_DIR/s-p3-b-deploy.log" 2>&1 &
DEP_B=$!
wait $DEP_A || { echo "FAIL: deploy A exited non-zero"; exit 1; }
wait $DEP_B || { echo "FAIL: deploy B exited non-zero"; exit 1; }

echo "S-P3: launching A generate in background..."
lakebench generate --yes "$CFG_A" --timeout 1200 >"$LOG_DIR/s-p3-a-generate.log" 2>&1 &
GEN_A=$!

# Wait until A's datagen Job runs and has written objects (at most 10 min).
for _ in $(seq 1 60); do
  if kc get job lakebench-datagen -n "$NS_A" >/dev/null 2>&1 && [ -n "$(datagen_keys "$CFG_A" | head -n 1)" ]; then break; fi
  kill -0 $GEN_A 2>/dev/null || break
  sleep 10
done

datagen_keys "$CFG_A" >"$LOG_DIR/s-p3-a-keys.before"

echo "S-P3: destroying B while A is generating..."
lb_run "$LOG_DIR/s-p3-b-destroy.log" destroy "$CFG_B" --yes
[ "$LB_RC" -eq 0 ] || { echo "FAIL: destroy B exited $LB_RC ($LB_PATHS)"; exit 1; }

# A must still be running -- its datagen Job is not gone.
if ! kc get job lakebench-datagen -n "$NS_A" >/dev/null 2>&1; then
  echo "FAIL: A's datagen job vanished after destroying B -- cross-deploy leak"
  exit 1
fi

echo "S-P3: waiting for A generate to finish..."
if ! wait $GEN_A; then
  echo "FAIL: A generate did not exit cleanly after B destroy"
  exit 1
fi

if ns_present "$NS_B"; then
  echo "FAIL: B namespace still present after destroy"
  exit 1
fi

# Post-conditions: B fully gone, A untouched, operator healthy.
grep -q "Namespace $NS_B deleted" "$LOG_DIR/s-p3-b-destroy.log" || { echo "FAIL: B destroy log lacks 'Namespace $NS_B deleted'"; exit 1; }
assert_buckets "$CFG_B" absent
assert_buckets "$CFG_A" present
if [ -s "$LOG_DIR/s-p3-a-keys.before" ]; then
  assert_keys_kept "$CFG_A" "$LOG_DIR/s-p3-a-keys.before"
else
  echo "  A had written no datagen object before B's destroy; nothing to compare"
fi
assert_watched "$NS_A" yes
assert_watched "$NS_B" no
assert_operator_healthy "$OP0"

echo "S-P3: destroying A after generate (a deployment with data)..."
lb_run "$LOG_DIR/s-p3-a-destroy.log" destroy "$CFG_A" --yes
[ "$LB_RC" -eq 0 ] || { echo "FAIL: A destroy exited $LB_RC ($LB_PATHS)"; exit 1; }
if ns_present "$NS_A"; then echo "FAIL: A namespace still present"; exit 1; fi
assert_buckets "$CFG_A" absent
assert_watched "$NS_A" no
assert_operator_healthy "$OP0"
if grep -qE 'Traceback|Unhandled' "$LOG_DIR"/s-p3-*destroy.log; then echo "FAIL: traceback in destroy logs"; exit 1; fi

echo "PASS: S-P3 -- A finished generating clean while B was destroyed; both torn down fully"
