#!/usr/bin/env bash
# S-P1-DestroyRunning
#
# Invariant: destroying deployment A does not affect deployment B running
# in parallel. Deploy both, start B's pipeline, destroy A once B has a Spark
# application, and prove B still ships a record with
# pipeline_benchmark.success true and scale_ratio between 0.95 and 1.10, and
# that none of B's generated objects went. Run by `harness.py scenario S-P1`.

set -euo pipefail

CFG_A="${LB_CONFIG_A:?LB_CONFIG_A must point to config file A}"
CFG_B="${LB_CONFIG_B:?LB_CONFIG_B must point to config file B}"
LOG_DIR="${LB_UAT_LOG_DIR:?LB_UAT_LOG_DIR must name a log directory}"
mkdir -p "$LOG_DIR"
source "$(dirname "$0")/lib-checks.sh"
NS_A=$(cfg_name "$CFG_A")

echo "S-P1: deploying A ($CFG_A) and B ($CFG_B) in parallel..."
lakebench deploy --yes "$CFG_A" >"$LOG_DIR/s-p1-a-deploy.log" 2>&1 &
DEPLOY_A=$!
lakebench deploy --yes "$CFG_B" >"$LOG_DIR/s-p1-b-deploy.log" 2>&1 &
DEPLOY_B=$!
wait $DEPLOY_A || { echo "FAIL: deploy A exited non-zero"; exit 1; }
wait $DEPLOY_B || { echo "FAIL: deploy B exited non-zero"; exit 1; }

echo "S-P1: generating on B..."
lb_run "$LOG_DIR/s-p1-b-generate.log" generate --yes "$CFG_B" --timeout 900
[ "$LB_RC" -eq 0 ] || { echo "FAIL: generate B exited $LB_RC"; exit 1; }

echo "S-P1: launching B pipeline in background..."
lakebench run --yes "$CFG_B" --timeout 1800 >"$LOG_DIR/s-p1-b-run.log" 2>&1 &
RUN_B=$!

# Destroy A only once B has a Spark application running (at most 15 min).
NS_B=$(cfg_name "$CFG_B")
for _ in $(seq 1 90); do
  if kc get sparkapplications -n "$NS_B" -o name 2>/dev/null | grep -q .; then break; fi
  kill -0 $RUN_B 2>/dev/null || break
  sleep 10
done
kc get sparkapplications -n "$NS_B" -o name 2>/dev/null | grep -q . || { echo "FAIL: B never started a Spark application"; exit 1; }
datagen_keys "$CFG_B" >"$LOG_DIR/s-p1-b-keys.before"

echo "S-P1: destroying A while B is running..."
lb_run "$LOG_DIR/s-p1-a-destroy.log" destroy "$CFG_A" --yes
[ "$LB_RC" -eq 0 ] || { echo "FAIL: destroy A exited $LB_RC ($LB_PATHS)"; exit 1; }
if ns_present "$NS_A"; then echo "FAIL: A namespace still present after destroy"; exit 1; fi

echo "S-P1: waiting for B's pipeline to finish..."
if ! wait $RUN_B; then
  echo "FAIL: B's pipeline did not exit cleanly"
  exit 1
fi

METRICS_B=$(ls -t lakebench-output/runs/ | head -n 1)
VERDICT=$(python3.11 -c "
import json, sys
pb = json.load(open(sys.argv[1])).get('pipeline_benchmark') or {}
ratio = (pb.get('scores') or {}).get('scale_ratio')
ok = pb.get('success') is True and isinstance(ratio, (int, float)) and 0.95 <= ratio <= 1.10
print(('ok' if ok else 'bad'), pb.get('success'), ratio)
" "lakebench-output/runs/$METRICS_B/metrics.json")
read -r OK SUCCESS SCALE_RATIO <<<"$VERDICT"
if [[ "$OK" != "ok" ]]; then
  echo "FAIL: B record success=$SUCCESS scale_ratio=$SCALE_RATIO (want success, 0.95 to 1.10)"
  exit 1
fi
assert_buckets "$CFG_B" present
assert_keys_kept "$CFG_B" "$LOG_DIR/s-p1-b-keys.before"

echo "PASS: S-P1 -- B finished with success=$SUCCESS scale_ratio=$SCALE_RATIO after A destroyed mid-run"
