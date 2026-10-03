#!/usr/bin/env bash
# S-P2-ConcurrentDeploy
#
# Two deploys of distinct configs starting within 2 seconds must both
# reach infrastructure-ready without either failing on a cluster-scoped
# resource (SecretClass, watch list, bucket tag), and both namespaces must
# carry distinct identity annotations. Run by `harness.py scenario S-P2`.

set -euo pipefail

CFG_A="${LB_CONFIG_A:?LB_CONFIG_A must point to config A}"
CFG_B="${LB_CONFIG_B:?LB_CONFIG_B must point to config B}"
LOG_DIR="${LB_UAT_LOG_DIR:?LB_UAT_LOG_DIR must name a log directory}"
mkdir -p "$LOG_DIR"
source "$(dirname "$0")/lib-checks.sh"

NS_A=$(cfg_name "$CFG_A")
NS_B=$(cfg_name "$CFG_B")

echo "S-P2: starting parallel deploys of $NS_A and $NS_B..."
lakebench deploy --yes "$CFG_A" >"$LOG_DIR/s-p2-a.log" 2>&1 &
DEP_A=$!
sleep 2
lakebench deploy --yes "$CFG_B" >"$LOG_DIR/s-p2-b.log" 2>&1 &
DEP_B=$!

wait $DEP_A || { echo "FAIL: deploy A failed under parallel load"; exit 1; }
wait $DEP_B || { echo "FAIL: deploy B failed under parallel load"; exit 1; }

ann() { kc get ns "$1" -o jsonpath="{.metadata.annotations.lakebench\.deployment/$2}"; }
ANN_A=$(ann "$NS_A" name)
ANN_B=$(ann "$NS_B" name)
FP_A=$(ann "$NS_A" api-server)
FP_B=$(ann "$NS_B" api-server)
NONCE_A=$(ann "$NS_A" deploy-nonce)
NONCE_B=$(ann "$NS_B" deploy-nonce)

if [[ -z "$ANN_A" || -z "$ANN_B" ]]; then
  echo "FAIL: identity annotation missing (A=$ANN_A, B=$ANN_B)"
  exit 1
fi
if [[ "$ANN_A" == "$ANN_B" ]]; then
  echo "FAIL: both namespaces stamped as $ANN_A -- rename collision"
  exit 1
fi
if [[ "$FP_A" != "$FP_B" ]]; then
  echo "FAIL: same cluster, different api-server fingerprints ($FP_A vs $FP_B)"
  exit 1
fi
if [[ -z "$NONCE_A" || -z "$NONCE_B" || "$NONCE_A" == "$NONCE_B" ]]; then
  echo "FAIL: deploy nonces missing or equal (A=$NONCE_A, B=$NONCE_B)"
  exit 1
fi

SC_A=$(kc get secretclass "lakebench-s3-credentials-$NS_A" -o name 2>/dev/null || true)
SC_B=$(kc get secretclass "lakebench-s3-credentials-$NS_B" -o name 2>/dev/null || true)
if [[ -z "$SC_A" || -z "$SC_B" ]]; then
  echo "FAIL: expected distinct SecretClasses per namespace (A=$SC_A, B=$SC_B)"
  exit 1
fi
assert_watched "$NS_A" yes
assert_watched "$NS_B" yes
assert_buckets "$CFG_A" present
assert_buckets "$CFG_B" present

echo "PASS: S-P2 -- both deploys reached ready, distinct identities and distinct SecretClasses"
