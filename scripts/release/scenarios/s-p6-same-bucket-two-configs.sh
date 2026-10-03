#!/usr/bin/env bash
# S-P6-SameBucketNameDifferentDeployments
#
# Two configs with distinct names name the same bronze bucket
# (LB_SHARED_BUCKET, absent before the scenario). A deploys and claims it;
# B's deploy must refuse (exit 3, deploy.identity_foreign) and leave the
# bucket A's, so two deployments never share and corrupt one bucket. Run by
# `harness.py scenario S-P6`, which then destroys B before A.

set -euo pipefail

CFG_A="${LB_CONFIG_A:?LB_CONFIG_A must reference LB_SHARED_BUCKET}"
CFG_B="${LB_CONFIG_B:?LB_CONFIG_B must reference LB_SHARED_BUCKET with a different name}"
SHARED="${LB_SHARED_BUCKET:?LB_SHARED_BUCKET must name the shared bronze bucket}"
LOG_DIR="${LB_UAT_LOG_DIR:?LB_UAT_LOG_DIR must name a log directory}"
mkdir -p "$LOG_DIR"
source "$(dirname "$0")/lib-checks.sh"

NS_A=$(cfg_name "$CFG_A")
NS_B=$(cfg_name "$CFG_B")
if [[ "$NS_A" == "$NS_B" ]]; then
  echo "SETUP FAILURE: A and B share the same name; the scenario needs distinct names"
  exit 2
fi

echo "S-P6: deploy A first, claiming $SHARED as $NS_A..."
lb_run "$LOG_DIR/s-p6-a-deploy.log" deploy --yes "$CFG_A"
[ "$LB_RC" -eq 0 ] || { echo "FAIL: deploy A exited $LB_RC ($LB_PATHS)"; exit 1; }
[ "$(bucket_owner "$CFG_A" "$SHARED")" = "$NS_A" ] || { echo "FAIL: $SHARED is not tagged $NS_A after A's deploy"; exit 1; }

echo "S-P6: deploy B on the same bucket must refuse..."
lb_run "$LOG_DIR/s-p6-b-deploy.log" deploy --yes "$CFG_B"
if [ "$LB_RC" -ne "$LB_EXIT_REFUSED" ] || ! has_path deploy.identity_foreign; then
  echo "FAIL: B deploy exited $LB_RC ($LB_PATHS); want a refusal (deploy.identity_foreign)"
  exit 1
fi
[ "$(bucket_owner "$CFG_A" "$SHARED")" = "$NS_A" ] || { echo "FAIL: B's deploy changed the owner of $SHARED"; exit 1; }
ns_present "$NS_A" || { echo "FAIL: A's namespace is gone"; exit 1; }

echo "PASS: S-P6 -- the second deploy refused the bucket A owns and left it A's"
