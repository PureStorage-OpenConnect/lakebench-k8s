#!/usr/bin/env bash
# S-P5-LegacyBucket
#
# A bucket that exists without a lakebench ownership tag (LB_LEGACY_BUCKET:
# a legacy or foreign bucket; the harness creates it with one object) is
# A's bronze bucket. Deploy must refuse to claim it (exit 3,
# deploy.identity_foreign). Destroy of A then removes A's namespace and the
# buckets A created, and refuses and leaves the legacy bucket as it was
# (exit 3, deploy.identity_foreign): never emptied, deleted or tagged.
# Run by `harness.py scenario S-P5`, which deletes the legacy bucket it
# created afterwards.

set -euo pipefail

CFG_A="${LB_CONFIG_A:?LB_CONFIG_A must point at config A with LB_LEGACY_BUCKET as its bronze bucket}"
LEGACY_BUCKET="${LB_LEGACY_BUCKET:?LB_LEGACY_BUCKET must name a pre-existing untagged bucket}"
LOG_DIR="${LB_UAT_LOG_DIR:?LB_UAT_LOG_DIR must name a log directory}"
mkdir -p "$LOG_DIR"
source "$(dirname "$0")/lib-checks.sh"
NS_A=$(cfg_name "$CFG_A")

BEFORE=$(bucket_objects "$CFG_A" "$LEGACY_BUCKET")
if [ "$BEFORE" -lt 1 ] || [ "$(bucket_owner "$CFG_A" "$LEGACY_BUCKET")" != "-" ]; then
  echo "SETUP FAILURE: $LEGACY_BUCKET must hold an object and carry no lakebench tag"
  exit 2
fi

echo "S-P5: deploy A over the untagged $LEGACY_BUCKET must refuse..."
lb_run "$LOG_DIR/s-p5-deploy.log" deploy --yes "$CFG_A"
if [ "$LB_RC" -ne "$LB_EXIT_REFUSED" ] || ! has_path deploy.identity_foreign; then
  echo "FAIL: deploy over an untagged bucket exited $LB_RC ($LB_PATHS), not a refusal"
  exit 1
fi
[ "$(bucket_objects "$CFG_A" "$LEGACY_BUCKET")" -eq "$BEFORE" ] || { echo "FAIL: deploy changed $LEGACY_BUCKET"; exit 1; }
[ "$(bucket_owner "$CFG_A" "$LEGACY_BUCKET")" = "-" ] || { echo "FAIL: deploy tagged $LEGACY_BUCKET"; exit 1; }

echo "S-P5: destroy A must remove A and refuse the legacy bucket..."
lb_run "$LOG_DIR/s-p5-destroy.log" destroy --yes "$CFG_A"
if [ "$LB_RC" -ne "$LB_EXIT_REFUSED" ] || ! has_path deploy.identity_foreign; then
  echo "FAIL: destroy exited $LB_RC ($LB_PATHS); want a refusal of the legacy bucket"
  exit 1
fi
[ "$(bucket_objects "$CFG_A" "$LEGACY_BUCKET")" -eq "$BEFORE" ] || { echo "FAIL: destroy emptied $LEGACY_BUCKET"; exit 1; }
[ "$(bucket_owner "$CFG_A" "$LEGACY_BUCKET")" = "-" ] || { echo "FAIL: destroy tagged $LEGACY_BUCKET"; exit 1; }
if ns_present "$NS_A"; then echo "FAIL: A namespace still present after destroy"; exit 1; fi

echo "PASS: S-P5 -- the untagged bucket was refused at deploy and at destroy and left as it was"
