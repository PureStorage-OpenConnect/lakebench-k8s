#!/usr/bin/env bash
# Shared checks for the parallel-safety scenarios S-P1 to S-P6.
#
# Run them through `scripts/release/harness.py scenario S-Pn`, which writes
# the configs and their ledger rows, exports LB_CONFIG_A / LB_CONFIG_B,
# LB_UAT_LOG_DIR, LB_KUBE_CONTEXT and LB_EXIT_<NAME> (the release tree's exit
# codes), puts a `lakebench` shim for the release tree first on PATH, and
# destroys whatever the script leaves behind by incarnation afterwards.
#
# Every check here reads. Bucket checks read the config's S3 settings with
# ${VAR} expanded from the environment inside python and never print a
# credential.

: "${LB_KUBE_CONTEXT:?LB_KUBE_CONTEXT must name the kube context of the configs}"
: "${LB_EXIT_REFUSED:?LB_EXIT_* must come from the harness (release tree exit codes)}"

kc() { kubectl --context "$LB_KUBE_CONTEXT" "$@"; }
hm() { helm --kube-context "$LB_KUBE_CONTEXT" "$@"; }

cfg_name() { python3.11 -c "import sys,yaml; print(yaml.safe_load(open(sys.argv[1]))['name'])" "$1"; }

# lb_run LOG ARGS... runs `lakebench ARGS...`, output to LOG; sets LB_RC to
# its exit code and LB_PATHS to the exit paths it named (LB_EXIT_PATH_FILE).
lb_run() {
  local log=$1 pf
  shift
  pf=$(mktemp "${LB_UAT_LOG_DIR:-.}/exit-path.XXXXXX")
  LB_RC=0
  LB_EXIT_PATH_FILE="$pf" lakebench "$@" >"$log" 2>&1 || LB_RC=$?
  LB_PATHS=" $(awk '{for (i = 2; i <= NF; i++) if ($i != "-") print $i}' "$pf" | sort -u | tr '\n' ' ')"
  rm -f "$pf"
}

has_path() { [[ "$LB_PATHS" == *" $1 "* ]]; }

_s3_py() {  # _s3_py CFG PYTHON_BODY ARGS... (body sees s3 and argv)
  local cfg=$1 body=$2
  shift 2
  python3.11 -c "
import os, sys, yaml, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
c = yaml.safe_load(open(sys.argv[1]))['platform']['storage']['s3']
key, secret = (os.path.expandvars(c[k]) for k in ('access_key', 'secret_key'))
if '\${' in key + secret:
    sys.exit('S3 credential variables are unset')
s3 = boto3.client('s3', endpoint_url=os.path.expandvars(c['endpoint']),
                  region_name=c.get('region', 'us-east-1'),
                  aws_access_key_id=key, aws_secret_access_key=secret,
                  config=Config(s3={'addressing_style': 'path'}))
argv = sys.argv[2:]
$body
" "$cfg" "$@"
}

# bucket_names CFG -> the bronze, silver and gold bucket names, one per line
bucket_names() {
  python3.11 -c "
import sys, yaml
c = yaml.safe_load(open(sys.argv[1]))
b = ((c.get('platform') or {}).get('storage') or {}).get('s3', {}).get('buckets') or {}
for k in ('bronze', 'silver', 'gold'):
    print(b.get(k) or f\"{c['name']}-{k}\")
" "$1"
}

# bucket_state CFG -> "<bucket> present|absent" per bucket of the config
bucket_state() {
  local b
  for b in $(bucket_names "$1"); do
    _s3_py "$1" "
try:
    s3.head_bucket(Bucket=argv[0]); print(argv[0], 'present')
except ClientError as e:
    code = e.response.get('Error', {}).get('Code', '')
    print(argv[0], 'absent' if code in ('404', 'NoSuchBucket') else f'error:{code}')
" "$b"
  done
}

assert_buckets() {  # assert_buckets CFG present|absent
  local out bad
  out=$(bucket_state "$1")
  echo "$out" | sed 's/^/  bucket: /'
  bad=$(echo "$out" | awk -v want="$2" '$2 != want')
  if [ -n "$bad" ]; then
    echo "FAIL: expected buckets $2 for $1"
    return 1
  fi
}

# bucket_objects CFG BUCKET -> number of objects in BUCKET (credentials from CFG)
bucket_objects() {
  _s3_py "$1" "
n = 0
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=argv[0]):
    n += page.get('KeyCount', 0)
print(n)
" "$2"
}

# bucket_owner CFG BUCKET -> the lakebench deployment-name tag, or "-" when untagged
bucket_owner() {
  _s3_py "$1" "
try:
    tags = s3.get_bucket_tagging(Bucket=argv[0]).get('TagSet', [])
except ClientError:
    tags = []
owner = [t['Value'] for t in tags if t['Key'] == 'lakebench.deployment']
print(owner[0] if owner else '-')
" "$2"
}

watch_list() {
  hm get values spark-operator -n spark-operator -a -o json |
    python3.11 -c "import json,sys; v=json.load(sys.stdin); print(' '.join(v.get('spark',{}).get('jobNamespaces',[]) or []))"
}

assert_watched() {  # assert_watched NS yes|no
  local wl
  wl=" $(watch_list) "
  if [ "$2" = yes ] && [[ "$wl" != *" $1 "* ]]; then echo "FAIL: $1 missing from spark.jobNamespaces"; return 1; fi
  if [ "$2" = no ] && [[ "$wl" == *" $1 "* ]]; then echo "FAIL: $1 still in spark.jobNamespaces"; return 1; fi
  echo "  watch list ok: $1 watched=$2"
}

operator_restarts() {
  kc get pods -n spark-operator -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.restartCount}{"\n"}{end}{end}' | awk '{s+=$1} END {print s+0}'
}

assert_operator_healthy() {  # assert_operator_healthy BASELINE_RESTARTS
  local now
  now=$(operator_restarts)
  if kc get pods -n spark-operator --no-headers | grep -qE 'CrashLoopBackOff|Error'; then
    echo "FAIL: spark-operator pod unhealthy"
    return 1
  fi
  if [ "$now" -gt "$1" ]; then echo "FAIL: spark-operator restarts $1 -> $now"; return 1; fi
  echo "  operator healthy (restarts $now)"
}

ns_present() { kc get ns "$1" >/dev/null 2>&1; }
