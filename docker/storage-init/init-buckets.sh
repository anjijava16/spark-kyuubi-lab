#!/bin/bash
# Creates the lab buckets in RustFS and uploads ./jobs and ./data.
# Runs in the official amazon/aws-cli image. Safe to run again and again:
# existing buckets are kept, and `s3 sync` only copies changed files.
set -euo pipefail

EP="${S3_ENDPOINT:-http://rustfs:9000}"
export AWS_ACCESS_KEY_ID="${RUSTFS_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${RUSTFS_SECRET_KEY}"
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""
# Newer AWS CLI versions add CRC checksums to every request by default.
# Many S3-compatible stores don't need them; only send them when required.
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
# http://rustfs:9000/bucket/key, not http://bucket.rustfs:9000/key
aws configure set default.s3.addressing_style path

s3()    { aws --endpoint-url "$EP" s3 "$@"; }
s3api() { aws --endpoint-url "$EP" s3api "$@"; }

echo ">> waiting for RustFS at $EP"
for i in $(seq 1 60); do
  if s3 ls >/dev/null 2>&1; then break; fi
  sleep 2
done
s3 ls >/dev/null   # if still not up, fail here with the real error

for b in jobs data warehouse spark-logs; do
  if s3api head-bucket --bucket "$b" >/dev/null 2>&1; then
    echo ">> bucket $b exists"
  else
    s3 mb "s3://$b"
  fi
done

# s3a sees a "directory" only if an object sits under it; the event-log
# writer refuses to start if its directory isn't there.
echo keep | s3 cp - s3://spark-logs/events/.keep >/dev/null

echo ">> uploading jobs/python -> s3://jobs/python/"
s3 sync /seed/jobs/python s3://jobs/python --exclude "*__pycache__*" --exclude "*.pyc"

if ls /seed/jobs/jars/*.jar >/dev/null 2>&1; then
  echo ">> uploading jobs/jars -> s3://jobs/jars/"
  s3 sync /seed/jobs/jars s3://jobs/jars --exclude ".gitkeep"
else
  echo ">> no jars in ./jobs/jars yet (run: make jars), skipping"
fi

echo ">> uploading data -> s3://data/input/"
s3 sync /seed/data s3://data/input

echo "---- s3://jobs ----"
s3 ls --recursive s3://jobs/
echo "---- s3://data/input ----"
s3 ls --recursive s3://data/input/
echo ">> storage ready"
