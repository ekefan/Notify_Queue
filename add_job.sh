#!/usr/bin/env bash

set -uo pipefail

if [[ $# -ne 1 || ! $1 =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: $0 <number-of-jobs>" >&2
  echo "Example: $0 25" >&2
  exit 2
fi

job_count=$1
api_url=${API_URL:-http://localhost:8000}
batch_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
send_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
created=0

for ((job_number = 1; job_number <= job_count; job_number++)); do
  idempotency_key="add-job-${batch_id}-${job_number}"

  if curl --fail --silent --show-error \
    --request POST "${api_url%/}/jobs" \
    --header 'Content-Type: application/json' \
    --header "Idempotency-Key: ${idempotency_key}" \
    --data "{\"recipient\":\"user${job_number}@example.com\",\"channel\":\"email\",\"payload\":{\"subject\":\"Notify Queue test\",\"body\":\"Generated job ${job_number}\"},\"send_at\":\"${send_at}\",\"priority\":1}" \
    >/dev/null; then
    ((created += 1))
    printf '\rCreated %d/%d jobs' "$created" "$job_count"
  else
    printf '\nFailed while creating job %d. Created %d/%d jobs.\n' \
      "$job_number" "$created" "$job_count" >&2
    exit 1
  fi
done

printf '\nDone. The publisher can now pick up %d due jobs.\n' "$created"
