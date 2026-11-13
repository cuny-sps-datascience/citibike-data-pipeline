#!/usr/bin/env bash
# Usage: remove-team.sh team-3 [...]
set -euo pipefail
for team in "$@"; do
  for key_id in $(aws iam list-access-keys --user-name "$team" --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
    aws iam delete-access-key --user-name "$team" --access-key-id "$key_id"
  done
  aws iam remove-user-from-group --group-name citibike-contributors --user-name "$team"
  aws iam delete-user --user-name "$team"
  databricks secrets delete-scope "${team}-aws"
done
