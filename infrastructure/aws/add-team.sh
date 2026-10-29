#!/usr/bin/env bash
# Usage: add-team.sh KEYS_FILE team-demo [team-1 ...]
# Creates each team's IAM user and key. The key goes into the Databricks scope <team>-aws
# (READ for <team>-sp) and is appended to KEYS_FILE; it is never printed.
set -euo pipefail
keys_file=$1; shift
umask 077
[ -s "$keys_file" ] || echo "team,aws_access_key_id,aws_secret_access_key" > "$keys_file"

for team in "$@"; do
  aws iam create-user --user-name "$team" > /dev/null
  aws iam add-user-to-group --group-name citibike-contributors --user-name "$team"
  databricks secrets create-scope "${team}-aws"

  key_json=$(aws iam create-access-key --user-name "$team")
  key_id=$(jq -r .AccessKey.AccessKeyId <<< "$key_json")
  secret=$(jq -r .AccessKey.SecretAccessKey <<< "$key_json")
  unset key_json
  databricks secrets put-secret "${team}-aws" aws-access-key-id --string-value "$key_id"
  databricks secrets put-secret "${team}-aws" aws-secret-access-key --string-value "$secret"
  echo "${team},${key_id},${secret}" >> "$keys_file"
  unset key_id secret

  sp_id=$(databricks service-principals list --filter "displayName eq \"${team}-sp\"" -o json | jq -r '.[0].applicationId')
  databricks secrets put-acl "${team}-aws" "$sp_id" READ
  databricks secrets list-acls "${team}-aws"
done
