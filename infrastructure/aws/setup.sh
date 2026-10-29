#!/usr/bin/env bash
# One-time setup: contributors group, lifecycle rule, S3 budget with a kill switch.
set -euo pipefail
cd "$(dirname "$0")"
account=103358525358
bucket=cuny-sps-citibike-archive

aws iam create-group --group-name citibike-contributors
aws iam put-group-policy --group-name citibike-contributors \
  --policy-name write-own-contributions-folder \
  --policy-document file://write-own-contributions-folder.json

aws s3api put-bucket-lifecycle-configuration --bucket "$bucket" \
  --lifecycle-configuration file://lifecycle.json

aws budgets create-budget --account-id "$account" \
  --budget file://s3-budget.json \
  --notifications-with-subscribers file://s3-budget-notifications.json

# At 100% of actual spend, AWS Budgets attaches AWSDenyAll to the group, which stops every team key.
aws iam create-role --role-name citibike-budget-action \
  --assume-role-policy-document file://budget-action-trust.json
aws iam put-role-policy --role-name citibike-budget-action \
  --policy-name attach-deny-to-contributors \
  --policy-document file://budget-action-permissions.json
sleep 15  # the new role takes a few seconds to become assumable
aws budgets create-budget-action --account-id "$account" \
  --budget-name citibike-s3-budget \
  --notification-type ACTUAL --action-type APPLY_IAM_POLICY \
  --action-threshold ActionThresholdValue=100,ActionThresholdType=PERCENTAGE \
  --definition "IamActionDefinition={PolicyArn=arn:aws:iam::aws:policy/AWSDenyAll,Groups=[citibike-contributors]}" \
  --execution-role-arn "arn:aws:iam::${account}:role/citibike-budget-action" \
  --approval-model AUTOMATIC \
  --subscribers SubscriptionType=EMAIL,Address=georgehagstrom@gmail.com
