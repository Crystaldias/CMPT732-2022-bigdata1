#!/usr/bin/env bash
# Instantiate the template into actual SQL
# The resulting SQL file should be kept in the
# `private` subdirectory, which is not version-controlled.
set -o errexit
set -o nounset

# File that must contain student's AWS Account ID
aws_file=private/aws-id.txt

if [[ $# -ne 0 ]]
then
  echo "${0} must be called without arguments"
  exit 1
fi

if ! [[ -f ${aws_file} ]]
then
  echo "Requires AWS account id in file ${aws_file}"
  exit 1
fi

aws_id=$(cat ${aws_file})

if [[ ${#aws_id} -ne 12 ]]
then
  echo "${aws_id} is not a valid AWS account ID: Must be exactly 12 digits long"
  exit
fi

# Assignment to variable required to avoid version incompatibilities of escaping.
# See compatibilty note:
# https://stackoverflow.com/questions/806906/how-do-i-test-if-a-variable-is-a-number-in-bash/806923
re='^[0-9]+$'
if ! [[ ${aws_id} =~ ${re} ]]
then
  echo "${aws_id} is not a valid AWS account id: Must only consist of digits"
  exit
fi

bucket_name='public-cmpt-732'
sed -e "s/AWS_ACCOUNT_ID/${aws_id}/g" -e "s/S3_BUCKET/${bucket_name}/g" sql-statements.tpl > private/sql-statements.sql
