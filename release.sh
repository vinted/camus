#!/bin/bash

set -e

AUTH_TOKEN_FILE=".auth_token"
PROJECT="vinted/camus"

if [ -f $AUTH_TOKEN_FILE ];
then
   oauth_key=`cat $AUTH_TOKEN_FILE`
else
   read -r -p "Did not find '$AUTH_TOKEN_FILE'. Please provide your GitHub OAuth key (create here https://github.com/settings/tokens): " oauth_key
fi

echo "Currently available release tags:"

release_list_json=$(curl -s -H "Content-Type: application/json" -H "Authorization: token $oauth_key" https://api.github.com/repos/$PROJECT/releases)

ruby -rjson -e 'puts JSON.parse(ARGV[0]).map { |rev| "* " + rev["tag_name"] }.join("\n")' "$release_list_json"

read -r -p "Please provide a new release tag name: " tag_name

echo "Building Camus release $tag_name"

mvn clean package -Dcamus.version=$tag_name

echo "Tagging Camus release $tag_name"

release_body=$(cat << EOF
{
    "tag_name": "$tag_name",
    "target_commitish": "master",
    "name": "v$tag_name",
    "body": "Vinted Camus release $tag_name",
    "draft": false,
    "prerelease": false
}
EOF
)

release_create_json=$(curl -s -H "Content-Type: application/json" -H "Authorization: token $oauth_key" -X POST -d "$release_body" https://api.github.com/repos/${PROJECT}/releases)
release_id=$(ruby -rjson -e 'puts JSON.parse(ARGV[0])["id"]' "$release_create_json")

echo "Uploading Camus ETL jar for $tag_name"

camus_etl_file="camus-etl-kafka-$tag_name-shaded.jar"

curl -s -H "Content-Type: application/zip" -H "Authorization: token $oauth_key" -X POST -T camus-etl-kafka/target/$camus_etl_file https://uploads.github.com/repos/${PROJECT}/releases/$release_id/assets?name=$camus_etl_file > /dev/null 2>&1

echo "Uploading Camus Sweeper jar for $tag_name"

camus_sweeper_file="camus-sweeper-$tag_name.jar"

curl -s -H "Content-Type: application/zip" -H "Authorization: token $oauth_key" -X POST -T camus-sweeper/target/$camus_sweeper_file https://uploads.github.com/repos/${PROJECT}/releases/$release_id/assets?name=$camus_sweeper_file > /dev/null 2>&1
