#!/usr/bin/env bash
# Uploads a small single-part file through the API with curl, exactly as the
# browser will: start -> presign part 1 -> PUT bytes to S3 -> complete.
set -euo pipefail

API=${API:-http://localhost:5173}
file=${1:-}
if [[ -z $file ]]; then
  file=$(mktemp --suffix=.zip)
  head -c 1048576 /dev/urandom >"$file" # 1 MiB; the API checks name and type, not contents
fi
size=$(wc -c <"$file" | tr -d ' ')

echo "1. start ($size bytes)"
start=$(curl -sS --fail-with-body -X POST "$API/api/uploads" -H 'Content-Type: application/json' \
  -d "$(jq -n --arg f "$(basename "$file")" --argjson s "$size" '{filename:$f, size:$s, contentType:"application/zip"}')")
echo "$start" | jq .
id=$(jq -r .id <<<"$start")
upload_id=$(jq -r .uploadId <<<"$start")
key=$(jq -r .key <<<"$start")

echo "2. presign part 1"
url=$(curl -sS --fail-with-body -X POST "$API/api/uploads/$id/part-url" -H 'Content-Type: application/json' \
  -d "$(jq -n --arg u "$upload_id" '{uploadId:$u, partNumber:1}')" | jq -r .url)
echo "${url%%\?*}?…(signature)"

echo "3. PUT bytes straight to S3"
etag=$(curl -sS --fail-with-body -X PUT -T "$file" "$url" -D - -o /dev/null |
  grep -i '^etag:' | cut -d' ' -f2 | tr -d '\r')
echo "   ETag: $etag"

echo "4. complete"
curl -sS --fail-with-body -X POST "$API/api/uploads/$id/complete" -H 'Content-Type: application/json' \
  -d "$(jq -n --arg u "$upload_id" --arg e "$etag" '{uploadId:$u, parts:[{partNumber:1, etag:$e}]}')" | jq .

echo "Done. Check it with:"
echo "  aws s3api head-object --bucket \"\$UPLOADS_BUCKET\" --key $key"
