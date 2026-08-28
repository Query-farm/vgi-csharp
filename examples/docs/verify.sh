#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
project="$repo_root/examples/docs/QueryFarm.Vgi.DocsExamples.csproj"
test_source="$repo_root/examples/docs/test/examples.test"
binary="$repo_root/examples/docs/bin/Release/net10.0/vgi-csharp-docs"

dotnet build "$project" --configuration Release

test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT
mkdir -p "$test_dir/test/sql"
sed "s|__VGI_WORKER__|launch:$binary|g" "$test_source" > "$test_dir/test/sql/examples.test"

(cd "$test_dir" && uvx haybarn-unittest test/sql/examples.test)
