#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec dotnet run --project "$repo_root/examples/docs/QueryFarm.Vgi.DocsExamples.csproj" --configuration Release -- "$@"
