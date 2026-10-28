#!/usr/bin/env bash
# Verify the packed SDK outside repository-wide dependency exclusions.
set -euo pipefail
package_dir=$(cd "${1:?package directory required}" && pwd)
version=$(sed -n 's/.*<Version>\([^<]*\)<\/Version>.*/\1/p' Directory.Build.props | head -1)
consumer_dir=$(mktemp -d)
trap 'rm -rf "$consumer_dir"' EXIT
mkdir -p "$consumer_dir/library" "$consumer_dir/app"
cat > "$consumer_dir/library/Library.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><TargetFramework>net10.0</TargetFramework></PropertyGroup>
  <ItemGroup><PackageReference Include="QueryFarm.Vgi" Version="$version" /></ItemGroup>
</Project>
EOF
cat > "$consumer_dir/library/Marker.cs" <<'EOF'
public static class Marker { public static string Name => QueryFarm.Vgi.Protocol.VgiProtocol.Name; }
EOF
cat > "$consumer_dir/app/Consumer.csproj" <<'EOF'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><TargetFramework>net10.0</TargetFramework><OutputType>Exe</OutputType><ImplicitUsings>enable</ImplicitUsings></PropertyGroup>
  <ItemGroup><ProjectReference Include="../library/Library.csproj" /></ItemGroup>
</Project>
EOF
cat > "$consumer_dir/app/Program.cs" <<'EOF'
using Apache.Arrow;
using Apache.Arrow.Types;
using DuckDB.NET.Data;
using QueryFarm.Vgi.Internal;
if (Marker.Name != "vgi.v3" || new StructType([]).Fields.Count != 0)
    throw new Exception("VGI/Arrow package contract mismatch");
var schema = new Schema([new Field("n", Int64Type.Default, false)], null);
using var batch = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Append(2).Build()], 2);
using var filtered = ExpressionFilterEvaluator.ApplyMask(batch, [false, true]);
if (filtered.Length != 1 || ((Int64Array)filtered.Column(0)).GetValue(0) != 2)
    throw new Exception("SDK Arrow filtering failed");
using var connection = new DuckDBConnection("DataSource=:memory:");
connection.Open();
using var command = connection.CreateCommand();
command.CommandText = "SELECT sum(x)::BIGINT FROM range(1, 4) t(x)";
if (Convert.ToInt64(command.ExecuteScalar()) != 6)
    throw new Exception("DuckDB data access failed");
Console.WriteLine("Package consumer passed: VGI v3, empty STRUCT, SDK Arrow filtering, and DuckDB data access");
EOF
# A private cache prevents a previously restored SDK package from hiding packaging mistakes.
dotnet restore "$consumer_dir/app/Consumer.csproj" --packages "$consumer_dir/packages" \
  --source "$package_dir" --source https://api.nuget.org/v3/index.json
dotnet run --project "$consumer_dir/app/Consumer.csproj" -c Release --no-restore
