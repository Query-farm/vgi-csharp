using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// v2 PartitionColumns (Hive-style) reference fixtures — backs <c>partition_columns*.test</c>. Each
/// declares <see cref="ITableFunction.PartitionKind"/> and annotates its output schema's partition
/// column(s) with <see cref="VgiWireMetadata.PartitionColumnKey"/> metadata; the worker emits ONE
/// Arrow batch per partition value, tagged with the <c>vgi_partition_values#b64</c> metadata built
/// by <see cref="PartitionValuesCodec.PartitionValues"/> so the C++ extension's
/// <c>TableFunction::get_partition_info</c> lets DuckDB's planner pick
/// <c>PhysicalPartitionedAggregate</c> for a matching GROUP BY. All functions here declare
/// <see cref="ITableFunction.MaxWorkers"/> &gt; 1 and claim partition indices from a per-execution
/// <see cref="CrossProcessWorkQueue"/> so parallel scans (multiple subprocess workers) never emit
/// the same partition twice.
/// </summary>
public static class PartitionColumnsFunctions
{
    internal static Dictionary<string, string> PartitionColumnMetadata() =>
        new() { [VgiWireMetadata.PartitionColumnKey] = VgiWireMetadata.PartitionColumnTrueValue };
}

/// <summary><c>ex.country_partitioned_sales(rows_per_country)</c> — 5 countries (<c>AU,BR,CA,FR,US</c>,
/// alphabetical), <c>rows_per_country</c> rows each. <c>sales</c> for country at alphabetical index
/// <c>i</c> = <c>i*1_000_000 + row</c>.</summary>
public sealed class CountryPartitionedSalesFunction : ITableFunction
{
    private static readonly string[] Countries = ["AU", "BR", "CA", "FR", "US"];

    public string Name => "country_partitioned_sales";

    public string SchemaName => "main";

    public int? MaxWorkers => 8;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("rows_per_country", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("country", StringType.Default, nullable: true, PartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("sales", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var rpc = initParams.Arguments.Int64(0);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, rpc, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long rowsPerCountry, Schema outputSchema) : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            var claimed = CrossProcessWorkQueue.ClaimChunk(key, chunkSize: 1, total: Countries.Length, out var idx);
            if (claimed == 0)
            {
                output.Finish();
                return;
            }

            var country = Countries[idx];
            var baseOffset = idx * 1_000_000L;

            var countryBuilder = new StringArray.Builder();
            var salesBuilder = new Int64Array.Builder();
            for (var i = 0L; i < rowsPerCountry; i++)
            {
                countryBuilder.Append(country);
                salesBuilder.Append(baseOffset + i);
            }

            var batch = new RecordBatch(outputSchema, [countryBuilder.Build(), salesBuilder.Build()], (int)rowsPerCountry);
            output.Emit(batch, PartitionValuesCodec.PartitionValues(outputSchema, batch));
        }
    }
}

/// <summary><c>ex.region_year_partitioned(rows_per_partition)</c> — 6 (region, year) tuples: AMER
/// 2023(idx0), AMER 2024(idx1), EMEA 2023(idx2), EMEA 2024(idx3), APAC 2023(idx4), APAC 2024(idx5).
/// <c>value</c> base = <c>idx*1000</c>; values = base + [0, rows_per_partition).</summary>
public sealed class RegionYearPartitionedFunction : ITableFunction
{
    private static readonly string[] Regions = ["AMER", "AMER", "EMEA", "EMEA", "APAC", "APAC"];
    private static readonly long[] Years = [2023, 2024, 2023, 2024, 2023, 2024];

    public string Name => "region_year_partitioned";

    public string SchemaName => "main";

    public int? MaxWorkers => 8;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("rows_per_partition", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("region", StringType.Default, nullable: true, PartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("year", Int64Type.Default, nullable: true, PartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("value", DoubleType.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var rpp = initParams.Arguments.Int64(0);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, rpp, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long rowsPerPartition, Schema outputSchema) : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            var claimed = CrossProcessWorkQueue.ClaimChunk(key, chunkSize: 1, total: Regions.Length, out var idx);
            if (claimed == 0)
            {
                output.Finish();
                return;
            }

            var region = Regions[idx];
            var year = Years[idx];
            var baseValue = idx * 1000.0;

            var regionBuilder = new StringArray.Builder();
            var yearBuilder = new Int64Array.Builder();
            var valueBuilder = new DoubleArray.Builder();
            for (var i = 0L; i < rowsPerPartition; i++)
            {
                regionBuilder.Append(region);
                yearBuilder.Append(year);
                valueBuilder.Append(baseValue + i);
            }

            var batch = new RecordBatch(
                outputSchema, [regionBuilder.Build(), yearBuilder.Build(), valueBuilder.Build()], (int)rowsPerPartition);
            output.Emit(batch, PartitionValuesCodec.PartitionValues(outputSchema, batch));
        }
    }
}

/// <summary><c>ex.partitioned_with_explicit_override(rows_per_category)</c> — 3 categories
/// (<c>books,music,video</c>). <c>revenue</c> base = <c>(idx+1)*100</c>; values = base +
/// [0, rows_per_category). Even though the partition column (<c>category</c>) IS present in the
/// emitted batch, this fixture supplies an EXPLICIT <c>partition_values</c> override (rather than
/// letting the batch auto-extract it) to exercise that code path.</summary>
public sealed class PartitionedWithExplicitOverrideFunction : ITableFunction
{
    private static readonly string[] Categories = ["books", "music", "video"];

    public string Name => "partitioned_with_explicit_override";

    public string SchemaName => "main";

    public int? MaxWorkers => 8;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("rows_per_category", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("category", StringType.Default, nullable: true, PartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("revenue", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var rpc = initParams.Arguments.Int64(0);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, rpc, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long rowsPerCategory, Schema outputSchema) : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            var claimed = CrossProcessWorkQueue.ClaimChunk(key, chunkSize: 1, total: Categories.Length, out var idx);
            if (claimed == 0)
            {
                output.Finish();
                return;
            }

            var category = Categories[idx];
            var baseRevenue = (idx + 1) * 100L;

            var categoryBuilder = new StringArray.Builder();
            var revenueBuilder = new Int64Array.Builder();
            for (var i = 0L; i < rowsPerCategory; i++)
            {
                categoryBuilder.Append(category);
                revenueBuilder.Append(baseRevenue + i);
            }

            var batch = new RecordBatch(outputSchema, [categoryBuilder.Build(), revenueBuilder.Build()], (int)rowsPerCategory);

            // Explicit override even though 'category' is present in the batch.
            var overrides = new Dictionary<string, PartitionValuesCodec.Range>
            {
                ["category"] = new PartitionValuesCodec.Range(category, category),
            };
            output.Emit(batch, PartitionValuesCodec.PartitionValues(outputSchema, batch, overrides));
        }
    }
}

/// <summary><c>ex.disjoint_range_partitioned(partitions, rows_per_partition := 10)</c> — disjoint
/// per-chunk integer ranges on <c>key</c>: partition <c>idx</c> emits keys in
/// <c>[idx*1000, idx*1000+rows_per_partition)</c>. Declares <c>DISJOINT_PARTITIONS</c> — wire-level
/// only; DuckDB has no <c>PhysicalPartitionedAggregate</c>-style consumer for it yet, so a GROUP BY
/// still falls back to <c>HASH_GROUP_BY</c> (only correctness is asserted).</summary>
public sealed class DisjointRangePartitionedFunction : ITableFunction
{
    public string Name => "disjoint_range_partitioned";

    public string SchemaName => "main";

    public int? MaxWorkers => 8;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.DisjointPartitions;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("partitions", Int64Type.Default),
            TableArgFields.Named("rows_per_partition", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("key", Int64Type.Default, nullable: true, PartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("value", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var partitions = initParams.Arguments.Int64(0);
        var rpp = initParams.Arguments.Int64Named("rows_per_partition", 10);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, partitions, rpp, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long partitions, long rowsPerPartition, Schema outputSchema) : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            var claimed = CrossProcessWorkQueue.ClaimChunk(key, chunkSize: 1, total: partitions, out var idx);
            if (claimed == 0)
            {
                output.Finish();
                return;
            }

            var baseKey = idx * 1000L;

            var keyBuilder = new Int64Array.Builder();
            var valueBuilder = new Int64Array.Builder();
            for (var i = 0L; i < rowsPerPartition; i++)
            {
                keyBuilder.Append(baseKey + i);
                valueBuilder.Append(idx * 10L + i);
            }

            var batch = new RecordBatch(outputSchema, [keyBuilder.Build(), valueBuilder.Build()], (int)rowsPerPartition);
            output.Emit(batch, PartitionValuesCodec.PartitionValues(outputSchema, batch));
        }
    }
}

/// <summary><c>ex.overlapping_range_partitioned(partitions, rows_per_partition := 10)</c> — same
/// shape as <see cref="DisjointRangePartitionedFunction"/> but with a stride (500) smaller than the
/// default <c>rows_per_partition</c>, so consecutive chunks genuinely share <c>key</c> values.
/// Declares <c>OVERLAPPING_PARTITIONS</c> — exists only to exercise that wire enum end-to-end (no
/// DuckDB consumer for it either; GROUP BY falls back to <c>HASH_GROUP_BY</c>).</summary>
public sealed class OverlappingRangePartitionedFunction : ITableFunction
{
    public string Name => "overlapping_range_partitioned";

    public string SchemaName => "main";

    public int? MaxWorkers => 8;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.OverlappingPartitions;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("partitions", Int64Type.Default),
            TableArgFields.Named("rows_per_partition", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("key", Int64Type.Default, nullable: true, PartitionColumnsFunctions.PartitionColumnMetadata()),
            new Field("value", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var partitions = initParams.Arguments.Int64(0);
        var rpp = initParams.Arguments.Int64Named("rows_per_partition", 10);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, partitions, rpp, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long partitions, long rowsPerPartition, Schema outputSchema) : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            var claimed = CrossProcessWorkQueue.ClaimChunk(key, chunkSize: 1, total: partitions, out var idx);
            if (claimed == 0)
            {
                output.Finish();
                return;
            }

            // Stride 500 (< default rows_per_partition=10... deliberately kept small: overlap is
            // meaningful once rows_per_partition > 500) makes consecutive chunks share key values.
            var baseKey = idx * 500L;

            var keyBuilder = new Int64Array.Builder();
            var valueBuilder = new Int64Array.Builder();
            for (var i = 0L; i < rowsPerPartition; i++)
            {
                keyBuilder.Append(baseKey + i);
                valueBuilder.Append(idx * 10L + i);
            }

            var batch = new RecordBatch(outputSchema, [keyBuilder.Build(), valueBuilder.Build()], (int)rowsPerPartition);
            output.Emit(batch, PartitionValuesCodec.PartitionValues(outputSchema, batch));
        }
    }
}
