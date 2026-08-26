using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary>
/// The per-partition result cache (SINGLE_VALUE_PARTITIONS + <c>vgi.cache.partition_scope</c>)
/// fixture family — backs <c>partition_scope*.test</c>. Every function here:
/// <list type="bullet">
/// <item>declares <see cref="ITableFunction.PartitionKind"/> = <see cref="VgiPartitionKind.SingleValuePartitions"/>;</item>
/// <item>marks its partition column(s) with <see cref="VgiWireMetadata.PartitionColumnKey"/> metadata
/// on <see cref="ITableFunction.OutputSchema"/>;</item>
/// <item>emits EXACTLY ONE partition value per batch, attaching the mandatory
/// <c>vgi_partition_values#b64</c> carrier (<see cref="PartitionValuesCodec"/>) built from the
/// partition-columns-only schema;</item>
/// <item>advertises <c>vgi.cache.ttl=300</c> (the reap-math in <c>partition_scope_ops.test</c>
/// depends on exactly this value) + <c>vgi.cache.partition_scope="1"</c>;</item>
/// <item>genuinely applies pushed-down filters (<see cref="ITableFunction.FilterPushdown"/> = true) —
/// DuckDB trusts a pushdown-capable function unconditionally, so a partial/absent filter apply would
/// leak wrong rows.</item>
/// </list>
/// </summary>
public static class CachePartitionFunctions
{
    internal const long TtlSeconds = 300;
}

/// <summary><c>ex.cache_partition_scope(n)</c> — 5 countries (<c>AU,BR,CA,FR,US</c>, alphabetical),
/// <c>n</c> rows each. <c>sales</c> for country at alphabetical index <c>i</c> = <c>i*1_000_000 + row</c>
/// (so <c>US</c>, index 4, is <c>4000000..4000000+n-1</c> — matches the test's pinned values).
/// Also reused, registered under the name <c>cache_partitioned</c>, for
/// <c>spill_partition_values.test</c> (which needs the exact same SINGLE_VALUE_PARTITIONS/country
/// shape but doesn't otherwise care about the specific offsets or exercise partition-scope serving
/// itself — only that <c>vgi_partition_values</c> round-trips correctly through a disk spill).</summary>
public sealed class CachePartitionScopeFunction(string name = "cache_partition_scope") : ITableFunction
{
    private static readonly string[] Countries = ["AU", "BR", "CA", "FR", "US"];

    public string Name => name;

    public string SchemaName => "main";

    public bool? FilterPushdown => true;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("country", StringType.Default, nullable: true, PartitionColumnMetadata()),
            new Field("sales", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    private static Dictionary<string, string> PartitionColumnMetadata() =>
        new() { [VgiWireMetadata.PartitionColumnKey] = VgiWireMetadata.PartitionColumnTrueValue };

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var n = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var partitionSchema = new Schema([initParams.OutputSchema.GetFieldByIndex(0)], metadata: null);
        return new Producer(n, decoded, initParams.OutputSchema, partitionSchema);
    }

    private sealed class Producer(long n, DecodedFilters? decoded, Schema outputSchema, Schema partitionSchema) : ITableFunctionProducer
    {
        private int _countryIndex;

        public void Produce(OutputCollector output)
        {
            while (_countryIndex < Countries.Length)
            {
                var country = Countries[_countryIndex];
                var baseOffset = _countryIndex * 1_000_000L;
                _countryIndex++;

                var sales = new List<long>();
                var row = new Dictionary<string, object?>();
                for (var i = 0L; i < n; i++)
                {
                    var s = baseOffset + i;
                    row["country"] = country;
                    row["sales"] = s;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        sales.Add(s);
                    }
                }

                if (sales.Count == 0)
                {
                    continue;
                }

                var countryBuilder = new StringArray.Builder();
                var salesBuilder = new Int64Array.Builder();
                foreach (var s in sales)
                {
                    countryBuilder.Append(country);
                    salesBuilder.Append(s);
                }

                // Only the FIRST batch's custom_metadata is actually parsed for vgi.cache.* — later
                // batches only need the per-batch partition_values carrier, but re-sending the
                // cache-control keys too is harmless.
                var metadata = new Dictionary<string, string>(CacheMetadata.PartitionScope(CachePartitionFunctions.TtlSeconds))
                {
                    ["vgi_partition_values#b64"] = PartitionValuesCodec.EncodeSingleValueBase64(partitionSchema, [country]),
                };
                output.Emit(new RecordBatch(outputSchema, [countryBuilder.Build(), salesBuilder.Build()], sales.Count), metadata);
                return;
            }

            output.Finish();
        }
    }
}

/// <summary><c>ex.cache_partition_parallel(n)</c> — 4 partition VALUES (<c>AU, CA, US</c>, and a
/// genuine SQL NULL), each claimed as one "chunk" from a shared cross-process work queue so
/// <c>threads=8</c> / <c>pool false</c> fans capture across &gt;1 real worker process
/// (<c>num_substreams &gt; 1</c>). <c>US</c> (queue index 2) is offset <c>2_000_000</c> — matches the
/// test's pinned <c>US 2000000,2000001,2000002</c>.</summary>
public sealed class CachePartitionParallelFunction : ITableFunction
{
    private static readonly string?[] Groups = ["AU", "CA", "US", null];

    public string Name => "cache_partition_parallel";

    public string SchemaName => "main";

    public int? MaxWorkers => 8;

    public bool? FilterPushdown => true;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("country", StringType.Default, nullable: true, PartitionColumnMetadata()),
            new Field("sales", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    private static Dictionary<string, string> PartitionColumnMetadata() =>
        new() { [VgiWireMetadata.PartitionColumnKey] = VgiWireMetadata.PartitionColumnTrueValue };

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var n = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var partitionSchema = new Schema([initParams.OutputSchema.GetFieldByIndex(0)], metadata: null);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, n, decoded, initParams.OutputSchema, partitionSchema);
    }

    private sealed class Producer(string key, long n, DecodedFilters? decoded, Schema outputSchema, Schema partitionSchema)
        : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            while (true)
            {
                var claimed = CrossProcessWorkQueue.ClaimChunk(key, chunkSize: 1, total: Groups.Length, out var groupIndex);
                if (claimed == 0)
                {
                    output.Finish();
                    return;
                }

                var country = Groups[groupIndex];
                var baseOffset = groupIndex * 1_000_000L;

                var sales = new List<long>();
                var row = new Dictionary<string, object?>();
                for (var i = 0L; i < n; i++)
                {
                    var s = baseOffset + i;
                    row["country"] = country;
                    row["sales"] = s;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        sales.Add(s);
                    }
                }

                if (sales.Count == 0)
                {
                    continue;
                }

                var countryBuilder = new StringArray.Builder();
                var salesBuilder = new Int64Array.Builder();
                foreach (var s in sales)
                {
                    if (country is null)
                    {
                        countryBuilder.AppendNull();
                    }
                    else
                    {
                        countryBuilder.Append(country);
                    }

                    salesBuilder.Append(s);
                }

                var metadata = new Dictionary<string, string>(CacheMetadata.PartitionScope(CachePartitionFunctions.TtlSeconds))
                {
                    ["vgi_partition_values#b64"] = PartitionValuesCodec.EncodeSingleValueBase64(partitionSchema, [country]),
                };
                output.Emit(new RecordBatch(outputSchema, [countryBuilder.Build(), salesBuilder.Build()], sales.Count), metadata);
                return;
            }
        }
    }
}

/// <summary><c>ex.cache_partition_multicol(n)</c> — TWO partition columns (<c>region ∈ {EU,US}</c>,
/// <c>year ∈ {2020,2022}</c>). <c>amount</c> base per <c>(region,year)</c> tuple =
/// <c>(regionIndex*2 + yearIndex) * 1000</c> — <c>(US,2020)</c> → index <c>(1*2+0)=2</c> → base
/// <c>2000</c>, matching the test's pinned <c>US 2020 2000,2001</c>.</summary>
public sealed class CachePartitionMulticolFunction : ITableFunction
{
    private static readonly string[] Regions = ["EU", "US"];
    private static readonly int[] Years = [2020, 2022];

    public string Name => "cache_partition_multicol";

    public string SchemaName => "main";

    public bool? FilterPushdown => true;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("region", StringType.Default, nullable: true, PartitionColumnMetadata()),
            new Field("year", Int32Type.Default, nullable: true, PartitionColumnMetadata()),
            new Field("amount", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    private static Dictionary<string, string> PartitionColumnMetadata() =>
        new() { [VgiWireMetadata.PartitionColumnKey] = VgiWireMetadata.PartitionColumnTrueValue };

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var n = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var partitionSchema = new Schema(
            [initParams.OutputSchema.GetFieldByIndex(0), initParams.OutputSchema.GetFieldByIndex(1)], metadata: null);
        return new Producer(n, decoded, initParams.OutputSchema, partitionSchema);
    }

    private sealed class Producer(long n, DecodedFilters? decoded, Schema outputSchema, Schema partitionSchema) : ITableFunctionProducer
    {
        private int _regionIndex;
        private int _yearIndex;

        public void Produce(OutputCollector output)
        {
            while (_regionIndex < Regions.Length)
            {
                var region = Regions[_regionIndex];
                var year = Years[_yearIndex];
                var baseAmount = ((long)_regionIndex * Years.Length + _yearIndex) * 1000;

                Advance();

                var amounts = new List<long>();
                var row = new Dictionary<string, object?>();
                for (var i = 0L; i < n; i++)
                {
                    var amount = baseAmount + i;
                    row["region"] = region;
                    row["year"] = (long)year;
                    row["amount"] = amount;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        amounts.Add(amount);
                    }
                }

                if (amounts.Count == 0)
                {
                    continue;
                }

                var regionBuilder = new StringArray.Builder();
                var yearBuilder = new Int32Array.Builder();
                var amountBuilder = new Int64Array.Builder();
                foreach (var amount in amounts)
                {
                    regionBuilder.Append(region);
                    yearBuilder.Append(year);
                    amountBuilder.Append(amount);
                }

                var metadata = new Dictionary<string, string>(CacheMetadata.PartitionScope(CachePartitionFunctions.TtlSeconds))
                {
                    ["vgi_partition_values#b64"] = PartitionValuesCodec.EncodeSingleValueBase64(partitionSchema, [region, year]),
                };
                output.Emit(
                    new RecordBatch(outputSchema, [regionBuilder.Build(), yearBuilder.Build(), amountBuilder.Build()], amounts.Count),
                    metadata);
                return;
            }

            output.Finish();
        }

        private void Advance()
        {
            _yearIndex++;
            if (_yearIndex >= Years.Length)
            {
                _yearIndex = 0;
                _regionIndex++;
            }
        }
    }
}

/// <summary><c>ex.cache_partition_proj(n)</c> — 2 countries (<c>CA</c> base 0, <c>US</c> base
/// 1_000_000 — matches the test's pinned <c>CA 0,1,2</c> / <c>US 1000000,1000001,1000002</c>), plus a
/// throwaway <c>extra</c> column (never itself checked — exists only so projection genuinely narrows
/// the output). Advertises BOTH <see cref="ITableFunction.ProjectionPushdown"/> and
/// <see cref="ITableFunction.FilterPushdown"/> — the partition-value carrier is always built from the
/// internal <c>country</c> value regardless of whether <c>country</c> survives projection.</summary>
public sealed class CachePartitionProjFunction : ITableFunction
{
    private static readonly string[] Countries = ["CA", "US"];

    public string Name => "cache_partition_proj";

    public string SchemaName => "main";

    public bool? FilterPushdown => true;

    public bool? ProjectionPushdown => true;

    public VgiPartitionKind PartitionKind => VgiPartitionKind.SingleValuePartitions;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("country", StringType.Default, nullable: true, PartitionColumnMetadata()),
            new Field("sales", Int64Type.Default, nullable: false),
            new Field("extra", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    private static Dictionary<string, string> PartitionColumnMetadata() =>
        new() { [VgiWireMetadata.PartitionColumnKey] = VgiWireMetadata.PartitionColumnTrueValue };

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var n = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var partitionSchema = new Schema([initParams.OutputSchema.GetFieldByIndex(0)], metadata: null);
        return new Producer(n, decoded, initParams.ProjectedSchema, initParams.ProjectionIds, partitionSchema);
    }

    private sealed class Producer(
        long n, DecodedFilters? decoded, Schema projectedSchema, IReadOnlyList<long>? projectionIds, Schema partitionSchema)
        : ITableFunctionProducer
    {
        private int _countryIndex;

        public void Produce(OutputCollector output)
        {
            while (_countryIndex < Countries.Length)
            {
                var country = Countries[_countryIndex];
                var baseOffset = _countryIndex * 1_000_000L;
                _countryIndex++;

                var matched = new List<long>();
                var row = new Dictionary<string, object?>();
                for (var i = 0L; i < n; i++)
                {
                    var sales = baseOffset + i;
                    row["country"] = country;
                    row["sales"] = sales;
                    row["extra"] = i;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        matched.Add(i);
                    }
                }

                if (matched.Count == 0)
                {
                    continue;
                }

                var indices = projectionIds ?? [0, 1, 2];

                IArrowArray BuildColumn(long fullIndex)
                {
                    if (fullIndex == 0)
                    {
                        var b = new StringArray.Builder();
                        foreach (var _ in matched)
                        {
                            b.Append(country);
                        }

                        return b.Build();
                    }

                    if (fullIndex == 1)
                    {
                        var b = new Int64Array.Builder();
                        foreach (var i in matched)
                        {
                            b.Append(baseOffset + i);
                        }

                        return b.Build();
                    }

                    var extraBuilder = new Int64Array.Builder();
                    foreach (var i in matched)
                    {
                        extraBuilder.Append(i);
                    }

                    return extraBuilder.Build();
                }

                var columns = indices.Select(BuildColumn).ToList();
                var metadata = new Dictionary<string, string>(CacheMetadata.PartitionScope(CachePartitionFunctions.TtlSeconds))
                {
                    ["vgi_partition_values#b64"] = PartitionValuesCodec.EncodeSingleValueBase64(partitionSchema, [country]),
                };
                output.Emit(new RecordBatch(projectedSchema, columns, matched.Count), metadata);
                return;
            }

            output.Finish();
        }
    }
}
