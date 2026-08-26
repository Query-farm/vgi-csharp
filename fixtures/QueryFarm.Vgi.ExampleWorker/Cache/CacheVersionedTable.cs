using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary>
/// <c>example.data.cache_versioned</c> — backs <c>cache/at_isolation.test</c>: the VGI result cache
/// key folds <c>at_unit</c>/<c>at_value</c>, so <c>AT (VERSION =&gt; 1)</c>/<c>AT (VERSION =&gt; 2)</c>/
/// the live (no-AT) scan are three distinct cache entries that never cross-serve. Columns-based
/// time travel (same mechanism as <see cref="ExampleWorker.Table.TimeTravelPushdownTables.TtPushdownCols"/>):
/// the catalog resolves AT into a <c>version</c> scan-function argument via
/// <see cref="CatalogTable.ResolveScanArguments"/>, and the underlying <c>cache_versioned_scan</c>
/// function (the name entries appear under in <c>vgi_result_cache()</c>) is the one that actually
/// advertises the cache TTL.
/// </summary>
public static class CacheVersionedTable
{
    private const string SchemaName = "data";

    private const int CurrentVersion = 3;

    private static readonly Dictionary<int, long[]> VersionValues = new()
    {
        [1] = [101, 102, 103],
        [2] = [201, 202],
        [3] = [301, 302, 303, 304],
    };

    public static CatalogTable Table { get; } = new()
    {
        Name = "cache_versioned",
        SchemaName = SchemaName,
        Comment = "Version-specific cacheable rows (AT-keyed cache isolation)",
        ScanFunction = new CacheVersionedFunction(),
        ScanArguments = [(long)CurrentVersion],
        InlineScanFunction = false,
        SupportsTimeTravel = true,
        ResolveScanArguments = ResolveScanArguments,
    };

    private static (IReadOnlyList<object?> Positional, IReadOnlyDictionary<string, object?> Named) ResolveScanArguments(
        string atUnit, string atValue)
    {
        IReadOnlyList<object?> positional = [(long)ResolveVersion(atUnit, atValue)];
        IReadOnlyDictionary<string, object?> named = new Dictionary<string, object?>();
        return (positional, named);
    }

    private static int ResolveVersion(string atUnit, string atValue)
    {
        if (string.Equals(atUnit, "VERSION", StringComparison.OrdinalIgnoreCase))
        {
            if (!int.TryParse(atValue, out var version) || !VersionValues.ContainsKey(version))
            {
                throw new InvalidOperationException($"Unknown version {atValue}; valid: 1, 2, 3");
            }

            return version;
        }

        if (string.Equals(atUnit, "TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            var year = int.Parse(atValue.AsSpan(0, Math.Min(4, atValue.Length)));
            if (year <= 2020)
            {
                return 1;
            }

            return year <= 2021 ? 2 : CurrentVersion;
        }

        throw new InvalidOperationException($"Unsupported AT clause unit: '{atUnit}'.");
    }

    public sealed class CacheVersionedFunction : ITableFunction
    {
        public string Name => "cache_versioned_scan";

        public string SchemaName => CacheVersionedTable.SchemaName;

        public string Description => "Version-specific rows; cacheable (AT-keyed)";

        public IReadOnlyList<string> Categories => ["generator", "cache", "testing"];

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("version", Int64Type.Default)], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("v", Int64Type.Default, nullable: false)], metadata: null);

        public ITableFunctionProducer CreateProducer(TableInitParams initParams)
        {
            var version = (int)initParams.Arguments.Int64(0);
            var values = VersionValues.TryGetValue(version, out var v) ? v : VersionValues[CurrentVersion];
            return new Producer(values, initParams.OutputSchema);
        }

        private sealed class Producer(long[] values, Schema outputSchema) : ITableFunctionProducer
        {
            private bool _emitted;

            public void Produce(OutputCollector output)
            {
                if (!_emitted)
                {
                    _emitted = true;
                    var builder = new Int64Array.Builder().AppendRange(values);
                    output.Emit(new RecordBatch(outputSchema, [builder.Build()], values.Length), CacheMetadata.Ttl(300));
                }

                output.Finish();
            }
        }
    }
}
