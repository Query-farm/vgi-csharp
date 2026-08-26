using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// Time-travel + filter-pushdown-together fixtures backing <c>table/time_travel_pushdown.test</c>:
/// a table can be BOTH partition-pruned (filter pushdown) AND time-travelled
/// (<c>AT (VERSION|TIMESTAMP ...)</c>) in the SAME query, declared two different ways —
/// <see cref="TtPushdownFnFunction"/> (function-backed, reads AT at INIT — the
/// <see cref="TableInitParams.AtUnit"/>/<see cref="TableInitParams.AtValue"/> plumbing this
/// milestone adds) and <see cref="TtPushdownColsFunction"/> (columns-based: the catalog resolves
/// AT into a scan-function <c>version</c> ARGUMENT via
/// <see cref="CatalogTable.ResolveScanArguments"/> — the "native" mechanism
/// <c>catalog_table_scan_branches_get</c> already had). Both share the same version→rows data and
/// output shape so one query can assert both signals (row content AND <c>pushed_filters</c>) at
/// once. Output schema is version-INDEPENDENT (no schema evolution here, unlike
/// <see cref="VersionedTimeTravelTables"/>), so <see cref="TtPushdownFnFunction"/> stays a plain
/// inline-bound function-backed table.
/// </summary>
public static class TimeTravelPushdownTables
{
    private const string SchemaName = "data";

    private const int CurrentVersion = 2;

    private static readonly Schema TtSchema = new(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("val", Int64Type.Default, nullable: true),
            new Field("seen_version", Int64Type.Default, nullable: true),
            new Field("pushed_filters", StringType.Default, nullable: true),
        ],
        metadata: null);

    private static readonly Dictionary<int, long[]> VersionIds = new()
    {
        [1] = [1, 2, 3, 4, 5],
        [2] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    };

    public static CatalogTable TtPushdownFn { get; } = new()
    {
        Name = "tt_pushdown_fn",
        SchemaName = SchemaName,
        Comment = "Function-backed: prunes by filter AND time-travels (AT read at init).",
        Columns = TtSchema,
        ScanFunction = new TtPushdownFnFunction(),
        SupportsTimeTravel = true,
    };

    public static CatalogTable TtPushdownCols { get; } = new()
    {
        Name = "tt_pushdown_cols",
        SchemaName = SchemaName,
        Comment = "Columns-based: prunes by filter AND time-travels (AT → version arg).",
        Columns = TtSchema,
        ScanFunction = new TtPushdownColsFunction(),
        ScanArguments = [(long)CurrentVersion],
        InlineScanFunction = false,
        SupportsTimeTravel = true,
        ResolveScanArguments = ResolveColsScanArguments,
    };

    public static IReadOnlyList<CatalogTable> All { get; } = [TtPushdownFn, TtPushdownCols];

    private static (IReadOnlyList<object?> Positional, IReadOnlyDictionary<string, object?> Named) ResolveColsScanArguments(
        string atUnit, string atValue)
    {
        IReadOnlyList<object?> positional = [(long)ResolveVersion(atUnit, atValue)];
        IReadOnlyDictionary<string, object?> named = new Dictionary<string, object?>();
        return (positional, named);
    }

    /// <summary>Resolves an AT clause to one of this fixture's versions (1 or 2). <c>null</c>/empty
    /// <paramref name="atUnit"/> → current version (2). <c>VERSION =&gt; n</c> → <c>n</c> (must be 1
    /// or 2). <c>TIMESTAMP</c> → year &lt;= 2020 → 1, else 2 (no lower bound — unlike
    /// <see cref="VersionedTimeTravelTables"/>, this fixture has no "didn't exist before" case).</summary>
    private static int ResolveVersion(string? atUnit, string? atValue)
    {
        if (string.IsNullOrEmpty(atUnit))
        {
            return CurrentVersion;
        }

        if (string.Equals(atUnit, "VERSION", StringComparison.OrdinalIgnoreCase))
        {
            if (!int.TryParse(atValue, out var version) || !VersionIds.ContainsKey(version))
            {
                throw new InvalidOperationException($"Unknown version {atValue}; valid: 1, 2");
            }

            return version;
        }

        if (string.Equals(atUnit, "TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            var year = int.Parse((atValue ?? "").AsSpan(0, Math.Min(4, atValue?.Length ?? 0)));
            return year <= 2020 ? 1 : 2;
        }

        throw new InvalidOperationException($"Unsupported AT clause unit: '{atUnit}'.");
    }

    /// <summary>Filters/projects <paramref name="version"/>'s rows and emits them in one batch —
    /// shared by both fixtures below.</summary>
    private static void EmitVersion(
        int version, DecodedFilters? decoded, string filterText, Schema projectedSchema,
        IReadOnlyList<long>? projectionIds, OutputCollector output)
    {
        var ids = VersionIds[version];
        var matched = new List<long>();
        var row = new Dictionary<string, object?>();
        foreach (var id in ids)
        {
            var val = id * 10;
            row["id"] = id;
            row["val"] = val;
            if (PushdownFilterEvaluator.Matches(decoded, row))
            {
                matched.Add(id);
            }
        }

        if (matched.Count == 0)
        {
            output.Finish();
            return;
        }

        var indices = projectionIds ?? [0, 1, 2, 3];
        var columns = indices.Select(i => BuildColumn((int)i, matched, version, filterText)).ToList();
        output.Emit(new RecordBatch(projectedSchema, columns, matched.Count));
        output.Finish();
    }

    private static IArrowArray BuildColumn(int fullIndex, IReadOnlyList<long> ids, int version, string filterText)
    {
        switch (fullIndex)
        {
            case 0:
                return new Int64Array.Builder().AppendRange(ids).Build();
            case 1:
                return new Int64Array.Builder().AppendRange(ids.Select(id => id * 10)).Build();
            case 2:
                return new Int64Array.Builder().AppendRange(ids.Select(_ => (long)version)).Build();
            default:
                var builder = new StringArray.Builder();
                foreach (var _ in ids)
                {
                    builder.Append(filterText);
                }

                return builder.Build();
        }
    }

    /// <summary>Function-backed: reads the AT clause off <see cref="TableInitParams.AtUnit"/>/
    /// <see cref="TableInitParams.AtValue"/> (populated from the embedded bind call's
    /// <see cref="Protocol.BindRequest.AtUnit"/>/<c>AtValue</c>) — no scan arguments at all, since
    /// the version comes from AT, not from a call argument.</summary>
    public sealed class TtPushdownFnFunction : ITableFunction
    {
        public string Name => "tt_pushdown_scan";

        public string SchemaName => TimeTravelPushdownTables.SchemaName;

        public string Description => "Function-backed time-travel + filter-pushdown scan (reads AT at init)";

        public IReadOnlyList<string> Categories => ["generator", "diagnostic", "testing"];

        public Schema ArgumentsSchema { get; } = new([], metadata: null);

        public Schema OutputSchema => TtSchema;

        public bool? FilterPushdown => true;

        public bool? ProjectionPushdown => true;

        public ITableFunctionProducer CreateProducer(TableInitParams initParams)
        {
            var version = ResolveVersion(initParams.AtUnit, initParams.AtValue);
            var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
            var filterText = PushdownFilterFormatter.Format(decoded);
            return new Producer(version, decoded, filterText, initParams.ProjectedSchema, initParams.ProjectionIds);
        }

        private sealed class Producer(
            int version, DecodedFilters? decoded, string filterText, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
            : ITableFunctionProducer
        {
            private bool _emitted;

            public void Produce(OutputCollector output)
            {
                if (_emitted)
                {
                    output.Finish();
                    return;
                }

                _emitted = true;
                EmitVersion(version, decoded, filterText, projectedSchema, projectionIds, output);
            }
        }
    }

    /// <summary>Columns-based: takes the resolved <c>version</c> as a positional argument, injected
    /// by <see cref="CatalogTable.ResolveScanArguments"/> from the AT clause via
    /// <c>catalog_table_scan_branches_get</c> — the native columns-based time-travel mechanism.</summary>
    public sealed class TtPushdownColsFunction : ITableFunction
    {
        public string Name => "tt_pushdown_cols_scan";

        public string SchemaName => TimeTravelPushdownTables.SchemaName;

        public string Description => "Columns-based time-travel + filter-pushdown scan (version via arg)";

        public IReadOnlyList<string> Categories => ["generator", "diagnostic", "testing"];

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("version", Int64Type.Default)], metadata: null);

        public Schema OutputSchema => TtSchema;

        public bool? FilterPushdown => true;

        public bool? ProjectionPushdown => true;

        public ITableFunctionProducer CreateProducer(TableInitParams initParams)
        {
            var version = (int)initParams.Arguments.Int64(0);
            var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
            var filterText = PushdownFilterFormatter.Format(decoded);
            return new Producer(version, decoded, filterText, initParams.ProjectedSchema, initParams.ProjectionIds);
        }

        private sealed class Producer(
            int version, DecodedFilters? decoded, string filterText, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
            : ITableFunctionProducer
        {
            private bool _emitted;

            public void Produce(OutputCollector output)
            {
                if (_emitted)
                {
                    output.Finish();
                    return;
                }

                _emitted = true;
                EmitVersion(version, decoded, filterText, projectedSchema, projectionIds, output);
            }
        }
    }
}
