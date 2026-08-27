using System.Collections.Concurrent;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// The <c>rff_*</c> catalog tables backing <c>test/sql/integration/table/required_filters_*.test</c>
/// — DuckDB's "required filter" pushdown contract (<see cref="CatalogTable.RequiredFilters"/>,
/// wired to <see cref="Protocol.TableInfo.RequiredFilters"/>). Enforcement (rejecting a query whose
/// WHERE clause doesn't cover every required CNF group, prefix-satisfaction for struct subfields,
/// OR/IS NULL/IN filter-shape recognition, the row-id sentinel-key skip) is entirely a C++
/// optimizer-side concern (<c>vgi_table_function_impl.cpp</c>) — this worker's only job is to
/// declare the requirement and serve the (small, fixed) row data underneath it.
/// </summary>
public static class RequiredFiltersTables
{
    private const string SchemaName = "data";

    public static CatalogTable Simple { get; } = BuildSimple();

    public static CatalogTable None { get; } = BuildNone();

    public static CatalogTable Multi { get; } = BuildMulti();

    public static CatalogTable Struct { get; } = BuildStruct();

    public static CatalogTable Nested { get; } = BuildNested();

    public static CatalogTable Or { get; } = BuildOr();

    public static CatalogTable Rowid { get; } = BuildRowid();

    public static CatalogTable Parquet { get; } = BuildParquet();

    public static CatalogTable Hive { get; } = BuildHive();

    public static CatalogTable HiveMixed { get; } = BuildHiveMixed();

    public static IReadOnlyList<CatalogTable> All { get; } =
        [Simple, None, Multi, Struct, Nested, Or, Rowid, Parquet, Hive, HiveMixed];

    /// <summary>Shared scratch dir for the <see cref="Parquet"/>/<see cref="Hive"/>/<see cref="HiveMixed"/>
    /// native-delegation fixtures — see <c>MultiBranchTables.BranchDir</c>'s identical doc comment
    /// (both must resolve the SAME concrete path the coupled <c>.test</c> files reference via
    /// <c>${VGI_TEST_BRANCH_DIR}</c>).</summary>
    private static string BranchDir()
    {
        var raw = Environment.GetEnvironmentVariable("VGI_TEST_BRANCH_DIR");
        if (string.IsNullOrEmpty(raw))
        {
            raw = Path.GetTempPath();
        }

        return raw.Replace('\\', '/').TrimEnd('/');
    }

    private static string BranchPath(string name) => $"{BranchDir()}/{name}";

    /// <summary>A native (non-worker) DuckDB function branch, e.g. <c>read_parquet(path)</c> or
    /// <c>read_parquet(path, hive_partitioning := true)</c>.</summary>
    private static ScanBranchSpec Native(string function, string path, Dictionary<string, object?>? named = null) => new()
    {
        FunctionName = function,
        PositionalArguments = [path],
        NamedArguments = named ?? new Dictionary<string, object?>(),
    };

    /// <summary>The <c>bbox: struct{xmin,ymin,xmax,ymax: float}</c> field shared by the native
    /// fixtures — field order MUST match the parquet files the coupled <c>.test</c> constructs via
    /// <c>COPY (SELECT {'xmin': ..., 'ymin': ..., 'xmax': ..., 'ymax': ...} AS bbox ...)</c>, since a
    /// native-delegation table's declared columns must match the native bind's output by position
    /// (see <c>VgiTableEntry::GetScanFunctionImpl</c>'s validation).</summary>
    private static Field BboxField() => new(
        "bbox",
        new StructType(
        [
            new Field("xmin", FloatType.Default, nullable: true),
            new Field("ymin", FloatType.Default, nullable: true),
            new Field("xmax", FloatType.Default, nullable: true),
            new Field("ymax", FloatType.Default, nullable: true),
        ]),
        nullable: true);

    /// <summary>The four bbox-corner required-filter groups, in the canonical declaration order
    /// every <c>required_filters_*.test</c> error message expects: xmin, xmax, ymin, ymax.</summary>
    private static IReadOnlyList<IReadOnlyList<string>> BboxRequiredFilters() =>
        [["bbox.xmin"], ["bbox.xmax"], ["bbox.ymin"], ["bbox.ymax"]];

    private static CatalogTable BuildParquet() => new()
    {
        Name = "rff_parquet",
        SchemaName = SchemaName,
        Comment = "required_filters over native read_parquet delegation (single file, identity projection)",
        Columns = new Schema([BboxField(), new Field("other", Int64Type.Default, nullable: true)], metadata: null),
        RequiredFilters = BboxRequiredFilters(),
        Branches = [Native("read_parquet", BranchPath("rff_seg.parquet"))],
    };

    /// <summary>Shared column list for <see cref="Hive"/>/<see cref="HiveMixed"/> — matches the
    /// Hive-partitioned glob's native bind output exactly (id, bbox, name, num, theme, type).</summary>
    private static Schema HiveColumns() => new(
        [
            new Field("id", StringType.Default, nullable: true),
            BboxField(),
            new Field("name", StringType.Default, nullable: true),
            new Field("num", Int64Type.Default, nullable: true),
            new Field("theme", StringType.Default, nullable: true),
            new Field("type", StringType.Default, nullable: true),
        ],
        metadata: null);

    private static ScanBranchSpec HiveGlobBranch() => Native(
        "read_parquet", BranchPath("rff_hive/*/*/*.parquet"),
        new Dictionary<string, object?> { ["hive_partitioning"] = true });

    private static CatalogTable BuildHive() => new()
    {
        Name = "rff_hive",
        SchemaName = SchemaName,
        Comment = "required_filters over a native read_parquet Hive-partitioned multi-file glob",
        Columns = HiveColumns(),
        RequiredFilters = BboxRequiredFilters(),
        Branches = [HiveGlobBranch()],
    };

    private static CatalogTable BuildHiveMixed() => new()
    {
        Name = "rff_hive_mixed",
        SchemaName = SchemaName,
        Comment = "required_filters: native Hive delegation requiring a top-level field ('id') + bbox.*",
        Columns = HiveColumns(),
        RequiredFilters = [["id"], .. BboxRequiredFilters()],
        Branches = [HiveGlobBranch()],
    };

    /// <summary>Flat two-column (a, b) shape shared by <see cref="Simple"/> and <see cref="None"/>.</summary>
    private static RecordBatch FlatAb(Schema schema, long[] a, long[] b) => new(
        schema,
        [
            new Int64Array.Builder().AppendRange(a).Build(),
            new Int64Array.Builder().AppendRange(b).Build(),
        ],
        a.Length);

    private static CatalogTable BuildSimple()
    {
        var schema = new Schema(
            [new Field("a", Int64Type.Default, nullable: true), new Field("b", Int64Type.Default, nullable: true)],
            metadata: null);
        var data = FlatAb(schema, [1, 2, 3], [10, 20, 30]);
        return new CatalogTable
        {
            Name = "rff_simple",
            SchemaName = SchemaName,
            Comment = "required_filters: single top-level required path on 'a'",
            RequiredFilters = [["a"]],
            ScanFunction = new StaticRowsFunction("rff_simple_scan", SchemaName, data),
        };
    }

    private static CatalogTable BuildNone()
    {
        var schema = new Schema(
            [new Field("a", Int64Type.Default, nullable: true), new Field("b", Int64Type.Default, nullable: true)],
            metadata: null);
        var data = FlatAb(schema, [1, 2, 3], [10, 20, 30]);
        return new CatalogTable
        {
            Name = "rff_none",
            SchemaName = SchemaName,
            Comment = "required_filters: control table with no requirements",
            ScanFunction = new StaticRowsFunction("rff_none_scan", SchemaName, data),
        };
    }

    /// <summary>Builds a <c>s STRUCT(a BIGINT, b BIGINT)</c> field + its backing array.</summary>
    private static (Field Field, StructArray Array) StructAb(long[] a, long[] b)
    {
        var structType = new StructType(
        [
            new Field("a", Int64Type.Default, nullable: true),
            new Field("b", Int64Type.Default, nullable: true),
        ]);
        var array = new StructArray(
            structType, a.Length,
            [new Int64Array.Builder().AppendRange(a).Build(), new Int64Array.Builder().AppendRange(b).Build()],
            ArrowBuffer.Empty, nullCount: 0);
        return (new Field("s", structType, nullable: true), array);
    }

    private static CatalogTable BuildMulti()
    {
        var (sField, sArray) = StructAb([1, 2], [10, 20]);
        var schema = new Schema([sField, new Field("top", Int64Type.Default, nullable: true)], metadata: null);
        var top = new Int64Array.Builder().AppendRange([100L, 200L]).Build();
        var data = new RecordBatch(schema, [sArray, top], 2);
        return new CatalogTable
        {
            Name = "rff_multi",
            SchemaName = SchemaName,
            Comment = "required_filters: mixed top-level + struct subfield requirements ('top', 's.a')",
            RequiredFilters = [["top"], ["s.a"]],
            ScanFunction = new StaticRowsFunction("rff_multi_scan", SchemaName, data),
        };
    }

    private static CatalogTable BuildStruct()
    {
        var (sField, sArray) = StructAb([1, 2, 3], [10, 20, 30]);
        var schema = new Schema([sField, new Field("other", Int64Type.Default, nullable: true)], metadata: null);
        var other = new Int64Array.Builder().AppendRange([100L, 200L, 300L]).Build();
        var data = new RecordBatch(schema, [sArray, other], 3);
        return new CatalogTable
        {
            Name = "rff_struct",
            SchemaName = SchemaName,
            Comment = "required_filters: struct-subfield required paths ('s.a', 's.b')",
            RequiredFilters = [["s.a"], ["s.b"]],
            ScanFunction = new StaticRowsFunction("rff_struct_scan", SchemaName, data),
        };
    }

    private static CatalogTable BuildNested()
    {
        var leafType = new StructType([new Field("leaf", Int64Type.Default, nullable: true)]);
        var leafArray = new StructArray(
            leafType, 3, [new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build()], ArrowBuffer.Empty, nullCount: 0);
        var midType = new StructType([new Field("mid", leafType, nullable: true)]);
        var midArray = new StructArray(midType, 3, [leafArray], ArrowBuffer.Empty, nullCount: 0);
        var schema = new Schema([new Field("wrapper", midType, nullable: true)], metadata: null);
        var data = new RecordBatch(schema, [midArray], 3);
        return new CatalogTable
        {
            Name = "rff_nested",
            SchemaName = SchemaName,
            Comment = "required_filters: 3-deep nested struct subfield requirement ('wrapper.mid.leaf')",
            RequiredFilters = [["wrapper.mid.leaf"]],
            ScanFunction = new StaticRowsFunction("rff_nested_scan", SchemaName, data),
        };
    }

    /// <summary>Reuses <see cref="Simple"/>'s exact scan-function INSTANCE (not a new one with the
    /// same shape) — mirroring the reference Python/Go workers' "rff_or reuses rff_simple_scan so
    /// it adds no function" fixture note (see <c>table/function_registration.test</c>'s hardcoded
    /// function-count inventory, and <see cref="Internal.CatalogRegistry.RegisterCatalogTable"/>'s
    /// reference-dedup doc comment for why sharing the instance — not just the name/shape — matters).
    /// Declared AFTER <see cref="Simple"/> so its scan function is already built when this runs.</summary>
    private static CatalogTable BuildOr() => new()
    {
        Name = "rff_or",
        SchemaName = SchemaName,
        Comment = "required_filters: OR-group — a filter on 'a' OR 'b' satisfies the requirement",
        RequiredFilters = [["a", "b"]],
        ScanFunction = Simple.ScanFunction,
    };

    private static CatalogTable BuildRowid()
    {
        var bboxType = new StructType(
        [
            new Field("xmin", FloatType.Default, nullable: true),
            new Field("xmax", FloatType.Default, nullable: true),
            new Field("ymin", FloatType.Default, nullable: true),
            new Field("ymax", FloatType.Default, nullable: true),
        ]);

        const int rows = 10;
        var rowId = new Int64Array.Builder();
        var xmin = new FloatArray.Builder();
        var ymin = new FloatArray.Builder();
        var xmax = new FloatArray.Builder();
        var ymax = new FloatArray.Builder();
        var other = new Int64Array.Builder();
        for (var i = 0; i < rows; i++)
        {
            rowId.Append(i);
            xmin.Append(i);
            ymin.Append(2.0f);
            xmax.Append(3.0f);
            ymax.Append(4.0f);
            other.Append(i * 10L);
        }

        var bboxArray = new StructArray(
            bboxType, rows, [xmin.Build(), xmax.Build(), ymin.Build(), ymax.Build()], ArrowBuffer.Empty, nullCount: 0);

        var schema = new Schema(
            [
                new Field("row_id", Int64Type.Default, nullable: true),
                new Field("bbox", bboxType, nullable: true),
                new Field("other", Int64Type.Default, nullable: true),
            ],
            metadata: null);
        var data = new RecordBatch(schema, [rowId.Build(), bboxArray, other.Build()], rows);

        return new CatalogTable
        {
            Name = "rff_rowid",
            SchemaName = SchemaName,
            Comment = "required_filters coexisting with a virtual row-id column",
            RequiredFilters = [["bbox.xmin"], ["bbox.xmax"], ["bbox.ymin"], ["bbox.ymax"]],
            RowIdColumn = "row_id",
            ScanFunction = new RffRowidScanFunction(schema, data),
        };
    }

    /// <summary>Backs <see cref="Rowid"/> — a table with a <c>row_id</c> virtual column MUST
    /// declare projection pushdown (DuckDB rejects a virtual column on a function that doesn't:
    /// "Virtual columns require projection pushdown"), so unlike every other table in this file
    /// this can't just reuse the always-emit-everything <see cref="StaticRowsFunction"/> — it has
    /// to actually narrow its output to <see cref="TableInitParams.ProjectionIds"/>. Registered
    /// under schema <c>main</c> (like every other capability-flag-advertising fixture function in
    /// this worker), not <see cref="SchemaName"/> ("data") — a scan function living in a different
    /// schema than the table it backs is an explicitly supported, ordinary shape (see
    /// <c>VgiTableEntry::GetScanFunctionImpl</c>'s schema-fallback comment).</summary>
    private sealed class RffRowidScanFunction(Schema fullSchema, RecordBatch data) : ITableFunction
    {
        private readonly byte[] _serializedData = RecordBatchIpc.Write(data);
        private readonly ConcurrentDictionary<string, byte[]> _serializedProjections = new();

        public string Name => "rff_rowid_scan";

        public string SchemaName => "main";

        public bool? ProjectionPushdown => true;

        public Schema ArgumentsSchema { get; } = new([], metadata: null);

        public Schema OutputSchema => fullSchema;

        public ITableFunctionProducer CreateProducer(TableInitParams initParams)
        {
            var indices = initParams.ProjectionIds
                ?? Enumerable.Range(0, fullSchema.FieldsList.Count).Select(i => (long)i).ToList();
            var projectionKey = string.Join(',', indices);
            var serializedProjection = _serializedProjections.GetOrAdd(
                projectionKey,
                _ => SerializeProjection(initParams.ProjectedSchema, indices));
            return new Producer(serializedProjection);
        }

        private byte[] SerializeProjection(Schema projectedSchema, IReadOnlyList<long> indices)
        {
            using var fullData = RecordBatchIpc.Read(_serializedData);
            var columns = indices.Select(i => fullData.Column((int)i)).ToList();
            var projectedData = new RecordBatch(projectedSchema, columns, fullData.Length);
            return RecordBatchIpc.Write(projectedData);
        }

        private sealed class Producer(byte[] serializedProjection) : ITableFunctionProducer
        {
            private bool _emitted;

            public void Produce(OutputCollector output)
            {
                if (!_emitted)
                {
                    _emitted = true;
                    // OutputCollector.Emit transfers ownership to vgi-rpc. Each scan therefore
                    // needs its own batch even when another scan uses the same projection.
                    output.Emit(RecordBatchIpc.Read(serializedProjection));
                }

                output.Finish();
            }
        }
    }
}
