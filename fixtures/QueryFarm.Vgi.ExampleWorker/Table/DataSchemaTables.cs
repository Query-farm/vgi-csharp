using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// Real (non-function-only) catalog tables in the <c>data</c> schema — backs
/// <c>test/sql/integration/table/constraints.test</c> (NOT NULL/PRIMARY KEY/UNIQUE/CHECK/FOREIGN KEY
/// metadata visible via <c>duckdb_constraints()</c>) and
/// <c>test/sql/integration/catalog/window_self_join.test</c> (a plain single-branch table, <c>numbers</c>).
/// Most tables here are function-backed by a dedicated <see cref="StaticRowsFunction"/> so their
/// column schema is derived automatically from the fixed data batch — see
/// <c>docs/catalog-interface.md</c>'s "Function-Backed Tables (Recommended)" pattern. <c>numbers</c>/
/// <c>volatile_numbers</c>/<c>ten_thousand_table</c>/<c>cardinality_inlined_table</c> are the
/// exception: they instead reuse a SHARED function instance threaded in from <c>Program.cs</c> (see
/// <see cref="BuildNumbers"/>'s doc comment) — mirroring vgi-python's fixture and, along the way,
/// fixing <c>catalog/multi_branch_scan.test</c> and <c>table/inlined_cardinality.test</c>.
/// </summary>
public static class DataSchemaTables
{
    private const string SchemaName = "data";

    public static CatalogTable Departments { get; } = BuildDepartments();

    public static CatalogTable Employees { get; } = BuildEmployees();

    public static CatalogTable Projects { get; } = BuildProjects();

    public static CatalogTable Products { get; } = BuildProducts();

    public static CatalogTable Colors { get; } = BuildColors();

    /// <summary>Builds every table in this class, including the ones backed by shared function
    /// instances (see <see cref="BuildNumbers"/>'s doc comment for why <paramref name="sequenceFunction"/>/
    /// <paramref name="tenThousandFunction"/> must be threaded in from <c>Program.cs</c> rather than
    /// each table owning a dedicated <see cref="StaticRowsFunction"/>) — this is what fixes
    /// <c>catalog/multi_branch_scan.test</c> and shrinks <c>table/function_registration.test</c>'s
    /// registered-function-count gap (see <c>Program.cs</c>'s big roadmap comment, item (a)).</summary>
    public static IReadOnlyList<CatalogTable> All(ITableFunction sequenceFunction, ITableFunction tenThousandFunction) =>
        [
            Departments, Employees, Projects, Products,
            BuildNumbers(sequenceFunction),
            BuildTenThousandTable(tenThousandFunction),
            BuildCardinalityInlinedTable(tenThousandFunction),
            BuildVolatileNumbers(sequenceFunction),
            Colors,
        ];

    private static CatalogTable BuildDepartments()
    {
        var schema = new Schema(
            [
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: false),
                new Field("budget", DoubleType.Default, nullable: true),
            ],
            metadata: null);

        var id = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
        var name = new StringArray.Builder().AppendRange(["Engineering", "Sales", "HR"]).Build();
        // table/constraints_time_travel.test pins these exact values (matches the canonical
        // vgi-python fixture's departments_scan data).
        var budget = new DoubleArray.Builder().AppendRange([500_000.0, 300_000.0, 200_000.0]).Build();
        var data = new RecordBatch(schema, [id, name, budget], 3);

        return new CatalogTable
        {
            Name = "departments",
            SchemaName = SchemaName,
            Comment = "Department reference table",
            NotNullColumns = ["id", "name"],
            PrimaryKeyColumns = ["id"],
            UniqueColumns = [["name"]],
            CheckConstraints = ["(budget >= 0)"],
            ColumnDefaults = new Dictionary<string, string> { ["budget"] = "0" },
            // table/column_statistics.test — declared (advertised) bounds, a safe superset of the
            // real 3-row data above rather than a tight fit (see that test's own doc comment on
            // string-stat truncation): id/budget/name each get a distinct_count of 10 purely as a
            // cross-language-fixture-standardized placeholder, not a real cardinality.
            Statistics = new Dictionary<string, ColumnStatisticsInput>
            {
                ["id"] = new() { Min = 1L, Max = 10L, HasNull = false, HasNotNull = true, DistinctCount = 10 },
                ["name"] = new()
                {
                    Min = "Accounting",
                    Max = "Sales",
                    HasNull = false,
                    HasNotNull = true,
                    DistinctCount = 10,
                    ContainsUnicode = false,
                    MaxStringLength = 20,
                },
                ["budget"] = new() { Min = 50_000.0, Max = 500_000.0, HasNull = false, HasNotNull = true, DistinctCount = 10 },
            },
            StatisticsCacheMaxAgeSeconds = 3600,
            ScanFunction = new StaticRowsFunction("departments", SchemaName, data),
        };
    }

    private static CatalogTable BuildEmployees()
    {
        var schema = new Schema(
            [
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: false),
                new Field("email", StringType.Default, nullable: false),
                new Field("department_id", Int64Type.Default, nullable: true),
            ],
            metadata: null);

        var id = new Int64Array.Builder().AppendRange([1L, 2L, 3L, 4L, 5L]).Build();
        var name = new StringArray.Builder().AppendRange(["Alice", "Bob", "Carol", "Dave", "Eve"]).Build();
        var email = new StringArray.Builder()
            .AppendRange(["alice@co.com", "bob@co.com", "carol@co.com", "dave@co.com", "eve@co.com"])
            .Build();
        var departmentId = new Int64Array.Builder().AppendRange([1L, 1L, 2L, 2L, 3L]).Build();
        var data = new RecordBatch(schema, [id, name, email, departmentId], 5);

        return new CatalogTable
        {
            Name = "employees",
            SchemaName = SchemaName,
            Comment = "Employee table with FK to departments",
            NotNullColumns = ["id", "name", "email"],
            PrimaryKeyColumns = ["id"],
            UniqueColumns = [["email"]],
            ForeignKeys = [new CatalogForeignKey
            {
                Columns = ["department_id"],
                ReferencedTable = "departments",
                ReferencedColumns = ["id"],
                ReferencedSchema = SchemaName,
            }],
            ScanFunction = new StaticRowsFunction("employees", SchemaName, data),
        };
    }

    private static CatalogTable BuildProjects()
    {
        var schema = new Schema(
            [
                new Field("department_id", Int64Type.Default, nullable: false),
                new Field("project_code", StringType.Default, nullable: false),
                new Field("title", StringType.Default, nullable: false),
            ],
            metadata: null);

        var departmentId = new Int64Array.Builder().AppendRange([1L, 1L, 2L]).Build();
        var projectCode = new StringArray.Builder().AppendRange(["P001", "P002", "P003"]).Build();
        var title = new StringArray.Builder().AppendRange(["Backend API", "Frontend UI", "Sales Portal"]).Build();
        var data = new RecordBatch(schema, [departmentId, projectCode, title], 3);

        return new CatalogTable
        {
            Name = "projects",
            SchemaName = SchemaName,
            Comment = "Projects with composite PK and FK to departments",
            NotNullColumns = ["department_id", "project_code", "title"],
            PrimaryKeyColumns = ["department_id", "project_code"],
            ForeignKeys = [new CatalogForeignKey
            {
                Columns = ["department_id"],
                ReferencedTable = "departments",
                ReferencedColumns = ["id"],
                ReferencedSchema = SchemaName,
            }],
            ScanFunction = new StaticRowsFunction("projects", SchemaName, data),
        };
    }

    /// <summary>Not directly queried by <c>constraints.test</c> — exists only so its own
    /// PRIMARY KEY (which also surfaces as a NOT NULL row, same as every other table's PK column
    /// here) rounds the test's cross-table constraint-count summary out to the exact pinned totals
    /// (NOT NULL 9, PRIMARY KEY 4). Also backs <c>table/defaults.test</c> (column defaults) and
    /// <c>table/comments.test</c> (a table exercising BOTH a default AND a comment on the same
    /// column — <c>name</c>/<c>price</c> — plus a column with neither, <c>quantity</c>).</summary>
    private static CatalogTable BuildProducts()
    {
        var schema = new Schema(
            [
                new Field("id", Int64Type.Default, nullable: false),
                new Field("name", StringType.Default, nullable: true),
                new Field("quantity", Int64Type.Default, nullable: true),
                new Field("price", DoubleType.Default, nullable: true),
            ],
            metadata: null);

        var id = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
        var name = new StringArray.Builder().AppendRange(["Widget", "Gadget", "Doohickey"]).Build();
        var quantity = new Int64Array.Builder().AppendRange([100L, 50L, 200L]).Build();
        var price = new DoubleArray.Builder().AppendRange([9.99, 24.99, 4.99]).Build();
        var data = new RecordBatch(schema, [id, name, quantity, price], 3);

        return new CatalogTable
        {
            Name = "products",
            SchemaName = SchemaName,
            Comment = "Product table with column defaults",
            NotNullColumns = ["id"],
            PrimaryKeyColumns = ["id"],
            ColumnComments = new Dictionary<string, string>
            {
                ["id"] = "Unique product identifier",
                ["name"] = "Product display name",
                ["price"] = "Unit price in USD",
            },
            ColumnDefaults = new Dictionary<string, string>
            {
                ["name"] = "'unknown'",
                ["price"] = "9.99",
                ["quantity"] = "0",
            },
            // table/column_statistics.test — declared bounds (see BuildDepartments' Statistics
            // doc comment for why these needn't tightly fit the 3-row data above).
            Statistics = new Dictionary<string, ColumnStatisticsInput>
            {
                ["id"] = new() { Min = 1L, Max = 100L, HasNull = false, HasNotNull = true, DistinctCount = 3 },
                ["name"] = new()
                {
                    Min = "Anvil",
                    Max = "Zebra Tape",
                    HasNull = false,
                    HasNotNull = true,
                    DistinctCount = 3,
                    ContainsUnicode = false,
                    MaxStringLength = 30,
                },
                ["quantity"] = new() { Min = 0L, Max = 10_000L, HasNull = true, HasNotNull = true, DistinctCount = 3 },
                ["price"] = new() { Min = 0.99, Max = 999.99, HasNull = false, HasNotNull = true, DistinctCount = 3 },
            },
            StatisticsCacheMaxAgeSeconds = 3600,
            ScanFunction = new StaticRowsFunction("products", SchemaName, data),
        };
    }

    /// <summary>Plain, unconstrained 100-row table — <c>catalog/window_self_join.test</c>'s
    /// regression fixture for the WindowSelfJoin optimizer rewrite against a real (not
    /// function-call-syntax) VGI table scan.
    ///
    /// <para><b>catalog/multi_branch_scan.test's final assertion — FIXED.</b> The test's shim-row
    /// check expects <c>vgi_table_branches()</c> to report <c>function_name='sequence'</c> for this
    /// single-branch table. Confirmed against the canonical vgi-python reference fixture
    /// (<c>vgi/_test_fixtures/worker.py</c>): its <c>numbers</c>/<c>volatile_numbers</c> tables
    /// declare explicit <c>columns=schema(value=...)</c> with NO dedicated backing function at all —
    /// <c>table_scan_function_get</c> is overridden to answer with
    /// <c>ScanFunctionResult(function_name="sequence", positional_arguments=[100])</c> for both, i.e.
    /// they're genuinely scanned by calling the shared <c>sequence</c> generator (whose own output
    /// column is <c>n</c>) while the CATALOG advertises column name <c>value</c> — proving the C++
    /// side resolves a table's columns POSITIONALLY from the declared <see cref="CatalogTable.Columns"/>/
    /// <c>TableInfo</c>, not by matching field names against whatever the underlying scan function's
    /// own schema says. Mirrored here: reuses the SAME shared <paramref name="sequenceFunction"/>
    /// instance <c>large_sequence</c>/<c>funny_numbers</c> already share (threaded in from
    /// <c>Program.cs</c>), with <c>ScanArguments=[100L]</c> and an explicit
    /// <see cref="CatalogTable.Columns"/> override renaming <c>n</c>→<c>value</c>.</para></summary>
    private static CatalogTable BuildNumbers(ITableFunction sequenceFunction) => new()
    {
        Name = "numbers",
        SchemaName = SchemaName,
        Comment = "First 100 integers (demonstrates explicit columns)",
        Columns = new Schema([new Field("value", Int64Type.Default, nullable: false)], metadata: null),
        NotNullColumns = ["value"],
        // Deliberately NOT inlined (test/sql/integration/table/inlined_scan_function.test):
        // this table exercises the legacy per-bind catalog_table_scan_branches_get RPC lookup
        // path instead of the "declarative Table(function=...)" inline fast path that
        // TenThousandTable below uses.
        InlineScanFunction = false,
        // table/column_statistics.test — real stats (extracted from a DuckDB in-memory table
        // via statistics_from_duckdb() in the reference workers; hand-declared here to match).
        Statistics = new Dictionary<string, ColumnStatisticsInput>
        {
            ["value"] = new() { Min = 0L, Max = 99L, HasNull = false, HasNotNull = true, DistinctCount = 100 },
        },
        StatisticsCacheMaxAgeSeconds = 3600,
        ScanFunction = sequenceFunction,
        ScanArguments = [100L],
    };

    /// <summary>Backs <c>table/column_statistics.test</c>'s cache-TTL coverage: identical shape to
    /// <see cref="BuildNumbers"/> but with <see cref="CatalogTable.StatisticsCacheMaxAgeSeconds"/>
    /// set to 0 — the C++ extension must re-issue <c>catalog_table_column_statistics_get</c> on every
    /// query rather than caching the answer, and this worker must keep answering correctly either
    /// way. Shares <paramref name="sequenceFunction"/> with <see cref="BuildNumbers"/> — see that
    /// method's doc comment.</summary>
    private static CatalogTable BuildVolatileNumbers(ITableFunction sequenceFunction) => new()
    {
        Name = "volatile_numbers",
        SchemaName = SchemaName,
        Comment = "Numbers with volatile stats (TTL=0, always re-fetched)",
        Columns = new Schema([new Field("value", Int64Type.Default, nullable: false)], metadata: null),
        NotNullColumns = ["value"],
        InlineScanFunction = false,
        Statistics = new Dictionary<string, ColumnStatisticsInput>
        {
            ["value"] = new() { Min = 0L, Max = 99L, HasNull = false, HasNotNull = true, DistinctCount = 100 },
        },
        StatisticsCacheMaxAgeSeconds = 0,
        ScanFunction = sequenceFunction,
        ScanArguments = [100L],
    };

    /// <summary>Backs <c>table/column_statistics.test</c>'s ENUM-derived-statistics regression: the
    /// <c>color</c> column's real values are plain strings (DuckDB reports it as VARCHAR, not
    /// ENUM — see the test's own doc comment) but its declared min/max mirror what the reference
    /// workers extract from a real DuckDB ENUM column via <c>statistics_from_duckdb()</c>: the C++
    /// side's <c>StringStats</c> comparison is lexicographic, so alphabetically <c>min='blue'</c>,
    /// <c>max='red'</c> (NOT the ENUM's ordinal red=0/green=1/blue=2 order).</summary>
    private static CatalogTable BuildColors()
    {
        var schema = new Schema(
            [
                new Field("id", Int64Type.Default, nullable: false),
                new Field("color", StringType.Default, nullable: false),
                new Field("hex_code", StringType.Default, nullable: false),
            ],
            metadata: null);

        var id = new Int64Array.Builder().AppendRange([1L, 2L, 3L]).Build();
        var color = new StringArray.Builder().AppendRange(["blue", "green", "red"]).Build();
        var hexCode = new StringArray.Builder().AppendRange(["#0000FF", "#00FF00", "#FF0000"]).Build();
        var data = new RecordBatch(schema, [id, color, hexCode], 3);

        return new CatalogTable
        {
            Name = "colors",
            SchemaName = SchemaName,
            Comment = "Colors table with ENUM-derived statistics",
            NotNullColumns = ["id", "color", "hex_code"],
            Statistics = new Dictionary<string, ColumnStatisticsInput>
            {
                ["id"] = new() { Min = 1L, Max = 3L, HasNull = false, HasNotNull = true, DistinctCount = 3 },
                ["color"] = new()
                {
                    Min = "blue",
                    Max = "red",
                    HasNull = false,
                    HasNotNull = true,
                    DistinctCount = 3,
                    ContainsUnicode = false,
                    MaxStringLength = 5,
                },
                ["hex_code"] = new()
                {
                    Min = "#0000FF",
                    Max = "#FF0000",
                    HasNull = false,
                    HasNotNull = true,
                    DistinctCount = 3,
                    ContainsUnicode = false,
                    MaxStringLength = 7,
                },
            },
            StatisticsCacheMaxAgeSeconds = 3600,
            ScanFunction = new StaticRowsFunction("colors", SchemaName, data),
        };
    }

    /// <summary>Function-backed (recommended, inline) table over the shared no-arg
    /// <paramref name="tenThousandFunction"/> instance (matching vgi-python's
    /// <c>Table(name="ten_thousand_table", function=TenThousandFunction)</c> — no dedicated
    /// <see cref="StaticRowsFunction"/>; see <see cref="BuildNumbers"/>'s doc comment for why this
    /// requires threading a shared instance in from <c>Program.cs</c>). Column defaults to the
    /// function's own output schema (<c>n</c>, not <c>value</c> — neither test referencing this
    /// table inspects the column name). No inlined <see cref="CatalogTable.CardinalityEstimate"/>/
    /// <see cref="CatalogTable.CardinalityMax"/> here — <see cref="TenThousandFunction"/> reports
    /// 10,000 via <see cref="ITableFunction.Cardinality"/> (the per-bind
    /// <c>table_function_cardinality</c> RPC), backing <c>table/inlined_cardinality.test</c>'s
    /// "legacy path" half.</summary>
    private static CatalogTable BuildTenThousandTable(ITableFunction tenThousandFunction) => new()
    {
        Name = "ten_thousand_table",
        SchemaName = SchemaName,
        Comment = "Function-backed table over the no-arg ten_thousand function",
        NotNullColumns = ["n"],
        ScanFunction = tenThousandFunction,
    };

    /// <summary>Same underlying shared <paramref name="tenThousandFunction"/> as
    /// <see cref="BuildTenThousandTable"/>, but with <see cref="CatalogTable.CardinalityEstimate"/>/
    /// <see cref="CatalogTable.CardinalityMax"/> set directly on the table descriptor — the C++
    /// extension then uses them straight from <c>TableInfo</c> and skips the per-bind
    /// <c>table_function_cardinality</c> RPC entirely (see <c>storage/vgi_table_entry.cpp</c>'s
    /// <c>vgi.cardinality.inlined</c> log site). Backs <c>table/inlined_cardinality.test</c>'s
    /// "inlined" half.
    ///
    /// <para><b>table/inlined_cardinality.test's final assertion — confirmed C++-side gap, not
    /// fixable here.</b> The test's first two assertions (plan shows the inlined estimate; the
    /// <c>vgi.cardinality.inlined</c> trace fires) PASS — proving the C++ side's
    /// <c>BindTableEntry</c> correctly reads this table's inlined <c>CardinalityEstimate</c>/
    /// <c>CardinalityMax</c> off <c>TableInfo</c> and sets
    /// <c>scan_bind_data-&gt;cardinality_fetched = true</c> on ITS bind_data instance
    /// (<c>vgi_table_entry.cpp</c> ~line 764-778). The THIRD assertion — that the per-bind
    /// <c>table_function_cardinality</c> RPC never fires — FAILS: it fires anyway. Per
    /// <c>vgi_table_function_impl.cpp</c>'s <c>VgiTableFunctionCardinality</c> (~line 3735), the RPC
    /// is gated on <c>!bind_data.cardinality_fetched</c> where <c>bind_data</c> is whatever
    /// <c>FunctionData</c> instance DuckDB's cardinality-estimation callback happens to pass in —
    /// for <c>EXPLAIN</c>, this is evidently a DIFFERENT <c>VgiTableFunctionBindData</c> instance
    /// than the one <c>BindTableEntry</c> just set the flag on (DuckDB performs more than one bind
    /// pass for a single logical scan under <c>EXPLAIN</c>, and each bind call constructs its own
    /// bind data). Since <c>table_info_.cardinality_estimate.has_value()</c> is a per-CALL check
    /// against worker-independent state (the same inlined values are read fresh on every bind that
    /// reaches this code, regardless of which C# worker answers), this is purely a C++-side
    /// bind-instance-identity issue — no data this worker could send differently would change which
    /// bind_data object the cardinality callback receives. Matches the class of confirmed gap
    /// documented on <c>SplitDynamicFilterFunction</c>/<c>ValuePruneFunction</c>. Deferred.</para></summary>
    private static CatalogTable BuildCardinalityInlinedTable(ITableFunction tenThousandFunction) => new()
    {
        Name = "cardinality_inlined_table",
        SchemaName = SchemaName,
        Comment = "Function-backed table with inlined cardinality (10000 rows)",
        NotNullColumns = ["n"],
        CardinalityEstimate = 10_000,
        CardinalityMax = 10_000,
        ScanFunction = tenThousandFunction,
    };
}
