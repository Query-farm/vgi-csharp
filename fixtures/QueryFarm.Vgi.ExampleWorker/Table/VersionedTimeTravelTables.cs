using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// Shared "resolve an AT clause to a version number" helper for the time-travel fixtures below —
/// <c>VERSION =&gt; N</c> resolves directly (must be a member of <paramref name="validVersions"/>);
/// <c>TIMESTAMP =&gt; ...</c> buckets by year (&lt;=2020 → 1, &lt;=2021 → 2, else → the newest
/// version) — matches vgi-python's/vgi-java's reference fixture semantics exactly (see
/// <c>vgi-python</c>'s <c>vgi/_test_fixtures/table/versioned.py</c>).
/// </summary>
internal static class VersionResolution
{
    public static int Resolve(string atUnit, string atValue, IReadOnlyList<int> validVersions)
    {
        if (string.Equals(atUnit, "VERSION", StringComparison.OrdinalIgnoreCase))
        {
            if (!int.TryParse(atValue, out var version) || !validVersions.Contains(version))
            {
                throw new InvalidOperationException(
                    $"Unknown version: {atValue}. Valid versions: {string.Join(", ", validVersions)}");
            }

            return version;
        }

        if (string.Equals(atUnit, "TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            var yearSpan = atValue.AsSpan(0, Math.Min(4, atValue.Length));
            if (!int.TryParse(yearSpan, out var year))
            {
                throw new InvalidOperationException($"Unknown timestamp: {atValue}");
            }

            if (year < 2020)
            {
                throw new InvalidOperationException("table did not exist before 2020");
            }

            if (year <= 2020)
            {
                return validVersions[0];
            }

            if (year <= 2021)
            {
                return validVersions.Count > 1 ? validVersions[1] : validVersions[^1];
            }

            return validVersions[^1];
        }

        throw new InvalidOperationException($"Unsupported AT clause unit: '{atUnit}'.");
    }
}

/// <summary>
/// <c>example.data.versioned_data</c> — backs <c>table/time_travel.test</c>: VERSION/TIMESTAMP-based
/// time travel over a table whose SCHEMA (not just its data) evolves per version:
/// <list type="bullet">
/// <item>Version 1: <c>(id)</c> — 3 rows.</item>
/// <item>Version 2: <c>(id, name, score, active)</c> — 5 rows.</item>
/// <item>Version 3 (current/default): <c>(id, score)</c> — 4 rows.</item>
/// </list>
/// The table declares <see cref="CatalogTable.SupportsTimeTravel"/> and
/// <see cref="CatalogTable.ResolveAtClause"/>: <c>catalog_table_get</c> swaps in a per-version
/// variant (different <see cref="CatalogTable.Columns"/> AND a different baked <c>version</c> scan
/// argument) so DuckDB's binder sees the RIGHT column list for each <c>AT (...)</c> query (schema
/// evolution errors, e.g. <c>SELECT score ... AT (VERSION =&gt; 1)</c>, are genuine Binder Errors —
/// no worker cooperation needed beyond reporting the right <see cref="TableInfo"/> per version) and
/// the real per-query <c>bind</c> RPC to <see cref="VersionedDataScanFunction"/> below resolves the
/// matching DATA. The version-swap itself lives entirely in <c>CatalogTableGetAsync</c>/this file;
/// <c>VgiServiceImpl</c> stays fixture-agnostic.
/// </summary>
public static class VersionedTimeTravelTables
{
    private const string SchemaName = "data";

    public static CatalogTable VersionedData { get; } = BuildVersionedData();

    public static CatalogTable VersionedConstraints { get; } = BuildVersionedConstraints();

    public static IReadOnlyList<CatalogTable> All { get; } = [VersionedData, VersionedConstraints];

    // ------------------------------------------------------------------
    // versioned_data
    // ------------------------------------------------------------------

    private static readonly IReadOnlyList<int> DataVersions = [1, 2, 3];

    private static readonly Schema V1Schema = new([new Field("id", Int64Type.Default, nullable: false)], metadata: null);

    private static readonly Schema V2Schema = new(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: true),
            new Field("score", DoubleType.Default, nullable: true),
            new Field("active", BooleanType.Default, nullable: true),
        ],
        metadata: null);

    private static readonly Schema V3Schema = new(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("score", DoubleType.Default, nullable: true),
        ],
        metadata: null);

    private static CatalogTable BuildVersionedData()
    {
        var scan = new VersionedDataScanFunction();
        return new CatalogTable
        {
            Name = "versioned_data",
            SchemaName = SchemaName,
            Comment = "Versioned data table demonstrating time travel with schema evolution",
            Columns = V3Schema,
            NotNullColumns = ["id"],
            ScanFunction = scan,
            ScanArguments = [3L],
            SupportsTimeTravel = true,
            ResolveAtClause = (atUnit, atValue) =>
            {
                var version = VersionResolution.Resolve(atUnit, atValue, DataVersions);
                return new CatalogTable
                {
                    Name = "versioned_data",
                    SchemaName = SchemaName,
                    Comment = "Versioned data table demonstrating time travel with schema evolution",
                    Columns = version switch { 1 => V1Schema, 2 => V2Schema, _ => V3Schema },
                    ScanFunction = scan,
                    ScanArguments = [(long)version],
                    SupportsTimeTravel = true,
                };
            },
        };
    }

    /// <summary>The read path for <see cref="VersionedData"/> — takes the resolved <c>version</c> as
    /// a positional argument (baked by <see cref="CatalogTable.ScanArguments"/>/
    /// <see cref="CatalogTable.ResolveAtClause"/> above) and both BINDS a version-specific output
    /// schema and PRODUCES that version's rows. Independently callable as
    /// <c>example.data.versioned_data_scan(&lt;version&gt;)</c> too.</summary>
    public sealed class VersionedDataScanFunction : ITableFunction
    {
        public string Name => "versioned_data_scan";

        public string SchemaName => VersionedTimeTravelTables.SchemaName;

        public string Description => "Returns versioned data with schema evolution";

        public IReadOnlyList<string> Categories => ["generator", "testing"];

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("version", Int64Type.Default)], metadata: null);

        public Schema OutputSchema => V3Schema;

        public void Bind(TableBindParams bindParams) => ValidateVersion(bindParams.Arguments.Int64(0));

        public Schema ResolveOutputSchema(TableBindParams bindParams) => SchemaFor(bindParams.Arguments.Int64(0));

        public ITableFunctionProducer CreateProducer(TableInitParams initParams)
        {
            var version = initParams.Arguments.Int64(0);
            ValidateVersion(version);
            return new Producer(version, initParams.OutputSchema);
        }

        private static void ValidateVersion(long version)
        {
            if (version is < 1 or > 3)
            {
                throw new InvalidOperationException($"Unknown version: {version}. Valid versions: 1, 2, 3");
            }
        }

        private static Schema SchemaFor(long version) => version switch
        {
            1 => V1Schema,
            2 => V2Schema,
            3 => V3Schema,
            _ => throw new InvalidOperationException($"Unknown version: {version}. Valid versions: 1, 2, 3"),
        };

        private sealed class Producer(long version, Schema outputSchema) : ITableFunctionProducer
        {
            private bool _emitted;

            public void Produce(OutputCollector output)
            {
                if (!_emitted)
                {
                    _emitted = true;
                    output.Emit(BuildBatch(version, outputSchema));
                }

                output.Finish();
            }

            private static RecordBatch BuildBatch(long version, Schema schema) => version switch
            {
                1 => new RecordBatch(schema, [Ints([1, 2, 3])], 3),
                2 => new RecordBatch(
                    schema,
                    [
                        Ints([1, 2, 3, 4, 5]),
                        Strings(["alice", "bob", "carol", "dave", "eve"]),
                        Doubles([10.0, 20.0, 30.0, 40.0, 50.0]),
                        Bools([true, false, true, false, true]),
                    ],
                    5),
                _ => new RecordBatch(schema, [Ints([1, 2, 3, 4]), Doubles([15.0, 25.0, 35.0, 45.0])], 4),
            };
        }
    }

    // ------------------------------------------------------------------
    // versioned_constraints
    // ------------------------------------------------------------------

    private static readonly IReadOnlyList<int> ConstraintsVersions = [1, 2, 3];

    private static readonly Schema C1Schema = new(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: true),
        ],
        metadata: null);

    private static readonly Schema C2Schema = new(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: true),
            new Field("email", StringType.Default, nullable: true),
        ],
        metadata: null);

    private static readonly Schema C3Schema = new(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: true),
            new Field("email", StringType.Default, nullable: true),
            new Field("department_id", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    /// <summary>Version 1: <c>(id, name)</c> — NOT NULL id only. Version 2 adds <c>email</c>
    /// (PK id, UNIQUE email). Version 3 (current/default) adds <c>department_id</c> (+ FK to
    /// <c>departments.id</c>). <c>duckdb_constraints()</c> always reflects the DEFAULT (version 3)
    /// registration — the AT-resolved variants below carry NO constraints of their own (constraint
    /// column indices are resolved against each variant's OWN <see cref="CatalogTable.Columns"/>,
    /// and versions 1/2 are missing columns the v3 constraint set references) — see
    /// <c>table/constraints_time_travel.test</c>'s own doc comment: "Time travel changes schema/data
    /// for queries but not the constraint metadata."</summary>
    private static CatalogTable BuildVersionedConstraints()
    {
        var scan = new VersionedConstraintsScanFunction();
        return new CatalogTable
        {
            Name = "versioned_constraints",
            SchemaName = SchemaName,
            Comment = "Table with constraints that evolve across versions",
            Columns = C3Schema,
            NotNullColumns = ["id", "name"],
            PrimaryKeyColumns = ["id"],
            UniqueColumns = [["email"]],
            ForeignKeys = [new CatalogForeignKey
            {
                Columns = ["department_id"],
                ReferencedTable = "departments",
                ReferencedColumns = ["id"],
                ReferencedSchema = SchemaName,
            }],
            ScanFunction = scan,
            ScanArguments = [3L],
            SupportsTimeTravel = true,
            ResolveAtClause = (atUnit, atValue) =>
            {
                var version = VersionResolution.Resolve(atUnit, atValue, ConstraintsVersions);
                return new CatalogTable
                {
                    Name = "versioned_constraints",
                    SchemaName = SchemaName,
                    Comment = "Table with constraints that evolve across versions",
                    Columns = version switch { 1 => C1Schema, 2 => C2Schema, _ => C3Schema },
                    ScanFunction = scan,
                    ScanArguments = [(long)version],
                    SupportsTimeTravel = true,
                };
            },
        };
    }

    public sealed class VersionedConstraintsScanFunction : ITableFunction
    {
        public string Name => "versioned_constraints_scan";

        public string SchemaName => VersionedTimeTravelTables.SchemaName;

        public string Description => "Returns versioned data for constraint evolution testing";

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("version", Int64Type.Default)], metadata: null);

        public Schema OutputSchema => C3Schema;

        public void Bind(TableBindParams bindParams) => ValidateVersion(bindParams.Arguments.Int64(0));

        public Schema ResolveOutputSchema(TableBindParams bindParams) => SchemaFor(bindParams.Arguments.Int64(0));

        public ITableFunctionProducer CreateProducer(TableInitParams initParams)
        {
            var version = initParams.Arguments.Int64(0);
            ValidateVersion(version);
            return new Producer(version, initParams.OutputSchema);
        }

        private static void ValidateVersion(long version)
        {
            if (version is < 1 or > 3)
            {
                throw new InvalidOperationException($"Unknown version: {version}");
            }
        }

        private static Schema SchemaFor(long version) => version switch
        {
            1 => C1Schema,
            2 => C2Schema,
            3 => C3Schema,
            _ => throw new InvalidOperationException($"Unknown version: {version}"),
        };

        private sealed class Producer(long version, Schema outputSchema) : ITableFunctionProducer
        {
            private bool _emitted;

            public void Produce(OutputCollector output)
            {
                if (!_emitted)
                {
                    _emitted = true;
                    output.Emit(BuildBatch(version, outputSchema));
                }

                output.Finish();
            }

            private static RecordBatch BuildBatch(long version, Schema schema) => version switch
            {
                1 => new RecordBatch(schema, [Ints([1, 2]), Strings(["Alice", "Bob"])], 2),
                2 => new RecordBatch(
                    schema,
                    [Ints([1, 2, 3]), Strings(["Alice", "Bob", "Carol"]), Strings(["a@co", "b@co", "c@co"])],
                    3),
                _ => new RecordBatch(
                    schema,
                    [
                        Ints([1, 2, 3]),
                        Strings(["Alice", "Bob", "Carol"]),
                        Strings(["a@co", "b@co", "c@co"]),
                        Ints([1, 2, 1]),
                    ],
                    3),
            };
        }
    }

    // ------------------------------------------------------------------
    // Shared array builders
    // ------------------------------------------------------------------

    private static Int64Array Ints(IEnumerable<long> values) => new Int64Array.Builder().AppendRange(values).Build();

    private static StringArray Strings(IEnumerable<string> values) => new StringArray.Builder().AppendRange(values).Build();

    private static DoubleArray Doubles(IEnumerable<double> values) => new DoubleArray.Builder().AppendRange(values).Build();

    private static BooleanArray Bools(IEnumerable<bool> values) => new BooleanArray.Builder().AppendRange(values).Build();
}
