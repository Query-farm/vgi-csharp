// ATTACH-time data_version_spec-shaped table-set fixture worker — drives ~/Development/vgi's
// test/sql/integration/attach/versioned_tables{,_impl,_resolved,_spec}.test.
//
// IMPORTANT: stdout is the wire channel — never Console.WriteLine here; use Console.Error for
// diagnostics only.
//
// Built and pointed at by DuckDB via:
//
//     ATTACH 'versioned_tables' AS v (TYPE vgi, LOCATION '<path to this executable>');
//
// A DEDICATED single-catalog process, not a catalog folded into ExampleWorker: these tests'
// discovery query (SELECT catalog, implementation_version, data_version_spec FROM vgi_catalogs(...))
// is unfiltered and expects exactly one row — a shared multi-catalog worker process would return
// one row per catalog it serves, which no WHERE clause here narrows.
//
// data_version_spec shapes the visible TABLE SET (1.0.0/1.1.0 -> animals only; 2.0.0 -> animals +
// plants; 3.0.0 -> plants only), plus 1.1.0's schema evolution (animals gains a `color` column).
// Attach-time only — none of the driving test files ever issue an AT (...) clause, so this does
// NOT use CatalogTable.SupportsTimeTravel/ResolveAtClause (that machinery is for a DIFFERENT,
// per-query time-travel shape; see table/time_travel.test). Each resolved data version gets its
// own isolated Worker.RegisterCatalogTable bucket under a composite identity
// "versioned_tables@<version>" — same identity-bucket mechanism same_name_catalogs.test's
// twin_a/twin_b prove safe, chosen dynamically per attach instead of statically per Register* call.

using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.VersionedTablesWorker;

const string CatalogName = "versioned_tables";

// Newest first — VersionMatch.Resolve's default-when-omitted picks candidates[0].
string[] dataVersionCandidates = ["3.0.0", "2.0.0", "1.1.0", "1.0.0"];
string[] implementationVersionCandidates = ["11.0.0", "10.1.0", "10.0.0"];

static string Identity(string resolvedDataVersion) => $"versioned_tables@{resolvedDataVersion}";

var animalsNoColor = BuildAnimals(withColor: false);
var animalsWithColor = BuildAnimals(withColor: true);
var plants = BuildPlants();

var worker = new Worker()
    .CatalogName(CatalogName)
    .RegisterCatalog(new CatalogInfo
    {
        Name = CatalogName,
        ImplementationVersion = "11.0.0",
        DataVersionSpec = ">=1.0.0,<4.0.0",
    })
    .OnAttach(request =>
    {
        var resolvedData = VersionMatch.Resolve(request.DataVersionSpec, dataVersionCandidates)
            ?? throw new InvalidOperationException($"Unsupported data_version_spec: '{request.DataVersionSpec}'");
        var resolvedImpl = VersionMatch.Resolve(request.ImplementationVersion, implementationVersionCandidates)
            ?? throw new InvalidOperationException($"Unsupported implementation_version: '{request.ImplementationVersion}'");

        return new AttachContext
        {
            Identity = Identity(resolvedData),
            ResolvedDataVersion = resolvedData,
            ResolvedImplementationVersion = resolvedImpl,
        };
    });

foreach (var version in new[] { "1.0.0", "1.1.0", "2.0.0", "3.0.0" })
{
    worker.MarkIdentityExclusive(Identity(version));
}

// 1.0.0: animals only, pre-evolution (3 columns).
worker.RegisterCatalogTable(new CatalogTable
{
    Name = "animals",
    SchemaName = "main",
    ScanFunction = new StaticRowsFunction("animals", "main", animalsNoColor),
}, identity: Identity("1.0.0"));

// 1.1.0: animals only, post-evolution (adds `color`).
worker.RegisterCatalogTable(new CatalogTable
{
    Name = "animals",
    SchemaName = "main",
    ScanFunction = new StaticRowsFunction("animals", "main", animalsWithColor),
}, identity: Identity("1.1.0"));

// 2.0.0: animals (evolved shape persists forward) + plants.
worker.RegisterCatalogTable(new CatalogTable
{
    Name = "animals",
    SchemaName = "main",
    ScanFunction = new StaticRowsFunction("animals", "main", animalsWithColor),
}, identity: Identity("2.0.0"));
worker.RegisterCatalogTable(new CatalogTable
{
    Name = "plants",
    SchemaName = "main",
    ScanFunction = new StaticRowsFunction("plants", "main", plants),
}, identity: Identity("2.0.0"));

// 3.0.0: plants only — animals is hidden entirely (SELECT ... FROM v3.main.animals must fail as
// an ordinary "table not found", not any AT-clause-specific error).
worker.RegisterCatalogTable(new CatalogTable
{
    Name = "plants",
    SchemaName = "main",
    ScanFunction = new StaticRowsFunction("plants", "main", plants),
}, identity: Identity("3.0.0"));

await worker.RunFromArgsAsync(args);

static RecordBatch BuildAnimals(bool withColor)
{
    // name, legs, sound (+ color at 1.1.0+) — column order matches
    // versioned_tables_spec.test's information_schema.columns assertion.
    string[] names = ["chicken", "cow", "horse", "pig", "sheep"];
    long[] legs = [2, 4, 4, 4, 4];
    string[] sounds = ["cluck", "moo", "neigh", "oink", "baa"];
    string[] colors = ["yellow", "brown", "brown", "pink", "white"];

    var fields = new List<Field>
    {
        new("name", StringType.Default, nullable: true),
        new("legs", Int64Type.Default, nullable: true),
        new("sound", StringType.Default, nullable: true),
    };
    var arrays = new List<IArrowArray>
    {
        new StringArray.Builder().AppendRange(names).Build(),
        new Int64Array.Builder().AppendRange(legs).Build(),
        new StringArray.Builder().AppendRange(sounds).Build(),
    };

    if (withColor)
    {
        fields.Add(new Field("color", StringType.Default, nullable: true));
        arrays.Add(new StringArray.Builder().AppendRange(colors).Build());
    }

    var schema = new Schema(fields, metadata: null);
    return new RecordBatch(schema, arrays, names.Length);
}

static RecordBatch BuildPlants()
{
    string[] names = ["oak", "pine", "rose", "tomato", "wheat"];
    string[] kinds = ["tree", "tree", "flower", "vegetable", "grass"];
    double[] heights = [20.0, 25.0, 0.6, 1.5, 1.0];

    var schema = new Schema(
    [
        new Field("name", StringType.Default, nullable: true),
        new Field("kind", StringType.Default, nullable: true),
        new Field("height_m", DoubleType.Default, nullable: true),
    ], metadata: null);

    var arrays = new List<IArrowArray>
    {
        new StringArray.Builder().AppendRange(names).Build(),
        new StringArray.Builder().AppendRange(kinds).Build(),
        new DoubleArray.Builder().AppendRange(heights).Build(),
    };

    return new RecordBatch(schema, arrays, names.Length);
}
