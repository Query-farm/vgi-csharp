// ATTACH-time data_version_spec/implementation_version resolution fixture worker — drives
// ~/Development/vgi's test/sql/integration/attach/versioning.test.
//
// IMPORTANT: stdout is the wire channel — never Console.WriteLine here; use Console.Error for
// diagnostics only.
//
// Built and pointed at by DuckDB via:
//
//     ATTACH 'versioned' AS v (TYPE vgi, LOCATION '<path to this executable>');
//
// A DEDICATED single-catalog process, not a catalog folded into ExampleWorker: versioning.test's
// discovery query (SELECT catalog, implementation_version, data_version_spec FROM
// vgi_catalogs(...)) is unfiltered and expects exactly one row — a shared multi-catalog worker
// process would return one row per catalog it serves, which no WHERE clause here narrows.

using QueryFarm.Vgi;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.VersionedWorker;

const string CatalogName = "versioned";

// Newest first — VersionMatch.Resolve's default-when-omitted picks candidates[0].
string[] dataVersionCandidates = ["1.2.0", "1.1.0", "1.0.0"];
string[] implementationVersionCandidates = ["1.0.0"];

var worker = new Worker()
    .CatalogName(CatalogName)
    .RegisterCatalog(new CatalogInfo
    {
        Name = CatalogName,
        ImplementationVersion = "1.0.0",
        DataVersionSpec = ">=1.0.0,<2.0.0",
    })
    .OnAttach(request =>
    {
        var resolvedData = VersionMatch.Resolve(request.DataVersionSpec, dataVersionCandidates)
            ?? throw new InvalidOperationException($"Unsupported data_version_spec: '{request.DataVersionSpec}'");
        var resolvedImpl = VersionMatch.Resolve(request.ImplementationVersion, implementationVersionCandidates)
            ?? throw new InvalidOperationException($"Unsupported implementation_version: '{request.ImplementationVersion}'");

        return new AttachContext
        {
            ResolvedDataVersion = resolvedData,
            ResolvedImplementationVersion = resolvedImpl,
        };
    });

await worker.RunFromArgsAsync(args);
