using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.ExampleWorker.Aggregate;
using QueryFarm.Vgi.ExampleWorker.Scalar;
using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.ExampleWorker.CatalogContents;

/// <summary>
/// The catalogs for the <c>catalog_contents</c> RPC (protocol 2.1.0), mirroring vgi-python's
/// <c>vgi/_test_fixtures/catalog_contents.py</c> and driven by the extension's
/// <c>test/sql/integration/catalog/catalog_contents*.test</c>.
/// <list type="bullet">
/// <item><c>example</c> opts in to the framework's content-hash etag
/// (<see cref="CatalogContentsEtagMode.ContentHash"/>) — <c>catalog_contents_conformance.test</c>'s
/// conditional calls run against it.</item>
/// <item>The same static two-schema catalog (<c>main</c>: every object kind; <c>extra</c>: one
/// table) served three ways: <c>contents_probe</c> advertises and serves <c>catalog_contents</c>
/// (version-frozen, no etag); <c>contents_broken</c> advertises it but its <c>catalog_contents</c>
/// throws, so a client falls back to the per-schema RPCs; <c>contents_legacy</c> does not
/// advertise it, like an older worker.</item>
/// <item>Three DDL-capable in-memory catalogs (<see cref="Worker.RegisterInMemoryCatalog"/>),
/// private per ATTACH: <c>contents_memory</c> reports <c>catalog_version</c> 0 and no etag (the
/// client's version-0 rule); <c>contents_reval</c> revalidates with the cheap etag
/// <c>gen-&lt;version&gt;</c>, answering a matching <c>if_none_match</c> before building
/// anything; <c>contents_hash</c> returns no etag of its own and uses the framework's content
/// hash.</item>
/// </list>
/// </summary>
internal static class CatalogContentsSetup
{
    public const string Probe = "contents_probe";
    public const string Broken = "contents_broken";
    public const string Legacy = "contents_legacy";
    public const string Memory = "contents_memory";
    public const string Reval = "contents_reval";
    public const string Hash = "contents_hash";

    public const string BrokenMessage = "contents_broken: catalog_contents deliberately fails";

    public static Worker Register(Worker worker)
    {
        worker.CatalogContentsEtag(CatalogContentsEtagMode.ContentHash, identity: "example");

        foreach (var name in new[] { Probe, Broken, Legacy })
        {
            RegisterStaticCatalog(worker, name);
        }

        worker.OnCatalogContents(Broken, _ => throw new InvalidOperationException(BrokenMessage));

        worker
            .RegisterInMemoryCatalog(Memory, new InMemoryCatalogOptions { ReportsVersion = false })
            .RegisterInMemoryCatalog(Reval)
            .OnCatalogContents(Reval, async request =>
            {
                var etag = $"gen-{request.CatalogVersion}";
                return request.IfNoneMatch == etag
                    ? CatalogContentsResult.Unchanged(etag)
                    : await request.BuildAsync() with { Etag = etag };
            })
            .RegisterInMemoryCatalog(Hash)
            .CatalogContentsEtag(CatalogContentsEtagMode.ContentHash, identity: Hash);
        return worker;
    }

    /// <summary>Chained into the worker's <c>OnAttach</c>: <c>contents_legacy</c> withdraws the
    /// <c>catalog_contents</c> advertisement for its attaches.</summary>
    public static AttachContext? Handle(CatalogAttachRequest request) =>
        request.Name == Legacy ? new AttachContext { SupportsCatalogContents = false } : null;

    private static void RegisterStaticCatalog(Worker worker, string name)
    {
        // One instance per catalog: the tables below reuse it by reference, so they add no
        // function of their own.
        var sequence = new SequenceFunction();
        worker
            .RegisterCatalog(new CatalogInfo { Name = name }, exclusive: true)
            .RegisterSchema("main", "Every object kind", identity: name)
            .RegisterSchema("extra", "A second schema, tables only", identity: name)
            .RegisterScalar(new DoubleFunction(), identity: name)
            .RegisterAggregate(new SumFunction("vgi_sum"), identity: name)
            .RegisterTable(sequence, identity: name)
            .RegisterCatalogTable(new CatalogTable
            {
                Name = "ten",
                SchemaName = "main",
                Comment = "Integers 0..9",
                ScanFunction = sequence,
                ScanArguments = [10L],
            }, identity: name)
            .RegisterCatalogTable(new CatalogTable
            {
                Name = "five",
                SchemaName = "extra",
                Comment = "Integers 0..4",
                ScanFunction = sequence,
                ScanArguments = [5L],
            }, identity: name)
            .RegisterView(new CatalogView { Name = "answer", Definition = "SELECT 42 AS answer", Comment = "One row" }, identity: name)
            .RegisterMacro(new CatalogMacro
            {
                Name = "contents_triple",
                MacroType = MacroType.Scalar,
                Parameters = ["x"],
                Definition = "x * 3",
                Comment = "Triple a value",
            }, identity: name)
            .RegisterMacro(new CatalogMacro
            {
                Name = "contents_range",
                MacroType = MacroType.Table,
                Parameters = ["n"],
                Definition = "SELECT * FROM range(n)",
                Comment = "Table macro over range(n)",
            }, identity: name);
    }
}
