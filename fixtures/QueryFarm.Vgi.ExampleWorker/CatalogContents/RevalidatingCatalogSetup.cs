using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.ExampleWorker.Scalar;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.ExampleWorker.CatalogContents;

/// <summary>
/// <c>catalog_contents</c> revalidation (protocol 2.1.0) on this worker, so the conditional path is
/// exercised in the C# lane:
/// <list type="bullet">
/// <item><c>example</c> opts in to the framework's content-hash etag
/// (<see cref="CatalogContentsEtagMode.ContentHash"/>): the etag is the SHA-256 of the snapshot,
/// computed once (the snapshot is cached), and a matching <c>if_none_match</c> is answered
/// not-modified. <c>catalog/catalog_contents_conformance.test</c> attaches <c>example</c>, so its
/// conditional-call checks run against this.</item>
/// <item><c>contents_reval</c> is a revalidating catalog with a cheap validator, mirroring
/// vgi-python's <c>contents_reval</c> fixture: its etag is <c>gen-&lt;n&gt;</c> with <c>n</c> the
/// catalog version, and a matching <c>if_none_match</c> is answered not-modified without building
/// anything. This SDK has no DDL, so the version (and the etag) never moves.</item>
/// </list>
/// </summary>
internal static class RevalidatingCatalogSetup
{
    public const string RevalCatalogName = "contents_reval";

    public static Worker Register(Worker worker) => worker
        .CatalogContentsEtag(CatalogContentsEtagMode.ContentHash, identity: "example")
        .RegisterCatalog(new CatalogInfo { Name = RevalCatalogName }, exclusive: true)
        .RegisterSchema("main", "Revalidating catalog_contents fixture", identity: RevalCatalogName)
        .RegisterScalar(new AddValuesFunction(), identity: RevalCatalogName)
        .RegisterView(new CatalogView { Name = "answer", Definition = "SELECT 42 AS answer" }, identity: RevalCatalogName)
        .OnCatalogContents(RevalCatalogName, async request =>
        {
            var etag = $"gen-{request.CatalogVersion}";
            return request.IfNoneMatch == etag
                ? CatalogContentsResult.Unchanged(etag)
                : await request.BuildAsync() with { Etag = etag };
        });
}
