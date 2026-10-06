namespace QueryFarm.Vgi.Catalog;

/// <summary>Options for a DDL-capable in-memory catalog
/// (<see cref="Worker.RegisterInMemoryCatalog"/>) — the C# counterpart of vgi-python's
/// <c>InMemoryCatalog</c>.</summary>
public sealed class InMemoryCatalogOptions
{
    /// <summary>Whether <c>catalog_version</c> reports the catalog's generation counter (starts at
    /// 1, moved by every DDL). <see langword="false"/> always reports 0 — "unknown", for a catalog
    /// that does not track its version; a client then clears its cache at every transaction start
    /// (see the version-0 rule in the extension's <c>docs/catalog_contents.md</c>).</summary>
    public bool ReportsVersion { get; init; } = true;
}
