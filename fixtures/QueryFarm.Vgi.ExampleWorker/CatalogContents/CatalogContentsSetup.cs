using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.ExampleWorker.Aggregate;
using QueryFarm.Vgi.ExampleWorker.Scalar;
using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.ExampleWorker.CatalogContents;

/// <summary>
/// The <c>catalog_contents</c> catalogs (<c>catalog/catalog_contents.test</c>,
/// <c>catalog/catalog_contents_fallback.test</c>), mirroring vgi-python's
/// <c>vgi/_test_fixtures/catalog_contents.py</c>: one two-schema catalog holding every kind the
/// client seeds — <c>main</c> (table <c>ten</c>, view <c>answer</c>, scalar <c>double</c>,
/// aggregate <c>vgi_sum</c>, table function <c>sequence</c>, scalar macro <c>contents_triple</c>,
/// table macro <c>contents_range</c>) and <c>extra</c> (table <c>five</c>) — served under three
/// names that differ only in how they answer <c>catalog_contents</c>:
/// <list type="bullet">
/// <item><c>contents_probe</c> advertises and serves it (the SDK default).</item>
/// <item><c>contents_broken</c> advertises it but refuses the call, driving the client's per-schema
/// fallback.</item>
/// <item><c>contents_legacy</c> does not advertise it, like an older worker.</item>
/// </list>
/// Python's fourth catalog, <c>contents_memory</c> (DDL invalidation), needs a DDL-capable catalog,
/// which this SDK does not have.
/// </summary>
internal static class CatalogContentsSetup
{
    public const string Probe = "contents_probe";
    public const string Broken = "contents_broken";
    public const string Legacy = "contents_legacy";

    public const string BrokenMessage = "contents_broken: catalog_contents deliberately fails";

    public static void Register(Worker worker)
    {
        foreach (var identity in new[] { Probe, Broken, Legacy })
        {
            // Exclusive: these catalogs hold exactly what is registered below, not the default
            // ("example") identity's roster as well.
            worker.MarkIdentityExclusive(identity);

            // One instance per catalog, reused as both tables' scan function so it is registered
            // once (CatalogRegistry dedups a ScanFunction by reference) — in `main` only.
            var sequence = new SequenceFunction();
            worker
                .RegisterSchema("main", "Every object kind", identity: identity)
                .RegisterSchema("extra", "A second schema, tables only", identity: identity)
                .RegisterScalar(new DoubleFunction(), identity)
                .RegisterAggregate(new SumFunction("vgi_sum"), identity)
                .RegisterTable(sequence, identity)
                .RegisterCatalogTable(new CatalogTable
                {
                    Name = "ten",
                    SchemaName = "main",
                    Comment = "Integers 0..9",
                    ScanFunction = sequence,
                    ScanArguments = [10L],
                }, identity)
                .RegisterCatalogTable(new CatalogTable
                {
                    Name = "five",
                    SchemaName = "extra",
                    Comment = "Integers 0..4",
                    ScanFunction = sequence,
                    ScanArguments = [5L],
                }, identity)
                .RegisterView(new CatalogView
                {
                    Name = "answer",
                    SchemaName = "main",
                    Definition = "SELECT 42 AS answer",
                    Comment = "One row",
                }, identity)
                .RegisterMacro(new CatalogMacro
                {
                    Name = "contents_triple",
                    SchemaName = "main",
                    MacroType = MacroType.Scalar,
                    Parameters = ["x"],
                    Definition = "x * 3",
                    Comment = "Triple a value",
                }, identity)
                .RegisterMacro(new CatalogMacro
                {
                    Name = "contents_range",
                    SchemaName = "main",
                    MacroType = MacroType.Table,
                    Parameters = ["n"],
                    Definition = "SELECT * FROM range(n)",
                    Comment = "Table macro over range(n)",
                }, identity);
        }
    }

    /// <summary><c>contents_legacy</c> withdraws the advertisement; every other attach is left to
    /// the next handler (<see langword="null"/>).</summary>
    public static AttachContext? HandleAttach(CatalogAttachRequest request) =>
        request.Name == Legacy ? new AttachContext { SupportsCatalogContents = false } : null;

    public static void HandleCatalogContents(string identity)
    {
        if (identity == Broken)
        {
            throw new InvalidOperationException(BrokenMessage);
        }
    }
}
