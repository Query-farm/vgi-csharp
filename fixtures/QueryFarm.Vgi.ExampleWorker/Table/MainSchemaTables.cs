using QueryFarm.Vgi.Catalog;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>Real catalog tables in the default (<c>main</c>) schema — currently none.
///
/// <c>attach/ddl_wire_contract.test</c>'s <c>ALTER TABLE example.main.even_numbers ...</c>
/// assertions used to be backed by a real table registered here, but every one of that test's
/// statements is a <c>statement error ... catalog is read-only</c> — DuckDB's VGI catalog refuses
/// EVERY DDL RPC generically (read-only worker) regardless of the target object's actual kind, so
/// a CATALOG VIEW satisfies it exactly as well as a table would. Since <c>view/views.test</c>
/// separately requires a real catalog VIEW named <c>main.even_numbers</c> (see Program.cs's
/// <c>RegisterView</c> calls) and a table and a view can't share one qualified name in this
/// worker's catalog (the table would shadow the view for ordinary <c>SELECT</c> resolution), this
/// class is intentionally empty — the view registration alone satisfies both tests.</summary>
public static class MainSchemaTables
{
    public static IReadOnlyList<CatalogTable> All { get; } = [];
}
