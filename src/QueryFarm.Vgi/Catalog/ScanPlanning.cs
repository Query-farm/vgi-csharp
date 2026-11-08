using Apache.Arrow;
using QueryFarm.Vgi.Arguments;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Catalog;

/// <summary>Protocol 3.2 <c>TableInfo.scan_planning</c> values (<see cref="CatalogTable.ScanPlanning"/>).
/// See vgi-python's <c>docs/protocol/scan-planning.md</c>.</summary>
public static class ScanPlanningModes
{
    /// <summary>Resolve the scan redirect at bind time — protocol 3.1 behaviour, the default.</summary>
    public const string Bind = "bind";

    /// <summary>Resolve the scan redirect once per scan, after the client has collected the query's
    /// predicates on the table, which arrive as <see cref="ScanPlanningRequest.Filters"/>.</summary>
    public const string Filtered = "filtered";

    /// <summary>Whether <paramref name="mode"/> is a value the protocol defines.</summary>
    public static bool IsKnown(string? mode) => mode is Bind or Filtered;
}

/// <summary>One <c>catalog_table_scan_branches_get</c> call, as seen by
/// <see cref="CatalogTable.PlanScanBranches"/>.</summary>
public sealed class ScanPlanningRequest
{
    /// <summary>The catalog identity the attach resolved to (see <c>Worker.RegisterCatalog</c>).</summary>
    public required string Identity { get; init; }

    /// <summary>The table being scanned.</summary>
    public required CatalogTable Table { get; init; }

    /// <summary>The schema path the client asked for.</summary>
    public required IReadOnlyList<string> SchemaPath { get; init; }

    /// <summary>The table name the client asked for.</summary>
    public required string TableName { get; init; }

    /// <summary>Time-travel unit (<c>VERSION</c>/<c>TIMESTAMP</c>), or null/empty for none.</summary>
    public string? AtUnit { get; init; }

    /// <summary>Time-travel value matching <see cref="AtUnit"/>.</summary>
    public string? AtValue { get; init; }

    /// <summary>The raw transaction token, or <see langword="null"/> outside a transaction.</summary>
    public byte[]? TransactionOpaqueData { get; init; }

    /// <summary>The query's predicates on this table, or <see langword="null"/> when none were
    /// collected (bind-time planning, a <see cref="ScanPlanningModes.Bind"/> table, or a query with no
    /// usable predicate). Every predicate is advisory.</summary>
    public ScanPlanningFilters? Filters { get; init; }
}

/// <summary>What <see cref="CatalogTable.PlanScanBranches"/> returns: the branches to scan. Zero
/// branches is legal for a <see cref="ScanPlanningModes.Filtered"/> table and means the scan produces
/// no rows (a <see cref="ScanPlanningModes.Bind"/> table's client still rejects zero branches).</summary>
public sealed record ScanBranchesPlan
{
    /// <summary>The branches, each built exactly like a <see cref="CatalogTable.Branches"/> entry.</summary>
    public IReadOnlyList<ScanBranchSpec> Branches { get; init; } = [];

    /// <summary>DuckDB extensions the branches need; <see langword="null"/> (the default) reports the
    /// table's own <see cref="CatalogTable.RequiredExtensions"/>.</summary>
    public IReadOnlyList<string>? RequiredExtensions { get; init; }
}

/// <summary>
/// The query predicates sent with a filtered <c>catalog_table_scan_branches_get</c> (protocol 3.2):
/// an Arrow IPC stream holding one VGI Filter Encoding v2 snapshot batch whose column indexes refer
/// to the table's declared columns. Decoded lazily, on first use. Every predicate is advisory — the
/// client re-applies all of them — so a worker may ignore any predicate, but must never drop a
/// branch or file that could hold a matching row. See vgi-python's
/// <c>docs/protocol/scan-planning.md</c>.
/// </summary>
public sealed class ScanPlanningFilters
{
    private readonly Func<Schema> _tableColumns;
    private readonly Lazy<FilterSet?> _decoded;

    /// <summary>Wraps raw <paramref name="raw"/> filter bytes; <paramref name="tableColumns"/> resolves
    /// the table's declared columns (the schema the column indexes refer to) and is called at most
    /// once, on first decode.</summary>
    public ScanPlanningFilters(byte[] raw, Func<Schema> tableColumns)
    {
        ArgumentNullException.ThrowIfNull(raw);
        ArgumentNullException.ThrowIfNull(tableColumns);
        Raw = raw;
        _tableColumns = tableColumns;
        _decoded = new Lazy<FilterSet?>(() => FilterSet.Decode(Raw, null, _tableColumns()));
    }

    /// <summary>The wire bytes exactly as received.</summary>
    public byte[] Raw { get; }

    /// <summary>The decoded predicates (cached), or <see langword="null"/> for an empty payload.
    /// Throws <see cref="InvalidDataException"/> for a malformed payload.</summary>
    public FilterSet? Decode() => _decoded.Value;

    /// <summary>
    /// Returns <see langword="false"/> only when the predicates provably exclude every row described
    /// by <paramref name="statistics"/> — a candidate (a file, a partition, a branch) described the
    /// same way as <see cref="ScanBranchSpec.ColumnStatistics"/>: every non-null value of a column
    /// lies within <c>[Min, Max]</c> (a null bound is unknown), <c>HasNull = false</c> means no
    /// NULLs, <c>HasNotNull = false</c> means entirely NULL. Use it to prune a manifest before
    /// returning branches. Predicates, columns and value types it cannot reason about count as "may
    /// match", so the answer errs towards keeping data.
    /// </summary>
    public bool MayMatch(IReadOnlyDictionary<string, ColumnStatisticsInput> statistics)
    {
        ArgumentNullException.ThrowIfNull(statistics);
        return Decode() is not { } filters || ScanPlanningEvaluator.MayMatch(filters.Decoded, statistics);
    }
}
