namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// Result of the <c>catalog_table_scan_branches_get</c> RPC — describes a table's scan as a list
/// of independent <see cref="ScanBranch"/>es the C++ optimizer UNIONs together (or, for the common
/// single-branch case, collapses back into the legacy single-function scan path — see
/// <c>VgiTableEntry::GetScanFunctionImpl</c>). Called for EVERY VGI table, not just multi-branch
/// ones — the <c>vgi_table_branches()</c> diagnostic function and the capability-detection cache
/// (<c>catalog/multi_branch_capability_cache.test</c>) both depend on a compliant worker answering
/// it for a plain, wholly ordinary function-backed table too (as a single synthesized branch).
/// Property order matches the generated <c>ScanBranchesResultSchema()</c>: branches,
/// required_extensions.
/// </summary>
public sealed class ScanBranchesResult
{
    /// <summary>Each element an <see cref="Internal.EmbeddedIpc"/>-encoded <see cref="ScanBranch"/>.
    /// MUST be non-empty — the C++ parser (<c>ParseScanBranchesResult</c>) throws a loud
    /// <c>BinderException</c> at bind time on an empty list (see
    /// <c>catalog/multi_branch_empty_branches.test</c>), rather than silently returning zero rows.</summary>
    public List<byte[]> Branches { get; set; } = [];

    /// <summary>DuckDB extensions required to scan any of <see cref="Branches"/> (e.g.
    /// <c>["iceberg"]</c> for an <c>iceberg_scan</c> branch) — auto-loaded by the C++ side before
    /// the scan runs. Union across all branches; empty means none.</summary>
    public List<string> RequiredExtensions { get; set; } = [];
}
