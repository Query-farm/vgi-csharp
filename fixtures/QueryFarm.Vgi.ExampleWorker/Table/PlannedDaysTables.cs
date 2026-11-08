using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// Protocol 3.2 filtered scan planning fixtures (<c>test/sql/integration/catalog/scan_planning_*.test</c>),
/// mirroring vgi-python's <c>planned_days*</c> tables. Five days of data, one parquet file per day:
/// <c>${VGI_TEST_BRANCH_DIR}/planned_days_&lt;day&gt;.parquet</c> (flat — DuckDB <c>COPY</c> cannot
/// create directories). The tests create the files for the first four
/// days only: the 2026-10-05 file never exists, so any scan that fails to prune that day errors with
/// "No files found" — a passing query proves the pruning happened.
/// </summary>
public static class PlannedDaysTables
{
    private const string SchemaName = "data";

    private static readonly DateOnly[] Days = [.. Enumerable.Range(1, 5).Select(d => new DateOnly(2026, 10, d))];

    private static readonly Schema Columns = new(
        [new Field("event_date", Date32Type.Default, nullable: true), new Field("n", Int64Type.Default, nullable: true)],
        metadata: null);

    public static IReadOnlyList<CatalogTable> All { get; } =
    [
        // Worker-side pruning: the planning hook drops day files with ScanPlanningFilters.MayMatch.
        new()
        {
            Name = "planned_days",
            SchemaName = SchemaName,
            Columns = Columns,
            ScanPlanning = ScanPlanningModes.Filtered,
            Comment = "Protocol 3.2: worker-side pruning of per-day files — scan_planning_filtered.test",
            RequiredExtensions = ["parquet"],
            PlanScanBranches = PlanFiltered,
        },
        // Client-side pruning: one branch per day carrying event_date min/max; the worker ignores
        // the predicates and the client prunes by the statistics.
        new()
        {
            Name = "planned_days_stats",
            SchemaName = SchemaName,
            Columns = Columns,
            ScanPlanning = ScanPlanningModes.Filtered,
            Comment = "Protocol 3.2: client-side pruning by ScanBranch statistics — scan_planning_stats.test",
            RequiredExtensions = ["parquet"],
            PlanScanBranches = _ => new ScanBranchesPlan
            {
                Branches = [.. Days.Select(day => DayBranch(day) with { RowCount = 10, ColumnStatistics = DayStats(day) })],
            },
        },
        // branch_filter contradiction on a bind-time table: one branch per day with
        // branch_filter event_date = DATE '<day>'.
        new()
        {
            Name = "planned_days_bf",
            SchemaName = SchemaName,
            Columns = Columns,
            Comment = "Protocol 3.2: pruning by branch_filter contradiction — scan_planning_branch_filter.test",
            RequiredExtensions = ["parquet"],
            Branches = [.. Days.Select(day => DayBranch(day) with { BranchFilter = $"event_date = DATE '{Iso(day)}'" })],
        },
    ];

    private static ScanBranchesPlan PlanFiltered(ScanPlanningRequest request)
    {
        var filters = request.Filters;
        var days = Days.Where(day => filters is null || filters.MayMatch(DayStats(day))).ToList();
        // Record what the worker saw so tests can assert the predicates arrived. Same bytes as
        // vgi-python's json.dump: {"received_filters": true, "days": ["2026-10-03", ...]}.
        File.WriteAllText(
            $"{BranchDir()}/planned_days_last_plan.json",
            $"{{\"received_filters\": {(filters is not null ? "true" : "false")}, \"days\": [" +
            string.Join(", ", days.Select(day => JsonSerializer.Serialize(Iso(day)))) + "]}");
        if (days.Count == 0)
        {
            return new ScanBranchesPlan();
        }

        return new ScanBranchesPlan
        {
            Branches =
            [
                new ScanBranchSpec
                {
                    FunctionName = "read_parquet",
                    PositionalArguments = [days.Select(DayFile).ToList()],
                    RowCount = 10L * days.Count,
                },
            ],
        };
    }

    private static ScanBranchSpec DayBranch(DateOnly day) => new()
    {
        FunctionName = "read_parquet",
        PositionalArguments = [DayFile(day)],
    };

    private static Dictionary<string, BranchColumnStatistics> DayStats(DateOnly day) => new()
    {
        ["event_date"] = new BranchColumnStatistics { Min = day, Max = day, HasNull = false },
    };

    private static string Iso(DateOnly day) => day.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);

    private static string DayFile(DateOnly day) => $"{BranchDir()}/planned_days_{Iso(day)}.parquet";

    /// <summary>Same scratch-dir convention as <see cref="MultiBranchTables"/>.</summary>
    private static string BranchDir()
    {
        var raw = Environment.GetEnvironmentVariable("VGI_TEST_BRANCH_DIR");
        if (string.IsNullOrEmpty(raw))
        {
            raw = Path.GetTempPath();
        }

        return raw.Replace('\\', '/').TrimEnd('/');
    }
}
