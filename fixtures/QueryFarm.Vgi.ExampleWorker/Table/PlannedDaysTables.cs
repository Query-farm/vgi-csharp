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
/// <para><c>planned_events</c> (<c>scan_planning_predicates*.test</c>) is a filtered table over
/// <c>planned_events_&lt;day&gt;.parquet</c> with the column types real catalogs filter on (DATE,
/// TIMESTAMPTZ, VARCHAR, BIGINT, DOUBLE, BOOLEAN). It prunes days on <c>event_date</c> and <c>ts</c>
/// statistics and records every planning call — the planned days and the described filters, null when
/// none arrived — in <c>planned_events_last_plan.json</c>, as vgi-python's fixture does.</para>
/// </summary>
public static class PlannedDaysTables
{
    private const string SchemaName = "data";

    private static readonly DateOnly[] Days = [.. Enumerable.Range(1, 5).Select(d => new DateOnly(2026, 10, d))];

    private static readonly Schema Columns = new(
        [new Field("event_date", Date32Type.Default, nullable: true), new Field("n", Int64Type.Default, nullable: true)],
        metadata: null);

    /// <summary>planned_events: one file per day (written by scan_planning_predicates.test), 10 rows
    /// each. Statistics cover event_date and ts, so predicates on either prune; every other column only
    /// reaches DuckDB.</summary>
    private static readonly Schema EventColumns = new(
        [
            new Field("event_date", Date32Type.Default, nullable: true),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true),
            new Field("series", StringType.Default, nullable: true),
            new Field("ticker", StringType.Default, nullable: true),
            new Field("n", Int64Type.Default, nullable: true),
            new Field("price", DoubleType.Default, nullable: true),
            new Field("flag", BooleanType.Default, nullable: true),
        ],
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
        // Filtered planning over a mixed-type table: the predicates a real catalog sees (string LIKE /
        // functions / NULLs / timestamps / bools), most of which the worker cannot use and DuckDB must
        // still apply.
        new()
        {
            Name = "planned_events",
            SchemaName = SchemaName,
            Columns = EventColumns,
            ScanPlanning = ScanPlanningModes.Filtered,
            Comment = "Protocol 3.2: filtered planning, non-prunable predicates — scan_planning_predicates.test",
            RequiredExtensions = ["parquet"],
            PlanScanBranches = PlanEvents,
        },
    ];

    /// <summary>
    /// Branches for data.planned_events: the day files the predicates may match. Every planning call
    /// writes planned_events_last_plan.json with the planned days and the described filters (null when
    /// the client sent none), so a test can assert what reached the worker, including that a predicate
    /// it cannot use (LIKE, most functions) stayed with the client. Only scan_planning_predicates*.test
    /// query this table, in one serial group, so the record is never overwritten concurrently. Same
    /// bytes as vgi-python's json.dump.
    /// </summary>
    private static ScanBranchesPlan PlanEvents(ScanPlanningRequest request)
    {
        var filters = request.Filters;
        var days = Days.Where(day => filters is null || filters.MayMatch(EventStats(day))).ToList();
        var described = filters is null
            ? "null"
            : "[" + string.Join(", ", DescribeFilters(filters).Select(line => JsonSerializer.Serialize(line))) + "]";
        WriteAtomically(
            $"{BranchDir()}/planned_events_last_plan.json",
            "{\"days\": [" + string.Join(", ", days.Select(day => JsonSerializer.Serialize(Iso(day)))) +
            "], \"filters\": " + described + "}");

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
                    PositionalArguments = [days.Select(EventFile).ToList()],
                    RowCount = 10L * days.Count,
                },
            ],
        };
    }

    /// <summary>event_date is the day; ts spans 00:00–09:00 UTC.</summary>
    private static Dictionary<string, BranchColumnStatistics> EventStats(DateOnly day)
    {
        var start = new DateTimeOffset(day.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
        return new Dictionary<string, BranchColumnStatistics>
        {
            ["event_date"] = new BranchColumnStatistics { Min = day, Max = day, HasNull = false },
            ["ts"] = new BranchColumnStatistics { Min = start, Max = start.AddHours(9), HasNull = false },
        };
    }

    private static string EventFile(DateOnly day) => $"{BranchDir()}/planned_events_{Iso(day)}.parquet";

    private static void WriteAtomically(string path, string text)
    {
        var temporary = $"{path}.{Environment.ProcessId}.tmp";
        File.WriteAllText(temporary, text);
        File.Move(temporary, path, overwrite: true);
    }

    /// <summary>
    /// One stable line per pushed predicate, sorted ordinally, exactly as vgi-python's fixture
    /// <c>_describe_filter</c> writes them over its decoded filter views: <c>col OP</c> for a
    /// column-vs-literal comparison (and for <c>WHERE flag</c> / <c>WHERE NOT flag</c>, which Python
    /// projects onto <c>flag = true/false</c>), <c>col IsNull</c> / <c>col IsNotNull</c>,
    /// <c>col In</c> for a non-negated IN, <c>And(...)</c> / <c>Or(...)</c> when every child has a
    /// view, and <c>col V2Expression</c> (its first column) for anything else. Predicates with no
    /// column and unsupported runtime filters have no view and are left out.
    /// </summary>
    private static List<string> DescribeFilters(ScanPlanningFilters filters)
    {
        var lines = new List<string>();
        foreach (var predicate in filters.Decode()?.Predicates ?? [])
        {
            if (Kind(predicate.Expression) == "runtime_filter")
            {
                continue;
            }

            if (DescribeView(predicate.Expression) is { } line)
            {
                lines.Add(line);
            }
        }

        lines.Sort(StringComparer.Ordinal);
        return lines;
    }

    private static string? DescribeView(JsonElement node)
    {
        switch (Kind(node))
        {
            case "comparison":
                var left = node.GetProperty("left");
                if (Kind(left) == "column_ref" && Kind(node.GetProperty("right")) == "literal" &&
                    node.GetProperty("op").GetString() is "eq" or "ne" or "lt" or "le" or "gt" or "ge")
                {
                    return $"{ColumnName(left)} {node.GetProperty("op").GetString()!.ToUpperInvariant()}";
                }

                break;
            case "column_ref" when IsBooleanColumn(node):
                return $"{ColumnName(node)} EQ";
            case "not" when IsBooleanColumn(node.GetProperty("expression")):
                return $"{ColumnName(node.GetProperty("expression"))} EQ";
            case "is_null" when Kind(node.GetProperty("expression")) == "column_ref":
                return $"{ColumnName(node.GetProperty("expression"))} " +
                    (node.GetProperty("negated").GetBoolean() ? "IsNotNull" : "IsNull");
            case "in" when Kind(node.GetProperty("expression")) == "column_ref" && !node.GetProperty("negated").GetBoolean():
                return $"{ColumnName(node.GetProperty("expression"))} In";
            case "and" or "or":
                var children = node.GetProperty("children").EnumerateArray().Select(DescribeView).ToList();
                if (children.All(child => child is not null))
                {
                    return $"{(Kind(node) == "and" ? "And" : "Or")}({string.Join(", ", children)})";
                }

                break;
        }

        return FirstColumn(node) is { } column ? $"{column} V2Expression" : null;
    }

    /// <summary>The first column reference in the expression, in vgi-python's visiting order.</summary>
    private static string? FirstColumn(JsonElement node)
    {
        if (Kind(node) == "column_ref")
        {
            return ColumnName(node);
        }

        IEnumerable<JsonElement> operands = Kind(node) switch
        {
            "field_ref" or "not" or "is_null" or "cast" or "negate" or "in" => [node.GetProperty("expression")],
            "comparison" or "arithmetic" => [node.GetProperty("left"), node.GetProperty("right")],
            "and" or "or" => node.GetProperty("children").EnumerateArray(),
            "call" => node.GetProperty("arguments").EnumerateArray(),
            "runtime_filter" => [node.GetProperty("input")],
            _ => [],
        };
        foreach (var operand in operands)
        {
            if (FirstColumn(operand) is { } column)
            {
                return column;
            }
        }

        return null;
    }

    private static bool IsBooleanColumn(JsonElement node) =>
        Kind(node) == "column_ref" &&
        node.GetProperty("column_index").GetInt32() is var index &&
        index < EventColumns.FieldsList.Count &&
        EventColumns.FieldsList[index].DataType is BooleanType;

    private static string? Kind(JsonElement node) =>
        node.ValueKind == JsonValueKind.Object && node.TryGetProperty("node", out var kind) ? kind.GetString() : null;

    private static string ColumnName(JsonElement node) => node.GetProperty("column_name").GetString()!;

    private static ScanBranchesPlan PlanFiltered(ScanPlanningRequest request)
    {
        var filters = request.Filters;
        var days = Days.Where(day => filters is null || filters.MayMatch(DayStats(day))).ToList();
        // Record the days a FILTERED planning call kept, so scan_planning_filtered.test can assert
        // the predicates arrived. Unfiltered calls (e.g. vgi_table_branches() from tests running in
        // parallel) must not overwrite it. Same bytes as vgi-python's json.dump.
        if (filters is not null)
        {
            File.WriteAllText(
                $"{BranchDir()}/planned_days_last_plan.json",
                "{\"received_filters\": true, \"days\": [" +
                string.Join(", ", days.Select(day => JsonSerializer.Serialize(Iso(day)))) + "]}");
        }

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
