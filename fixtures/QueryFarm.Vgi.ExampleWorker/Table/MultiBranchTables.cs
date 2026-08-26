using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// The <c>multi_branch_*</c> catalog tables backing <c>test/sql/integration/catalog/multi_branch_*.test</c>
/// and <c>test/sql/integration/splits/multi_branch.test</c> — each declares
/// <see cref="CatalogTable.Branches"/> instead of a single <see cref="CatalogTable.ScanFunction"/>,
/// so the C++ optimizer (<c>VgiMultiScanRewriter</c>) stitches the branches together into a
/// <c>UNION ALL</c> (or refuses per the various loud-fail contracts — empty branches, two writable
/// arms, AT-clauses, etc.). Everything about HOW branches combine (union semantics, branch_filter
/// pruning, column reconciliation by name, join/lateral/pushdown interactions) is exercised
/// entirely on the C++ side; this worker only declares the branch shapes.
/// </summary>
public static class MultiBranchTables
{
    private const string SchemaName = "data";

    private static readonly Schema NSchema = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public static CatalogTable Numbers { get; } = BuildNumbers();

    public static CatalogTable FilteredNumbers { get; } = BuildFilteredNumbers();

    public static CatalogTable Empty { get; } = BuildEmpty();

    public static CatalogTable TwoWritable { get; } = BuildTwoWritable();

    public static CatalogTable Split { get; } = BuildSplit();

    public static CatalogTable Format { get; } = BuildFormat();

    public static CatalogTable Hetero { get; } = BuildHetero();

    public static CatalogTable Iceberg { get; } = BuildIceberg();

    public static CatalogTable NoPushdown { get; } = BuildNoPushdown();

    public static CatalogTable Recon { get; } = BuildRecon();

    public static IReadOnlyList<CatalogTable> All { get; } =
        [Numbers, FilteredNumbers, Empty, TwoWritable, Split, Format, Hetero, Iceberg, NoPushdown, Recon];

    /// <summary>Shared scratch dir for the native-branch fixtures (their <c>read_parquet</c>/
    /// <c>read_csv_auto</c>/<c>iceberg_scan</c> arms). Must name the SAME concrete path the coupled
    /// <c>.test</c> files reference via <c>${VGI_TEST_BRANCH_DIR}</c> — defaults to the OS temp dir
    /// when unset (matching vgi-rust's/vgi-go's own worker fixtures) so these tables still resolve
    /// to SOME writable location even outside the test harness.</summary>
    private static string BranchDir()
    {
        var raw = Environment.GetEnvironmentVariable("VGI_TEST_BRANCH_DIR");
        if (string.IsNullOrEmpty(raw))
        {
            raw = Path.GetTempPath();
        }

        return raw.Replace('\\', '/').TrimEnd('/');
    }

    private static string BranchPath(string name) => $"{BranchDir()}/{name}";

    private static ScanBranchSpec Seq(long count) => new()
    {
        FunctionName = "sequence",
        PositionalArguments = [count],
    };

    private static ScanBranchSpec Native(string function, string path) => new()
    {
        FunctionName = function,
        PositionalArguments = [path],
    };

    private static CatalogTable BuildNumbers() => new()
    {
        Name = "multi_branch_numbers",
        SchemaName = SchemaName,
        Comment = "Multi-branch: UNION of sequence(50) + sequence(50) — used by multi_branch_scan.test",
        Columns = NSchema,
        Branches = [Seq(50), Seq(50)],
    };

    private static CatalogTable BuildFilteredNumbers() => new()
    {
        Name = "multi_branch_filtered_numbers",
        SchemaName = SchemaName,
        Comment = "Multi-branch with complementary branch_filters — exercises pruning",
        Columns = NSchema,
        Branches =
        [
            Seq(100) with { BranchFilter = "n < 50" },
            Seq(100) with { BranchFilter = "n >= 50" },
        ],
    };

    private static CatalogTable BuildEmpty() => new()
    {
        Name = "multi_branch_empty",
        SchemaName = SchemaName,
        Comment = "Multi-branch: empty branches list — used by multi_branch_empty_branches.test",
        Columns = NSchema,
        Branches = [],
    };

    private static CatalogTable BuildTwoWritable() => new()
    {
        Name = "multi_branch_two_writable",
        SchemaName = SchemaName,
        Comment = "Multi-branch with two writable=True arms — used by multi_branch_two_writable.test",
        Columns = NSchema,
        Branches = [Seq(10) with { Writable = true }, Seq(10) with { Writable = true }],
    };

    private static CatalogTable BuildSplit() => new()
    {
        Name = "multi_branch_split",
        SchemaName = SchemaName,
        Comment = "Multi-branch: split_sequence(30, splits=6) + sequence(20) — used by splits/multi_branch.test",
        Columns = NSchema,
        Branches =
        [
            new ScanBranchSpec
            {
                FunctionName = "split_sequence",
                NamedArguments = new Dictionary<string, object?> { ["n"] = 30L, ["splits"] = 6L },
            },
            Seq(20),
        ],
    };

    private static CatalogTable BuildFormat() => new()
    {
        Name = "multi_branch_format",
        SchemaName = SchemaName,
        Comment = "Format branch: read_csv with delim/header options — used by multi_branch_format.test",
        Columns = new Schema(
            [new Field("n", Int64Type.Default, nullable: true), new Field("label", StringType.Default, nullable: true)],
            metadata: null),
        Branches =
        [
            new ScanBranchSpec
            {
                FormatName = "csv",
                FormatLocations = [BranchPath("vgi_format_branch.csv")],
                FormatOptions = new Dictionary<string, object?>
                {
                    ["delim"] = "|",
                    ["header"] = true,
                    ["nullstr"] = "row_2",
                },
            },
        ],
    };

    private static CatalogTable BuildHetero() => new()
    {
        Name = "multi_branch_hetero",
        SchemaName = SchemaName,
        Comment = "Multi-branch: sequence(50) + read_parquet — used by multi_branch_heterogeneous.test",
        Columns = NSchema,
        Branches = [Seq(50), Native("read_parquet", BranchPath("vgi_hetero_branch.parquet"))],
    };

    private static CatalogTable BuildIceberg() => new()
    {
        Name = "multi_branch_iceberg",
        SchemaName = SchemaName,
        Comment = "Multi-branch: sequence(50) + iceberg_scan — used by multi_branch_iceberg.test",
        Columns = NSchema,
        Branches = [Seq(50), Native("iceberg_scan", BranchPath("vgi_iceberg_branch"))],
        RequiredExtensions = ["iceberg"],
    };

    private static CatalogTable BuildNoPushdown() => new()
    {
        Name = "multi_branch_nopushdown",
        SchemaName = SchemaName,
        Comment = "Multi-branch: VGI + read_csv — used by multi_branch_pushdown_incapable.test",
        Columns = NSchema,
        Branches = [Seq(50), Native("read_csv_auto", BranchPath("vgi_nopushdown_branch.csv"))],
    };

    private static CatalogTable BuildRecon() => new()
    {
        Name = "multi_branch_recon",
        SchemaName = SchemaName,
        Comment = "Multi-branch: column reconciliation — used by multi_branch_reconciliation.test",
        Columns = new Schema(
            [new Field("a", Int64Type.Default, nullable: true), new Field("b", Int64Type.Default, nullable: true)],
            metadata: null),
        Branches =
        [
            Native("read_parquet", BranchPath("vgi_recon_a_b.parquet")),
            Native("read_parquet", BranchPath("vgi_recon_b_a.parquet")),
            Native("read_parquet", BranchPath("vgi_recon_a_only.parquet")),
        ],
    };
}
