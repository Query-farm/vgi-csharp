using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>filtered_columns_echo(count)</c> — introspects which columns the pushed-down filter tree
/// references (<c>filtered_cols</c>, <c>has_n</c>, <c>has_tag</c>) and, for the string column
/// <c>tag</c>, whether its filter resolves to a discrete value set (<see cref="ValuePruneFunction.ResolveColumnValues"/>).
/// Also genuinely applies the pushed filter (rows must stay correct). Backs
/// <c>filtered_columns_pushdown.test</c>.
/// </summary>
public sealed class FilteredColumnsEchoFunction : ITableFunction
{
    public string Name => "filtered_columns_echo";

    public string Description => "Reports which columns a pushed-down filter tree references";

    public bool? FilterPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("tag", StringType.Default, nullable: true),
            new Field("filtered_cols", StringType.Default, nullable: true),
            new Field("has_n", BooleanType.Default, nullable: true),
            new Field("has_tag", BooleanType.Default, nullable: true),
            new Field("tag_values", StringType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var filteredColumns = CollectColumns(decoded);
        var filteredColsText = filteredColumns.Count == 0 ? "(empty)" : string.Join(",", filteredColumns.OrderBy(c => c, StringComparer.Ordinal));
        var hasN = filteredColumns.Contains("n");
        var hasTag = filteredColumns.Contains("tag");
        var tagValues = ValuePruneFunction.ResolveColumnValues(decoded, "tag");
        var tagValuesText = tagValues is null ? "(none)" : string.Join(",", tagValues.Select(v => (string?)v));

        return new Producer(count, decoded, filteredColsText, hasN, hasTag, tagValuesText, initParams.OutputSchema);
    }

    private static HashSet<string> CollectColumns(DecodedFilters? filters)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        if (filters is null || filters.Root.ValueKind != JsonValueKind.Array)
        {
            return result;
        }

        foreach (var node in filters.Root.EnumerateArray())
        {
            Collect(node, result);
        }

        return result;
    }

    private static void Collect(JsonElement node, HashSet<string> result)
    {
        if (node.TryGetProperty("column_name", out var name) && name.GetString() is { } n)
        {
            result.Add(n);
        }

        if (node.TryGetProperty("children", out var children) && children.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in children.EnumerateArray())
            {
                Collect(child, result);
            }
        }
    }

    private sealed class Producer(
        long count, DecodedFilters? decoded, string filteredCols, bool hasN, bool hasTag, string tagValues, Schema outputSchema)
        : ITableFunctionProducer
    {
        private const int BatchSize = 2048;
        private long _next;

        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();
            var tags = new List<string>();
            var row = new Dictionary<string, object?>();
            while (ns.Count == 0 && _next < count)
            {
                var candidateRows = (int)Math.Min(BatchSize, count - _next);
                var start = _next;
                _next += candidateRows;
                for (var i = 0; i < candidateRows; i++)
                {
                    var n = start + i;
                    var tag = $"t{n}";
                    row["n"] = n;
                    row["tag"] = tag;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        ns.Add(n);
                        tags.Add(tag);
                    }
                }
            }

            if (ns.Count == 0)
            {
                output.Finish();
                return;
            }

            var rows = ns.Count;
            var nBuilder = new Int64Array.Builder();
            var tagBuilder = new StringArray.Builder();
            var colsBuilder = new StringArray.Builder();
            var hasNBuilder = new BooleanArray.Builder();
            var hasTagBuilder = new BooleanArray.Builder();
            var tagValuesBuilder = new StringArray.Builder();

            for (var i = 0; i < rows; i++)
            {
                nBuilder.Append(ns[i]);
                tagBuilder.Append(tags[i]);
                colsBuilder.Append(filteredCols);
                hasNBuilder.Append(hasN);
                hasTagBuilder.Append(hasTag);
                tagValuesBuilder.Append(tagValues);
            }

            output.Emit(new RecordBatch(
                outputSchema,
                [nBuilder.Build(), tagBuilder.Build(), colsBuilder.Build(), hasNBuilder.Build(), hasTagBuilder.Build(), tagValuesBuilder.Build()],
                rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
