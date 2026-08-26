using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>dict_filter_echo(count)</c> — emits <c>count</c> rows with an <c>s</c> column declared as
/// plain Arrow <c>dictionary&lt;int8, utf8&gt;</c> (NOT wrapped in DuckDB's ENUM extension
/// metadata), so DuckDB types it as VARCHAR while the worker still emits it dictionary-encoded on
/// the wire — pins <c>filter_pushdown/dictionary_varchar.test</c>'s scenario: a VARCHAR-typed
/// pushdown literal compared against a column that is, underneath, still a dictionary array.
/// Row <c>i</c> carries <c>('red', 'green', 'blue')[i % 3]</c>.
/// </summary>
public sealed class DictFilterEchoFunction : ITableFunction
{
    private static readonly string[] Colors = ["red", "green", "blue"];

    public string Name => "dict_filter_echo";

    public string Description => "Emits rows with a dictionary-encoded (non-ENUM) VARCHAR column, filterable";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("s", new DictionaryType(Int8Type.Default, StringType.Default, ordered: false), nullable: true),
        ],
        metadata: null);

    public bool? FilterPushdown => true;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        return new Producer(count, decoded, initParams.OutputSchema);
    }

    private sealed class Producer(long count, DecodedFilters? decoded, Schema outputSchema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();
            var ss = new List<string>();
            var row = new Dictionary<string, object?>();

            while (ns.Count == 0 && _next < count)
            {
                var n = _next;
                _next++;
                var s = Colors[n % 3];
                row["n"] = n;
                row["s"] = s;
                if (PushdownFilterEvaluator.Matches(decoded, row))
                {
                    ns.Add(n);
                    ss.Add(s);
                }
            }

            if (ns.Count == 0)
            {
                output.Finish();
                return;
            }

            var nBuilder = new Int64Array.Builder();
            foreach (var n in ns)
            {
                nBuilder.Append(n);
            }

            var dictType = (DictionaryType)outputSchema.GetFieldByIndex(1).DataType;
            var valuesBuilder = new StringArray.Builder();
            foreach (var color in Colors)
            {
                valuesBuilder.Append(color);
            }

            var values = valuesBuilder.Build();
            var indexBuilder = new Int8Array.Builder();
            foreach (var s in ss)
            {
                indexBuilder.Append((sbyte)System.Array.IndexOf(Colors, s));
            }

            var sArray = new DictionaryArray(dictType, indexBuilder.Build(), values);

            output.Emit(new RecordBatch(outputSchema, [nBuilder.Build(), sArray], ns.Count));

            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
