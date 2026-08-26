using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>union_varargs(*configs)</c> — each vararg is a <c>UNION(i BIGINT, s VARCHAR)</c>; echoes the
/// active member's tag and stringified value back per vararg, one row each (<c>idx</c> = call-site
/// position). Proves a sparse-union-typed vararg round-trips end-to-end — DuckDB only ever emits
/// SPARSE unions over Arrow (never dense), so that's the only mode this needs to decode. Backs
/// <c>table/union_varargs.test</c>.
/// </summary>
public sealed class UnionVarargsFunction : ITableFunction
{
    private static readonly UnionType ConfigUnionType = new(
        [
            new Field("i", Int64Type.Default, nullable: true),
            new Field("s", StringType.Default, nullable: true),
        ],
        [0, 1],
        UnionMode.Sparse);

    public string Name => "union_varargs";

    public string Description => "Echoes each UNION(i BIGINT, s VARCHAR) vararg's active tag/value";

    public Schema ArgumentsSchema { get; } = new(
        [
            new Field(
                "configs", ConfigUnionType, nullable: true,
                new Dictionary<string, string> { [VgiWireMetadata.VarargsKey] = VgiWireMetadata.VarargsTrueValue }),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("idx", Int64Type.Default, nullable: true),
            new Field("tag", StringType.Default, nullable: true),
            new Field("value", StringType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var rows = new List<(string Tag, string Value)>();
        for (var i = 0; i < initParams.Arguments.PositionalCount; i++)
        {
            rows.Add(ReadTaggedUnion(initParams.Arguments.PositionalArray(i)));
        }

        return new Producer(rows, initParams.OutputSchema);
    }

    private static (string Tag, string Value) ReadTaggedUnion(IArrowArray? array)
    {
        if (array is not UnionArray union || union.Length == 0)
        {
            throw new InvalidOperationException("union_varargs: missing value for a vararg.");
        }

        var typeId = union.TypeIds[0];
        var tag = union.Type.Fields[typeId].Name;
        var value = ScalarArgCodec.ReadScalar(union.Fields[typeId], 0);
        return (tag, value?.ToString() ?? "");
    }

    private sealed class Producer(IReadOnlyList<(string Tag, string Value)> rows, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                if (rows.Count > 0)
                {
                    var idxBuilder = new Int64Array.Builder();
                    var tagBuilder = new StringArray.Builder();
                    var valueBuilder = new StringArray.Builder();
                    for (var i = 0; i < rows.Count; i++)
                    {
                        idxBuilder.Append(i);
                        tagBuilder.Append(rows[i].Tag);
                        valueBuilder.Append(rows[i].Value);
                    }

                    output.Emit(new RecordBatch(
                        outputSchema, [idxBuilder.Build(), tagBuilder.Build(), valueBuilder.Build()], rows.Count));
                }
            }

            output.Finish();
        }
    }
}
