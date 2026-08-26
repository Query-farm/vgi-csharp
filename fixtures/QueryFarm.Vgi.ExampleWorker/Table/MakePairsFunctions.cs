using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>make_pairs</c> — three overloads sharing one name and the same (2) argument COUNT,
/// distinguished purely by argument TYPES (int+int / str+str / int+str — mirroring the scalar
/// <c>pair_type</c> fixture's dispatch shape for table functions). Backs
/// <c>overload/table_overload.test</c>.
/// </summary>
public sealed class MakePairsIntFunction : ITableFunction
{
    public string Name => "make_pairs";

    public string Description => "Generate integer pairs (i, i*2)";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("start", Int64Type.Default), TableArgFields.Positional("stop", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("a", Int64Type.Default, nullable: true), new Field("b", Int64Type.Default, nullable: true)],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var start = initParams.Arguments.Int64(0);
        var stop = initParams.Arguments.Int64(1);
        return new OneShot(_ =>
        {
            var aBuilder = new Int64Array.Builder();
            var bBuilder = new Int64Array.Builder();
            var rows = 0;
            for (var i = start; i < stop; i++)
            {
                aBuilder.Append(i);
                bBuilder.Append(i * 2);
                rows++;
            }

            return rows == 0 ? null : new RecordBatch(initParams.OutputSchema, [aBuilder.Build(), bBuilder.Build()], rows);
        });
    }
}

public sealed class MakePairsStrFunction : ITableFunction
{
    private const int FixedRows = 5;

    public string Name => "make_pairs";

    public string Description => "Generate string pairs with prefix and suffix";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("prefix", StringType.Default), TableArgFields.Positional("suffix", StringType.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("a", StringType.Default, nullable: true), new Field("b", StringType.Default, nullable: true)],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var prefix = initParams.Arguments.StringPositional(0);
        var suffix = initParams.Arguments.StringPositional(1);
        return new OneShot(_ =>
        {
            var aBuilder = new StringArray.Builder();
            var bBuilder = new StringArray.Builder();
            for (var i = 0; i < FixedRows; i++)
            {
                aBuilder.Append($"{prefix}{i}");
                bBuilder.Append($"{suffix}{i}");
            }

            return new RecordBatch(initParams.OutputSchema, [aBuilder.Build(), bBuilder.Build()], FixedRows);
        });
    }
}

public sealed class MakePairsIntStrFunction : ITableFunction
{
    private const int FixedRows = 5;

    public string Name => "make_pairs";

    public string Description => "Generate mixed int/string pairs";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("start", Int64Type.Default), TableArgFields.Positional("label", StringType.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("a", Int64Type.Default, nullable: true), new Field("b", StringType.Default, nullable: true)],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var start = initParams.Arguments.Int64(0);
        var prefix = initParams.Arguments.StringPositional(1);
        return new OneShot(_ =>
        {
            var aBuilder = new Int64Array.Builder();
            var bBuilder = new StringArray.Builder();
            for (var i = 0; i < FixedRows; i++)
            {
                aBuilder.Append(start + i);
                bBuilder.Append($"{prefix}{i}");
            }

            return new RecordBatch(initParams.OutputSchema, [aBuilder.Build(), bBuilder.Build()], FixedRows);
        });
    }
}

/// <summary>Emits at most one non-empty batch (built by <paramref name="build"/>, which may return
/// <see langword="null"/> for an empty result) then finishes — shared by every <c>make_pairs</c>
/// overload, none of which need real multi-batch pacing.</summary>
internal sealed class OneShot(Func<OutputCollector, RecordBatch?> build) : ITableFunctionProducer
{
    private bool _done;

    public void Produce(OutputCollector output)
    {
        if (_done)
        {
            output.Finish();
            return;
        }

        _done = true;
        var batch = build(output);
        if (batch is not null)
        {
            output.Emit(batch);
        }

        output.Finish();
    }
}
