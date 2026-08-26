using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>repeat_value(count, values...)</c> — two VARARGS overloads sharing one name, disambiguated
/// by the varargs columns' TYPE (int64 vs. string), each emitting <c>count</c> copies of one row
/// built from the (bind-time-constant) vararg values — columns named <c>v0, v1, ...</c>, however
/// many vararg values the call site passed (a genuinely dynamic output schema, resolved from the
/// bound argument COUNT). Backs <c>overload/table_varargs_overload.test</c>.
/// </summary>
public sealed class RepeatValueIntFunction : ITableFunction
{
    public string Name => "repeat_value";

    public string Description => "Repeats a row of int64 varargs values `count` times";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("count", Int64Type.Default), VarargsField("values", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableBindParams bindParams) => BuildOutputSchema(bindParams.Arguments, Int64Type.Default);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var values = new List<long?>();
        for (var i = 1; i < initParams.Arguments.PositionalCount; i++)
        {
            values.Add(initParams.Arguments.Positional(i) is { } v ? Convert.ToInt64(v) : null);
        }

        return new Producer(count, initParams.OutputSchema, output =>
        {
            var builders = values.Select(_ => new Int64Array.Builder()).ToList();
            for (var i = 0; i < builders.Count; i++)
            {
                if (values[i] is { } v)
                {
                    builders[i].Append(v);
                }
                else
                {
                    builders[i].AppendNull();
                }
            }

            return builders.Select(b => (IArrowArray)b.Build()).ToList();
        });
    }

    internal static Field VarargsField(string name, IArrowType elementType) => new(
        name, elementType, nullable: true,
        new Dictionary<string, string> { [VgiWireMetadata.VarargsKey] = VgiWireMetadata.VarargsTrueValue });

    internal static Schema BuildOutputSchema(TableArguments arguments, IArrowType elementType)
    {
        var n = Math.Max(0, arguments.PositionalCount - 1);
        return new Schema(Enumerable.Range(0, n).Select(i => new Field($"v{i}", elementType, nullable: true)), metadata: null);
    }
}

public sealed class RepeatValueStrFunction : ITableFunction
{
    public string Name => "repeat_value";

    public string Description => "Repeats a row of string varargs values `count` times";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("count", Int64Type.Default), RepeatValueIntFunction.VarargsField("values", StringType.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableBindParams bindParams) =>
        RepeatValueIntFunction.BuildOutputSchema(bindParams.Arguments, StringType.Default);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var values = new List<string?>();
        for (var i = 1; i < initParams.Arguments.PositionalCount; i++)
        {
            values.Add(initParams.Arguments.Positional(i) as string);
        }

        return new Producer(count, initParams.OutputSchema, output =>
        {
            var builders = values.Select(_ => new StringArray.Builder()).ToList();
            for (var i = 0; i < builders.Count; i++)
            {
                builders[i].Append(values[i]);
            }

            return builders.Select(b => (IArrowArray)b.Build()).ToList();
        });
    }
}

/// <summary>Emits <c>count</c> copies of one fixed row (built once via <paramref name="buildRow"/>
/// and repeated column-by-column) — shared by both <c>repeat_value</c> overloads.</summary>
internal sealed class Producer(long count, Schema outputSchema, Func<OutputCollector, List<IArrowArray>> buildRow) : ITableFunctionProducer
{
    private long _next;

    public void Produce(OutputCollector output)
    {
        if (_next >= count)
        {
            output.Finish();
            return;
        }

        var rowColumns = buildRow(output);
        var rows = (int)(count - _next);
        var columns = rowColumns.Select(col => RepeatColumn(col, rows)).ToList();
        _next = count;
        output.Emit(new RecordBatch(outputSchema, columns, rows));
        output.Finish();
    }

    private static IArrowArray RepeatColumn(IArrowArray oneRow, int times) =>
        RowSelector.Select(oneRow, Enumerable.Repeat(0, times).ToList());
}
