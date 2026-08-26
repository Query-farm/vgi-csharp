using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>make_series</c> — five overloads sharing one name: three distinguished purely by POSITIONAL
/// ARGUMENT COUNT (1/2/3), plus two MORE 1-argument overloads distinguished from each other and
/// from the count-form by argument TYPE (int64 count / string CSV / double step) — pins
/// <see cref="Internal.OverloadResolver"/>'s combined arity-and-type matching for table functions.
/// Backs <c>overload/table_overload.test</c>.
/// </summary>
public sealed class MakeSeriesCountFunction : ITableFunction
{
    public string Name => "make_series";

    public string Description => "Generate integers from 0 to count-1";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.PositionalWithRange("count", Int64Type.Default, ge: 0)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("value", Int64Type.Default, nullable: true)], metadata: null);

    public void Bind(TableBindParams bindParams) => Validate(bindParams.Arguments);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        Validate(initParams.Arguments);
        var count = initParams.Arguments.Int64(0);
        return new Int64RangeProducer(0, count, 1, initParams.OutputSchema);
    }

    /// <summary>Enforces the declared <c>count &gt;= 0</c> range (<c>catalog/function_arguments_constraints.test</c>) —
    /// <c>vgi_function_arguments()</c> surfacing the constraint is only half the contract; a bind
    /// must actually reject an out-of-range constant too.</summary>
    private static void Validate(TableArguments args)
    {
        if (args.Int64(0) < 0)
        {
            throw new InvalidOperationException("Argument 'count' must be >= 0");
        }
    }
}

public sealed class MakeSeriesRangeFunction : ITableFunction
{
    public string Name => "make_series";

    public string Description => "Generate integers from start to stop-1";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("start", Int64Type.Default), TableArgFields.Positional("stop", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("value", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var start = initParams.Arguments.Int64(0);
        var stop = initParams.Arguments.Int64(1);
        return new Int64RangeProducer(start, stop, 1, initParams.OutputSchema);
    }
}

public sealed class MakeSeriesRangeStepFunction : ITableFunction
{
    public string Name => "make_series";

    public string Description => "Generate integers from start to stop-1 with step";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("start", Int64Type.Default),
            TableArgFields.Positional("stop", Int64Type.Default),
            TableArgFields.PositionalWithRange("step", Int64Type.Default, ge: 1),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("value", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var start = initParams.Arguments.Int64(0);
        var stop = initParams.Arguments.Int64(1);
        var step = initParams.Arguments.Int64(2);
        return new Int64RangeProducer(start, stop, step <= 0 ? 1 : step, initParams.OutputSchema);
    }
}

/// <summary>Emits a single batch of <c>start, start+step, start+2*step, ...</c> while strictly less
/// than <c>stop</c> — used by all three integer-arity <c>make_series</c> overloads.</summary>
internal sealed class Int64RangeProducer(long start, long stop, long step, Schema outputSchema) : ITableFunctionProducer
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
        var builder = new Int64Array.Builder();
        var rows = 0;
        for (var v = start; v < stop; v += step)
        {
            builder.Append(v);
            rows++;
        }

        if (rows > 0)
        {
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows));
        }

        output.Finish();
    }
}

public sealed class MakeSeriesCsvFunction : ITableFunction
{
    public string Name => "make_series";

    public string Description => "Parse comma-separated integers into rows";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("values", StringType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("value", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var csv = initParams.Arguments.StringPositional(0);
        var values = csv.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(long.Parse).ToList();
        return new CsvProducer(values, initParams.OutputSchema);
    }

    private sealed class CsvProducer(List<long> values, Schema outputSchema) : ITableFunctionProducer
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
            if (values.Count > 0)
            {
                var builder = new Int64Array.Builder();
                foreach (var v in values)
                {
                    builder.Append(v);
                }

                output.Emit(new RecordBatch(outputSchema, [builder.Build()], values.Count));
            }

            output.Finish();
        }
    }
}

public sealed class MakeSeriesStepFunction : ITableFunction
{
    public string Name => "make_series";

    public string Description => "Generate 10 float values with given step size";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("step", DoubleType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("value", DoubleType.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var step = initParams.Arguments.PositionalArray(0) is DoubleArray d && !d.IsNull(0) ? d.GetValue(0)!.Value : 0.0;
        return new StepProducer(step, initParams.OutputSchema);
    }

    private sealed class StepProducer(double step, Schema outputSchema) : ITableFunctionProducer
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
            var builder = new DoubleArray.Builder();
            for (var i = 0; i < 10; i++)
            {
                builder.Append(i * step);
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], 10));
            output.Finish();
        }
    }
}
