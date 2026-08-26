using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// <c>repeat_inputs(repeat_count, data TABLE)</c> — duplicates each INCOMING batch
/// <c>repeat_count</c> times. A table-in-out <see cref="ITableInOutProcessor.Process"/> turn may
/// emit AT MOST ONE output batch (see <see cref="OutputCollector.Emit(RecordBatch)"/>'s "at most
/// one per turn" contract), so "repeat the batch" means concatenating <c>repeat_count</c> copies of
/// it into one bigger output batch in place, not calling <c>Emit</c> more than once — no FINALIZE
/// phase needed. Backs <c>table_in_out/repeat_inputs/{basic,types,scale}.test</c>.
/// </summary>
public sealed class RepeatInputsFunction : ITableInOutFunction
{
    public string Name => "repeat_inputs";

    public string Description => "Duplicates each input batch N times";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("repeat_count", Int64Type.Default), TableArgFields.Table("data")],
        metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(initParams.Arguments.Int64(0));

    private sealed class Processor(long repeatCount) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output) => output.Emit(Repeat(input, repeatCount));

        private static RecordBatch Repeat(RecordBatch input, long repeatCount)
        {
            if (repeatCount <= 0 || input.Length == 0)
            {
                return input.Slice(0, 0);
            }

            if (repeatCount == 1)
            {
                return input;
            }

            var times = (int)repeatCount;
            var columns = new IArrowArray[input.ColumnCount];
            for (var c = 0; c < input.ColumnCount; c++)
            {
                columns[c] = RepeatColumn(input.Column(c), times);
            }

            return new RecordBatch(input.Schema, columns, input.Length * times);
        }

        /// <summary>Repeats one column <paramref name="times"/> times — DictionaryArray (an ENUM
        /// column) needs special handling: the vendored <c>ArrayDataConcatenator</c> has no
        /// <c>IArrowTypeVisitor&lt;DictionaryType&gt;</c>, and since <c>DictionaryType</c> IS-A
        /// <c>FixedWidthType</c>, concatenating it directly dispatches to the plain
        /// <c>FixedWidthType</c> visitor, which drops the dictionary VALUES array reference
        /// entirely (decodes as empty). Work around it exactly as
        /// <c>ConstantColumnsFunction.Broadcast</c> does: concatenate only the plain INDEX array,
        /// then re-wrap with the ORIGINAL dictionary values array (unchanged by a repeat).</summary>
        private static IArrowArray RepeatColumn(IArrowArray column, int times)
        {
            if (column is DictionaryArray dict)
            {
                var indices = RepeatPlain(dict.Indices, times);
                return new DictionaryArray((DictionaryType)dict.Data.DataType, indices, dict.Dictionary);
            }

            return RepeatPlain(column, times);
        }

        private static IArrowArray RepeatPlain(IArrowArray column, int times)
        {
            var repeated = new IArrowArray[times];
            System.Array.Fill(repeated, column);
            return ArrowArrayConcatenator.Concatenate(repeated);
        }
    }
}
