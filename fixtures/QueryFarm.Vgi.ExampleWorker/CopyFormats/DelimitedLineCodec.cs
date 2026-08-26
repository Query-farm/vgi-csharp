using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.ExampleWorker.CopyFormats;

/// <summary>
/// Toy delimited-text encode/decode shared by the <c>example_lines</c>/<c>example_lines_out</c>/
/// <c>example_lines_ordered_out</c> COPY format fixtures (test/sql/integration/copy_to/*.test,
/// copy_from/*.test) — one line per row, fields joined/split by a configurable delimiter, a
/// configurable NULL token. Deliberately minimal (no quoting/escaping): the test data never puts a
/// delimiter character inside a field value.
/// </summary>
internal static class DelimitedLineCodec
{
    public static string FormatValue(IArrowArray array, int row, string nullString)
    {
        if (array.IsNull(row))
        {
            return nullString;
        }

        return array switch
        {
            Int8Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            Int16Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            Int32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt8Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt16Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            UInt64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            FloatArray a => a.GetValue(row)!.Value.ToString("R", CultureInfo.InvariantCulture),
            DoubleArray a => a.GetValue(row)!.Value.ToString("R", CultureInfo.InvariantCulture),
            BooleanArray a => a.GetValue(row)!.Value ? "true" : "false",
            StringArray a => a.GetString(row),
            _ => throw new NotSupportedException($"example_lines: unsupported column type '{array.Data.DataType}' for writing."),
        };
    }

    public static ColumnBuilder CreateBuilder(IArrowType type) => type switch
    {
        Int32Type => new Int32ColumnBuilder(),
        Int64Type => new Int64ColumnBuilder(),
        DoubleType => new DoubleColumnBuilder(),
        BooleanType => new BooleanColumnBuilder(),
        StringType => new StringColumnBuilder(),
        _ => throw new NotSupportedException($"example_lines: unsupported column type '{type}' for reading."),
    };

    /// <summary>Accumulates one output column's parsed values across every read row.</summary>
    internal abstract class ColumnBuilder
    {
        public abstract void Append(string? text, string nullString);

        public abstract IArrowArray Build();
    }

    private sealed class Int32ColumnBuilder : ColumnBuilder
    {
        private readonly Int32Array.Builder _builder = new();

        public override void Append(string? text, string nullString)
        {
            if (text is null || text == nullString)
            {
                _builder.AppendNull();
            }
            else
            {
                _builder.Append(int.Parse(text, CultureInfo.InvariantCulture));
            }
        }

        public override IArrowArray Build() => _builder.Build();
    }

    private sealed class Int64ColumnBuilder : ColumnBuilder
    {
        private readonly Int64Array.Builder _builder = new();

        public override void Append(string? text, string nullString)
        {
            if (text is null || text == nullString)
            {
                _builder.AppendNull();
            }
            else
            {
                _builder.Append(long.Parse(text, CultureInfo.InvariantCulture));
            }
        }

        public override IArrowArray Build() => _builder.Build();
    }

    private sealed class DoubleColumnBuilder : ColumnBuilder
    {
        private readonly DoubleArray.Builder _builder = new();

        public override void Append(string? text, string nullString)
        {
            if (text is null || text == nullString)
            {
                _builder.AppendNull();
            }
            else
            {
                _builder.Append(double.Parse(text, CultureInfo.InvariantCulture));
            }
        }

        public override IArrowArray Build() => _builder.Build();
    }

    private sealed class BooleanColumnBuilder : ColumnBuilder
    {
        private readonly BooleanArray.Builder _builder = new();

        public override void Append(string? text, string nullString)
        {
            if (text is null || text == nullString)
            {
                _builder.AppendNull();
            }
            else
            {
                _builder.Append(bool.Parse(text));
            }
        }

        public override IArrowArray Build() => _builder.Build();
    }

    private sealed class StringColumnBuilder : ColumnBuilder
    {
        private readonly StringArray.Builder _builder = new();

        public override void Append(string? text, string nullString)
        {
            if (text is null || text == nullString)
            {
                _builder.AppendNull();
            }
            else
            {
                _builder.Append(text);
            }
        }

        public override IArrowArray Build() => _builder.Build();
    }
}
