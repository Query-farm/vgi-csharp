using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.ExampleWorker.CopyFormats;

/// <summary>
/// <c>COPY ... FROM (FORMAT 'example_lines', ...)</c> — the toy delimited-text reader backing
/// test/sql/integration/copy_from/*.test. Options: <c>delimiter</c> (default <c>,</c>),
/// <c>null_string</c> (REQUIRED — worker-side enforced), <c>skip_rows</c> (default 0),
/// <c>on_error</c> (<c>'raise'</c> default or <c>'skip'</c> — a row whose column count doesn't
/// match <see cref="Protocol.CopyFromContext.ExpectedSchema"/> either throws or is dropped).
/// </summary>
public sealed class ExampleLinesFunction : CopyFromFunction
{
    public override string Name => "example_lines";

    public override string Description => "Toy delimited-text reader for tests";

    public override Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.NamedWithDoc("delimiter", StringType.Default, "Field separator"),
            TableArgFields.NamedWithDoc("null_string", StringType.Default, "Token parsed as SQL NULL"),
            TableArgFields.NamedWithDoc("skip_rows", Int64Type.Default, "Leading lines to skip before data"),
            TableArgFields.NamedWithDoc("on_error", StringType.Default, "Behavior on a row whose column count does not match the target"),
        ],
        metadata: null);

    protected override void OnBind(TableBindParams bindParams, Protocol.CopyFromContext copyFrom)
    {
        if (bindParams.Arguments.NamedArray("null_string") is null)
        {
            throw new InvalidOperationException($"{Name}: required option 'null_string' is missing.");
        }
    }

    protected override void Read(string path, TableInitParams initParams, Schema expectedSchema, Action<RecordBatch> emit)
    {
        var delimiter = initParams.Arguments.StringNamed("delimiter", ",");
        var nullString = initParams.Arguments.StringNamed("null_string", "");
        var skipRows = initParams.Arguments.Int64Named("skip_rows", 0);
        var onError = initParams.Arguments.StringNamed("on_error", "raise");

        var columnCount = expectedSchema.FieldsList.Count;
        var builders = expectedSchema.FieldsList.Select(f => DelimitedLineCodec.CreateBuilder(f.DataType)).ToList();
        var rowCount = 0;

        foreach (var line in File.ReadLines(path).Skip((int)skipRows))
        {
            var fields = line.Split(delimiter);
            if (fields.Length != columnCount)
            {
                if (onError == "skip")
                {
                    continue;
                }

                throw new InvalidOperationException(
                    $"{Name}: row has {fields.Length} field(s), expected {columnCount}: '{line}'");
            }

            for (var i = 0; i < columnCount; i++)
            {
                builders[i].Append(fields[i], nullString);
            }

            rowCount++;
        }

        if (rowCount > 0)
        {
            emit(new RecordBatch(expectedSchema, builders.Select(b => b.Build()).ToList(), rowCount));
        }
    }
}
