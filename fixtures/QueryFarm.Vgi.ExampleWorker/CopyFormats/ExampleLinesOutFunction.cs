using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;

namespace QueryFarm.Vgi.ExampleWorker.CopyFormats;

/// <summary>
/// <c>COPY ... TO (FORMAT 'example_lines_out', ...)</c> — the toy delimited-text writer backing
/// test/sql/integration/copy_to/*.test. Options: <c>delimiter</c> (default <c>,</c>),
/// <c>null_string</c> (REQUIRED), <c>header</c>/<c>header_repeat</c> (default false / 1, ranged
/// 1..5), <c>fail_on_value</c> (default disabled — throws mid-write on a matching cell, for
/// failure/recovery testing), <c>on_exists</c> (<c>'overwrite'</c> default or <c>'error'</c>).
///
/// Sink batches are formatted to text immediately and appended to execution-scoped
/// <see cref="TableBufferingProcessParams.Storage"/> (never written to the real destination
/// directly — multiple Sink connections, in-process or cross-process, would race on one file);
/// <see cref="Close"/> (Combine, exactly once) assembles every logged chunk plus an optional
/// header into the real destination.
/// </summary>
public class ExampleLinesOutFunction(string name = "example_lines_out") : CopyToFunction
{
    private const string DataNamespace = "lines";
    private const string DataKey = "data";
    private const string HeaderNamespace = "lines";
    private const string HeaderKey = "header";

    public override string Name => name;

    public override string Description => "Toy delimited-text writer for tests";

    public override Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.NamedWithDoc("delimiter", StringType.Default, "Field separator"),
            TableArgFields.NamedWithDoc("null_string", StringType.Default, "Token written for SQL NULL"),
            TableArgFields.NamedWithDoc("header", BooleanType.Default, "Write a header row of column names"),
            TableArgFields.NamedWithDoc("header_repeat", Int64Type.Default, "When header=true, write the header line this many times"),
            TableArgFields.NamedWithDoc("fail_on_value", StringType.Default, "If non-empty, fail mid-write when a cell equals this value"),
            TableArgFields.NamedWithDoc("on_exists", StringType.Default, "Behavior when the destination file already exists"),
        ],
        metadata: null);

    protected override void OnBind(TableInOutBindParams bindParams, CopyToContext copyTo)
    {
        if (bindParams.Arguments.NamedArray("null_string") is null)
        {
            throw new InvalidOperationException($"{Name}: required option 'null_string' is missing.");
        }

        var headerRepeat = bindParams.Arguments.Int64Named("header_repeat", 1);
        if (headerRepeat is < 1 or > 5)
        {
            throw new InvalidOperationException($"{Name}: option 'header_repeat' must be between 1 and 5 (got {headerRepeat}).");
        }

        var onExists = bindParams.Arguments.StringNamed("on_exists", "overwrite");
        if (onExists is not ("overwrite" or "error"))
        {
            throw new InvalidOperationException($"{Name}: option 'on_exists' must be one of 'overwrite', 'error' (got '{onExists}').");
        }
    }

    protected override void Write(RecordBatch batch, TableBufferingProcessParams processParams, string filePath)
    {
        var delimiter = processParams.Arguments.StringNamed("delimiter", ",");
        var nullString = processParams.Arguments.StringNamed("null_string", "");
        var failOnValue = processParams.Arguments.StringNamed("fail_on_value", "");

        var columnCount = batch.Schema.FieldsList.Count;
        var lines = new StringBuilder();
        var cells = new string[columnCount];
        for (var r = 0; r < batch.Length; r++)
        {
            for (var c = 0; c < columnCount; c++)
            {
                var value = DelimitedLineCodec.FormatValue(batch.Column(c), r, nullString);
                if (failOnValue.Length > 0 && value == failOnValue)
                {
                    throw new InvalidOperationException($"{Name}: fail_on_value triggered on value '{failOnValue}'.");
                }

                cells[c] = value;
            }

            lines.Append(string.Join(delimiter, cells)).Append('\n');
        }

        processParams.Storage.Append(DataNamespace, DataKey, Encoding.UTF8.GetBytes(lines.ToString()));

        // Idempotent: every batch shares the same schema, so re-storing this is harmless — it's
        // what lets Close() build a header even when a batch was actually written (rather than
        // needing InputSchema, kept as the fallback for a truly empty source).
        var header = string.Join(delimiter, batch.Schema.FieldsList.Select(f => f.Name));
        processParams.Storage.Append(HeaderNamespace, HeaderKey, Encoding.UTF8.GetBytes(header));
    }

    protected override void Close(TableBufferingCombineParams combineParams, string filePath)
    {
        var delimiter = combineParams.Arguments.StringNamed("delimiter", ",");
        var header = combineParams.Arguments.BoolNamed("header", false);
        var headerRepeat = combineParams.Arguments.Int64Named("header_repeat", 1);
        var onExists = combineParams.Arguments.StringNamed("on_exists", "overwrite");

        if (onExists == "error" && File.Exists(filePath))
        {
            throw new InvalidOperationException($"{Name}: destination '{filePath}' already exists.");
        }

        var output = new StringBuilder();
        if (header)
        {
            var headerLine = combineParams.Storage.ScanLog(HeaderNamespace, HeaderKey).FirstOrDefault() is { } headerBytes
                ? Encoding.UTF8.GetString(headerBytes)
                : combineParams.InputSchema is { } schema
                    ? string.Join(delimiter, schema.FieldsList.Select(f => f.Name))
                    : "";

            for (var i = 0; i < headerRepeat; i++)
            {
                output.Append(headerLine).Append('\n');
            }
        }

        foreach (var chunk in combineParams.Storage.ScanLog(DataNamespace, DataKey))
        {
            output.Append(Encoding.UTF8.GetString(chunk));
        }

        File.WriteAllText(filePath, output.ToString());
    }
}
