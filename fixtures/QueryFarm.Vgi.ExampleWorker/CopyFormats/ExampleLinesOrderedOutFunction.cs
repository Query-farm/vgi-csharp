namespace QueryFarm.Vgi.ExampleWorker.CopyFormats;

/// <summary>
/// <c>COPY ... TO (FORMAT 'example_lines_ordered_out', ...)</c> — identical to
/// <see cref="ExampleLinesOutFunction"/> except <see cref="SinkOrderDependent"/> forces a
/// single-threaded, source-ordered sink (test/sql/integration/copy_to/ordered.test).
/// </summary>
public sealed class ExampleLinesOrderedOutFunction() : ExampleLinesOutFunction("example_lines_ordered_out")
{
    public override bool SinkOrderDependent => true;
}
