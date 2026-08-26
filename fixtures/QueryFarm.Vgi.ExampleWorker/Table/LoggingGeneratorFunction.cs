using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Logging;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>logging_generator(count)</c> — emits <c>count</c> rows and, along the way, client-visible log
/// messages (<c>OutputCollector.ClientLog</c>) that surface via DuckDB's <c>duckdb_logs()</c>.
/// Backs <c>logging_generator.test</c>.
/// </summary>
public sealed class LoggingGeneratorFunction : ITableFunction
{
    public string Name => "logging_generator";

    public string Description => "Emits log messages during generation";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        return new Producer(count, initParams.OutputSchema);
    }

    private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
    {
        private const int BatchSize = 1000;
        private long _next;
        private bool _startedLogged;

        public void Produce(OutputCollector output)
        {
            if (!_startedLogged)
            {
                output.ClientLog(VgiLogLevel.Info, $"Starting generation of {count} values");
                _startedLogged = true;
            }

            if (_next >= count)
            {
                output.ClientLog(VgiLogLevel.Info, "Generation complete");
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, count - _next);
            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(_next + i);
            }

            _next += rows;
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows));
            if (_next >= count)
            {
                output.ClientLog(VgiLogLevel.Info, "Generation complete");
                output.Finish();
            }
        }
    }
}
