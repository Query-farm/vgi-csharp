using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>profiling_demo(n, batch_size:=)</c> — a chunked <c>0..n-1</c> generator whose
/// <see cref="ITableFunction.DynamicToString"/> override surfaces per-execution diagnostics
/// (<c>rows_produced</c>/<c>batches_emitted</c>/<c>elapsed_ms</c>) under <c>EXPLAIN ANALYZE</c>.
/// Backs <c>table/dynamic_to_string.test</c>. <see cref="MaxWorkers"/> is pinned to 1 (vgi-python's
/// <c>@init_single_worker</c>) so <c>batches_emitted</c> is an EXACT, deterministic
/// <c>ceil(n / batch_size)</c> regardless of the session's <c>threads</c> setting — a parallel scan
/// would otherwise split the work across multiple <see cref="FunctionStorage"/> log streams (still
/// summed correctly by <see cref="DynamicToString"/>, but batches-per-thread wouldn't be
/// individually predictable).
/// </summary>
public sealed class ProfilingDemoFunction : ITableFunction
{
    private const string StorageNamespace = "profiling_demo";
    private const string StorageKey = "batches";

    public string Name => "profiling_demo";

    public string Description => "Chunked generator whose dynamic_to_string surfaces per-execution diagnostics";

    public int? MaxWorkers => 1;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("n", Int64Type.Default),
            TableArgFields.Named("batch_size", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var n = initParams.Arguments.Int64(0);
        var batchSize = Math.Max(1, initParams.Arguments.Int64Named("batch_size", 1000));
        var storage = initParams.ExecutionId is { Length: > 0 } id ? new FunctionStorage(id) : null;
        return new Producer(n, batchSize, initParams.OutputSchema, storage);
    }

    /// <summary>Sums every batch this execution's producer(s) persisted (see <see cref="Producer.Produce"/>)
    /// — empty when the function was never actually driven to completion (e.g. plain <c>EXPLAIN</c>,
    /// which never opens a producer stream at all) or nothing was ever emitted.</summary>
    public IReadOnlyDictionary<string, string> DynamicToString(TableBindParams bindParams, byte[] executionId)
    {
        if (executionId.Length == 0)
        {
            return new Dictionary<string, string>();
        }

        var entries = new FunctionStorage(executionId).ScanLog(StorageNamespace, StorageKey);
        if (entries.Count == 0)
        {
            return new Dictionary<string, string>();
        }

        long rows = 0;
        var elapsedUs = 0.0;
        foreach (var entry in entries)
        {
            rows += BitConverter.ToInt64(entry, 0);
            elapsedUs = Math.Max(elapsedUs, BitConverter.ToDouble(entry, 8));
        }

        return new Dictionary<string, string>
        {
            ["rows_produced"] = rows.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["batches_emitted"] = entries.Count.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["elapsed_ms"] = (elapsedUs / 1000.0).ToString("F2", System.Globalization.CultureInfo.InvariantCulture),
        };
    }

    private sealed class Producer(long n, long batchSize, Schema outputSchema, FunctionStorage? storage) : ITableFunctionProducer
    {
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= n)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(batchSize, n - _next);
            var builder = new Int64Array.Builder();
            builder.Reserve(rows);
            for (var i = 0; i < rows; i++)
            {
                builder.Append(_next + i);
            }

            _next += rows;

            var entry = new byte[16];
            BitConverter.GetBytes((long)rows).CopyTo(entry, 0);
            BitConverter.GetBytes(_stopwatch.Elapsed.TotalMicroseconds).CopyTo(entry, 8);
            storage?.Append(StorageNamespace, StorageKey, entry);

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows));
            if (_next >= n)
            {
                output.Finish();
            }
        }
    }
}
