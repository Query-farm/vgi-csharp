using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary><c>cached_echo(data TABLE)</c> — classic streaming table-in-out (M1) passthrough, cache
/// metadata added — backs <c>exchange_streaming.test</c>'s per-input-BATCH memoization proof.</summary>
public sealed class CachedEchoFunction : ITableInOutFunction
{
    public string Name => "cached_echo";

    public string SchemaName => "main";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor();

    private sealed class Processor : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output) => output.Emit(input, CacheMetadata.Ttl(300));
    }
}

/// <summary><c>cached_reval_echo(data TABLE)</c> — the classic-TABLE-arg half of the
/// "always-revalidate" contract (<c>exchange_revalidate.test</c>) — same not_modified logic as
/// <see cref="CachedRevalDoubleFunction"/>, applied to passthrough rows instead of a per-row map. The
/// output schema mirrors the input (passthrough), so a 0-row not_modified reply is just the incoming
/// batch sliced to zero rows — no per-type empty-array construction needed.</summary>
public sealed class CachedRevalEchoFunction : ITableInOutFunction
{
    private const string Etag = "cached-reval-echo-etag-v1";

    public string Name => "cached_reval_echo";

    public string SchemaName => "main";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => bindParams.InputSchema;

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor();

    private sealed class Processor : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            if (RevalidationHelper.IsNotModified(output.InputMetadata, Etag))
            {
                output.Emit(input.Slice(0, 0), CacheMetadata.NotModified());
                return;
            }

            output.Emit(input, CacheMetadata.Revalidatable(Etag));
        }
    }
}
