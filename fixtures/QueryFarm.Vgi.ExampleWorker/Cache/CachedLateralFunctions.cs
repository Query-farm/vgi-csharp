using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary>
/// "Blended" (a.k.a. vgi-python's <c>RowTransformFunction</c>) table-in-out functions — an
/// <see cref="ITableInOutFunction"/> whose <see cref="ITableInOutFunction.ArgumentsSchema"/> declares
/// ONLY plain typed positional args (NO <see cref="TableArgFields.Table"/>-marked field) AND
/// overrides <see cref="ITableInOutFunction.InputFromArgs"/> = true. The wire-level distinction
/// between a "streaming TABLE-arg" function and a "blended" one IS <c>InputFromArgs</c>
/// (<c>FunctionInfo.InputFromArgs</c>) — omitting it (the default, false) makes DuckDB's binder
/// REJECT a correlated <c>LATERAL f(t.x)</c> call outright ("does not support lateral join column
/// parameters"), discovered empirically against the real C++ extension while building this fixture
/// family. A blended call works both as a streaming column form (<c>FROM t, f(t.x)</c>) and as a
/// correlated <c>LATERAL f(t.x)</c> — DuckDB re-associates any outer/correlated columns itself; the
/// worker only ever sees its own declared positional arg columns on
/// <see cref="ITableInOutProcessor.Process"/>'s input batch (indexed by POSITION, like
/// <see cref="Scalar.ScalarProcessParams"/> — the wire column names are DuckDB's own synthetic ones,
/// not <see cref="ITableInOutFunction.ArgumentsSchema"/>'s).
/// </summary>
public sealed class CachedDoubleFunction : ITableInOutFunction
{
    public string Name => "cached_double";

    public string SchemaName => "main";

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("x", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("doubled", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(initParams.OutputSchema);

    private sealed class Processor(Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var x = (Int64Array)input.Column(0);
            var builder = new Int64Array.Builder();
            for (var i = 0; i < x.Length; i++)
            {
                builder.Append(x.IsNull(i) ? null : x.GetValue(i)!.Value * 2);
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], x.Length), CacheMetadata.PerValue(300));
        }
    }
}

/// <summary><c>cached_explode(n)</c> — a genuine 1:N blended map: for input row value <c>n</c>,
/// emits <c>n</c> output rows <c>i = 0..n-1</c> (<c>n=0</c> emits none) — stresses the per-value
/// memo's variable-row-count-per-input-tuple storage/replay (<c>per_value_multi_batch.test</c>,
/// <c>per_value_negative_memo.test</c>).</summary>
public sealed class CachedExplodeFunction : ITableInOutFunction
{
    public string Name => "cached_explode";

    public string SchemaName => "main";

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("i", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(initParams.OutputSchema);

    private sealed class Processor(Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var n = (Int64Array)input.Column(0);
            var builder = new Int64Array.Builder();
            var parentRows = new List<int>();
            for (var r = 0; r < n.Length; r++)
            {
                if (n.IsNull(r))
                {
                    continue;
                }

                var count = n.GetValue(r)!.Value;
                for (var i = 0L; i < count; i++)
                {
                    builder.Append(i);
                    parentRows.Add(r);
                }
            }

            // A fan-out (1->N) or filtering (1->0, e.g. n=0) blended LATERAL map must attach
            // per-output-row provenance whenever it is NOT a genuine per-row identity map —
            // otherwise the C++ side can't know which correlated/outer row each output row
            // belongs to. Format: vgi_rpc.parent_row#b64 = base64(raw little-endian int32[], one
            // entry per OUTPUT row = its 0-based parent INPUT row index) — see
            // vgi_lateral_batch_operator.cpp's DecodeParentRow. Identity requires each OUTPUT
            // row's parent to equal ITS OWN row index — matching total counts is NOT sufficient
            // (e.g. a deduped input batch of 3 rows with fans [0, 1, 2] also totals 3 output
            // rows, but output row 0's parent is input row 1, not 0 — a genuine fan-out, not an
            // identity map). Building the key metadata dict directly rather than via
            // CacheMetadata.PerValue avoids allocating it on the (very common) 1:1 fast path.
            var isIdentity = parentRows.Count == n.Length;
            for (var i = 0; isIdentity && i < parentRows.Count; i++)
            {
                isIdentity = parentRows[i] == i;
            }

            var metadata = new Dictionary<string, string>(CacheMetadata.PerValue(300));
            if (!isIdentity)
            {
                var bytes = new byte[parentRows.Count * sizeof(int)];
                for (var i = 0; i < parentRows.Count; i++)
                {
                    BitConverter.TryWriteBytes(bytes.AsSpan(i * sizeof(int), sizeof(int)), parentRows[i]);
                }

                metadata["vgi_rpc.parent_row#b64"] = Convert.ToBase64String(bytes);
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], parentRows.Count), metadata);
        }
    }
}

/// <summary><c>cached_reval_double(x)</c> — the LATERAL/blended half of the "always-revalidate"
/// (<c>ttl=0 + etag + revalidatable</c>) contract (<c>exchange_revalidate.test</c>), analogous to
/// <see cref="CachedRevalEchoFunction"/> but for a blended (per-row-arg) map. Reads
/// <c>vgi.cache.if_none_match</c> off this turn's incoming metadata (now threaded through by
/// <c>Internal.TableInOutExchangeStreamState</c>) and replies with a 0-row
/// <see cref="CacheMetadata.NotModified"/> batch when the worker's stable ETag still matches.</summary>
public sealed class CachedRevalDoubleFunction : ITableInOutFunction
{
    private const string Etag = "cached-reval-double-etag-v1";

    public string Name => "cached_reval_double";

    public string SchemaName => "main";

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("x", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("doubled", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(initParams.OutputSchema);

    private sealed class Processor(Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            if (RevalidationHelper.IsNotModified(output.InputMetadata, Etag))
            {
                var empty = new Int64Array.Builder().Build();
                output.Emit(new RecordBatch(outputSchema, [empty], 0), CacheMetadata.NotModified());
                return;
            }

            var x = (Int64Array)input.Column(0);
            var builder = new Int64Array.Builder();
            for (var i = 0; i < x.Length; i++)
            {
                builder.Append(x.IsNull(i) ? null : x.GetValue(i)!.Value * 2);
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], x.Length), CacheMetadata.Revalidatable(Etag));
        }
    }
}
