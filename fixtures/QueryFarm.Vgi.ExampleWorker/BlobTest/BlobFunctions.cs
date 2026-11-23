using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.BlobTest;

/// <summary>
/// The <c>blob_test</c> catalog: the SDK-neutral blob-vendor fixture contract (vgi
/// <c>docs/testing/blob-test-catalog.md</c>; reference vgi-python
/// <c>vgi/_test_fixtures/blob_catalog.py</c>) that the extension's
/// <c>test/sql/external_storage/</c> lane runs against: large scalar and table outputs, large
/// inputs and bind parameters, storage round trips, and the protocol 3.4 argument-reference
/// fixtures (<c>blob_param_*</c>).
/// </summary>
public static class BlobCatalog
{
    public const string Name = "blob_test";

    public static void Register(Worker worker)
    {
        worker.RegisterCatalog(new CatalogInfo { Name = Name }, exclusive: true);
        worker.RegisterScalar(new BlobRepeatFunction(), identity: Name);
        worker.RegisterScalar(new BlobLenFunction(), identity: Name);
        worker.RegisterScalar(new BlobParamScalarFunction(), identity: Name);
        worker.RegisterTable(new BlobRowsFunction(), identity: Name);
        worker.RegisterTable(new BlobParamEchoFunction(), identity: Name);
        worker.RegisterTable(new BlobParamBatchesFunction(), identity: Name);
        worker.RegisterTable(new BlobParamStreamsFunction(), identity: Name);
        worker.RegisterTableBuffering(new BlobBufferEchoFunction(), identity: Name);
        worker.RegisterTableInOut(new BlobTioFunction(), identity: Name);
        worker.RegisterTableInOut(new BlobStoreValuesFunction(), identity: Name);
        // A scan that binds blob_param_echo('t' x 65536) over RPC on every query (not inlined in the
        // catalog), repeatable within a query: the serialized-plan rebind must reuse the reference.
        worker.RegisterCatalogTable(new CatalogTable
        {
            Name = "blob_param_table",
            SchemaName = "main",
            Comment = "Scans blob_param_echo(repeat('t', 65536)), bound over RPC",
            ScanFunction = new BlobParamEchoFunction("blob_param_table_scan", BindRepeatability.StableWithinQuery),
            ScanArguments = [new string('t', 65536)],
            InlineScanFunction = false,
        }, identity: Name);
    }

    internal static readonly Field PayloadLen = new("payload_len", Int64Type.Default, nullable: true);
    internal static readonly Field PayloadSha256 = new("payload_sha256", StringType.Default, nullable: true);

    internal static string Sha256Hex(byte[] data) => Convert.ToHexStringLower(SHA256.HashData(data));

    /// <summary>The value at <paramref name="row"/> as DuckDB text (UTF-8 bytes), or null.</summary>
    internal static byte[]? TextBytes(IArrowArray array, int row)
    {
        if (array.IsNull(row))
        {
            return null;
        }

        return Encoding.UTF8.GetBytes(array switch
        {
            StringArray s => s.GetString(row),
            LargeStringArray s => s.GetString(row),
            StringViewArray s => s.GetString(row),
            Int64Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            Int32Array a => a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
            _ => Convert.ToString(ScalarArgCodec.ReadScalar(array, row), CultureInfo.InvariantCulture) ?? "",
        });
    }

    /// <summary><paramref name="batch"/>'s <paramref name="keep"/> columns plus the checksum columns
    /// of its first column.</summary>
    internal static RecordBatch WithChecksums(RecordBatch batch, IEnumerable<int> keep)
    {
        var lengths = new Int64Array.Builder();
        var hashes = new StringArray.Builder();
        for (var i = 0; i < batch.Length; i++)
        {
            AppendChecksum(TextBytes(batch.Column(0), i), lengths, hashes);
        }

        return Append(batch, keep, lengths.Build(), hashes.Build());
    }

    internal static void AppendChecksum(byte[]? value, Int64Array.Builder lengths, StringArray.Builder hashes)
    {
        if (value is null)
        {
            lengths.AppendNull();
            hashes.AppendNull();
            return;
        }

        lengths.Append(value.Length);
        hashes.Append(Sha256Hex(value));
    }

    internal static RecordBatch Append(RecordBatch batch, IEnumerable<int> keep, IArrowArray lengths, IArrowArray hashes)
    {
        var indices = keep.ToList();
        var fields = indices.Select(batch.Schema.GetFieldByIndex).Append(PayloadLen).Append(PayloadSha256).ToList();
        var columns = indices.Select(batch.Column).Append(lengths).Append(hashes).ToList();
        return new RecordBatch(new Schema(fields, metadata: null), columns, batch.Length);
    }

    internal static Schema WithChecksumFields(Schema input, int skip) =>
        new(input.FieldsList.Skip(skip).Append(PayloadLen).Append(PayloadSha256).ToList(), metadata: null);

    internal static byte[] WriteIpc(RecordBatch batch) => RecordBatchIpc.Write(batch);

    internal static RecordBatch ReadIpc(byte[] bytes) => RecordBatchIpc.Read(bytes);

    internal static RecordBatch ChecksumRow(Schema schema, string? payload, long? leading = null)
    {
        var bytes = payload is null ? null : Encoding.UTF8.GetBytes(payload);
        var lengths = new Int64Array.Builder();
        var hashes = new StringArray.Builder();
        AppendChecksum(bytes, lengths, hashes);
        IArrowArray[] columns = leading is { } lead
            ? [new Int64Array.Builder().Append(lead).Build(), lengths.Build(), hashes.Build()]
            : [lengths.Build(), hashes.Build()];
        return new RecordBatch(schema, columns, 1);
    }
}

/// <summary><c>blob_repeat(s, n)</c>: <c>s</c> repeated <c>n</c> times (large scalar outputs).</summary>
public sealed class BlobRepeatFunction : ScalarFn
{
    public override string Name => "blob_repeat";

    public override string Description => "Repeat a string n times (large scalar outputs)";

    private void Compute([Param] StringArray s, [Param] Int64Array n, StringArray.Builder result)
    {
        for (var i = 0; i < s.Length; i++)
        {
            if (s.IsNull(i) || n.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            var count = (int)Math.Max(0, n.GetValue(i)!.Value);
            result.Append(new StringBuilder(s.GetString(i).Length * count).Insert(0, s.GetString(i), count).ToString());
        }
    }
}

/// <summary><c>blob_len(s)</c>: the UTF-8 byte length (large scalar inputs).</summary>
public sealed class BlobLenFunction : ScalarFn
{
    public override string Name => "blob_len";

    public override string Description => "UTF-8 byte length of a string (large scalar inputs)";

    private void Compute([Param] StringArray s, Int64Array.Builder result)
    {
        for (var i = 0; i < s.Length; i++)
        {
            if (s.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(Encoding.UTF8.GetByteCount(s.GetString(i)));
        }
    }
}

/// <summary><c>blob_param_scalar(i, payload)</c>: <c>sha256(payload)</c> for each non-NULL
/// <c>i</c>; <c>payload</c> is a constant parameter, so it rides the bind (protocol 3.4).</summary>
public sealed class BlobParamScalarFunction : ScalarFn
{
    public override string Name => "blob_param_scalar";

    public override string Description => "sha256 of a constant payload for every non-NULL row";

    private void Compute([Param] Int64Array i, [ConstParam] string? payload, StringArray.Builder result)
    {
        var hash = payload is null ? null : BlobCatalog.Sha256Hex(Encoding.UTF8.GetBytes(payload));
        for (var row = 0; row < i.Length; row++)
        {
            if (i.IsNull(row) || hash is null)
            {
                result.AppendNull();
                continue;
            }

            result.Append(hash);
        }
    }
}

/// <summary><c>blob_rows(rows, width)</c>: row <c>i</c> is <c>chr(97 + i % 26)</c> repeated
/// <c>width</c> times, in 2,048-row batches (large table outputs).</summary>
public sealed class BlobRowsFunction : ITableFunction
{
    private const int BatchRows = 2048;

    public string Name => "blob_rows";

    public string Description => "rows x width characters (large table outputs)";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("rows", Int64Type.Default), TableArgFields.Positional("width", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("i", Int64Type.Default, nullable: true), new Field("payload", StringType.Default, nullable: true)], metadata: null);

    public void Bind(TableBindParams bindParams)
    {
        if (bindParams.Arguments.Int64(0) < 0 || bindParams.Arguments.Int64(1) < 0)
        {
            throw new VgiInvalidArgumentException("blob_rows: rows and width must be >= 0");
        }
    }

    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64(0);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.Arguments.Int64(0), (int)initParams.Arguments.Int64(1), OutputSchema);

    private sealed class Producer(long rows, int width, Schema schema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            var count = (int)Math.Min(BatchRows, rows - _next);
            if (count > 0)
            {
                var ids = new Int64Array.Builder();
                var payloads = new StringArray.Builder();
                for (var k = 0; k < count; k++)
                {
                    var i = _next + k;
                    ids.Append(i);
                    payloads.Append(new string((char)('a' + (i % 26)), width));
                }

                _next += count;
                output.Emit(new RecordBatch(schema, [ids.Build(), payloads.Build()], count));
            }

            if (_next >= rows)
            {
                output.Finish();
            }
        }
    }
}

/// <summary><c>blob_param_echo(payload)</c>: one row with the checksum columns of
/// <c>payload</c> (large bind parameters). Also, as <c>blob_param_table_scan</c> reporting
/// <c>STABLE_WITHIN_QUERY</c>, the scan behind <c>blob_param_table</c>.</summary>
public sealed class BlobParamEchoFunction(string name = "blob_param_echo", BindRepeatability repeatability = BindRepeatability.Unknown) : ITableFunction
{
    public string Name => name;

    public string Description => "Checksum columns of a (large) bind parameter";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("payload", StringType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([BlobCatalog.PayloadLen, BlobCatalog.PayloadSha256], metadata: null);

    public void Bind(TableBindParams bindParams)
    {
    }

    public long? Cardinality(TableBindParams bindParams) => 1;

    public BindRepeatability Repeatability(TableBindParams parameters) => repeatability;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new OneShot(BlobCatalog.ChecksumRow(OutputSchema, initParams.Arguments.StringPositional(0)));

    internal sealed class OneShot(RecordBatch batch) : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            output.Emit(batch);
            output.Finish();
        }
    }
}

/// <summary><c>blob_param_batches(payload, batches)</c>: <c>batches</c> one-row batches, one per
/// stream turn, each <c>(i, strlen(payload), sha256(payload))</c>.</summary>
public sealed class BlobParamBatchesFunction : ITableFunction
{
    public string Name => "blob_param_batches";

    public string Description => "One checksum row per stream turn, over a (large) bind parameter";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("payload", StringType.Default), TableArgFields.Positional("batches", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("i", Int64Type.Default, nullable: true), BlobCatalog.PayloadLen, BlobCatalog.PayloadSha256], metadata: null);

    public void Bind(TableBindParams bindParams)
    {
        if (bindParams.Arguments.Int64(1) < 0)
        {
            throw new VgiInvalidArgumentException("blob_param_batches: batches must be >= 0");
        }
    }

    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64(1);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.Arguments.StringPositional(0), initParams.Arguments.Int64(1), OutputSchema);

    private sealed class Producer(string? payload, long batches, Schema schema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next < batches)
            {
                output.Emit(BlobCatalog.ChecksumRow(schema, payload, _next));
                _next++;
            }

            if (_next >= batches)
            {
                output.Finish();
            }
        }
    }
}

/// <summary><c>blob_param_streams(payload, streams)</c>: one bind planned as <c>streams</c>
/// splits; each stream emits <c>(stream, strlen(payload), sha256(payload))</c>. Every stream's init
/// embeds the same <c>bind_call</c>, so the process resolves the argument once.</summary>
public sealed class BlobParamStreamsFunction : ITableFunction
{
    public string Name => "blob_param_streams";

    public string Description => "Parallel streams over one (large) bind parameter";

    public bool SupportsSplits => true;

    public int? MaxWorkers => 16;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("payload", StringType.Default), TableArgFields.Positional("streams", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("stream", Int64Type.Default, nullable: true), BlobCatalog.PayloadLen, BlobCatalog.PayloadSha256], metadata: null);

    public void Bind(TableBindParams bindParams)
    {
        if (bindParams.Arguments.Int64(1) < 1)
        {
            throw new VgiInvalidArgumentException("blob_param_streams: streams must be >= 1");
        }
    }

    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64(1);

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request)
    {
        var streams = bindParams.Arguments.Int64(1);
        var splits = new List<ScanSplit>();
        for (long s = 0; s < streams; s++)
        {
            splits.Add(ScanSplit.Of(BitConverter.GetBytes(s), rows: 1, bytes: 64));
        }

        return new PlanResult { Splits = splits, MaxWorkers = Math.Min(streams, 16) };
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payload = initParams.Arguments.StringPositional(0);
        var batches = (initParams.SplitPayloads ?? [BitConverter.GetBytes(0L)])
            .Select(split => BlobCatalog.ChecksumRow(OutputSchema, payload, BitConverter.ToInt64(split)))
            .ToList();
        return new Accumulate.BatchListProducer(batches);
    }
}

/// <summary><c>blob_buffer_echo(t)</c>: <i>process</i> stores each input batch (an IPC stream) as
/// one value appended to the execution's log; <i>finalize</i> replays every stored batch with the
/// checksum columns of its first column, one batch per tick.</summary>
public sealed class BlobBufferEchoFunction : ITableBufferingFunction
{
    private const string Namespace = "blob_buffer_echo";
    private const string LogKey = "batches";

    public string Name => "blob_buffer_echo";

    public string Description => "Stores every input batch in worker storage, then replays it";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("t")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public void Bind(TableInOutBindParams bindParams)
    {
    }

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => BlobCatalog.WithChecksumFields(bindParams.InputSchema, 0);

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams)
    {
        processParams.Storage.Append(Namespace, LogKey, BlobCatalog.WriteIpc(batch));
        return processParams.ExecutionId;
    }

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
        [combineParams.ExecutionId];

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new Replay(finalizeParams.Storage.ScanLog(Namespace, LogKey));

    private sealed class Replay(IReadOnlyList<byte[]> stored) : ITableFunctionProducer
    {
        private int _next;

        public void Produce(OutputCollector output)
        {
            if (_next < stored.Count)
            {
                var batch = BlobCatalog.ReadIpc(stored[_next++]);
                output.Emit(BlobCatalog.WithChecksums(batch, Enumerable.Range(0, batch.ColumnCount)));
            }

            if (_next >= stored.Count)
            {
                output.Finish();
            }
        }
    }
}

/// <summary><c>blob_tio(t)</c>: each input batch is stored (an IPC stream) as one value under a
/// fresh key, read back, deleted, and emitted as read back with the checksum columns of its first
/// column.</summary>
public sealed class BlobTioFunction : ITableInOutFunction
{
    public string Name => "blob_tio";

    public string Description => "Round-trips every input batch through worker storage";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("t")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => BlobCatalog.WithChecksumFields(bindParams.InputSchema, 0);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(new FunctionStorage(initParams.ExecutionId ?? Guid.NewGuid().ToByteArray()));

    private sealed class Processor(FunctionStorage storage) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var key = Guid.NewGuid().ToString("N");
            storage.WriteSingle("blob_tio", key, BlobCatalog.WriteIpc(input));
            var stored = storage.ReadSingle("blob_tio", key)
                ?? throw new InvalidOperationException($"blob_tio: storage value {key} is missing straight after the write");
            storage.DeleteSingle("blob_tio", key);
            var batch = BlobCatalog.ReadIpc(stored);
            output.Emit(BlobCatalog.WithChecksums(batch, Enumerable.Range(0, batch.ColumnCount)));
        }
    }
}

/// <summary><c>blob_store_values(t)</c>: the UTF-8 bytes of each non-NULL value of the first
/// column are stored as their own storage value, read back in one pass, deleted, and the input
/// columns 2..n are emitted with the checksum columns of the bytes read back.</summary>
public sealed class BlobStoreValuesFunction : ITableInOutFunction
{
    public string Name => "blob_store_values";

    public string Description => "Stores each first-column value as its own storage value";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("t")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => BlobCatalog.WithChecksumFields(bindParams.InputSchema, 1);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(new FunctionStorage(initParams.ExecutionId ?? Guid.NewGuid().ToByteArray()));

    private sealed class Processor(FunctionStorage storage) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var keys = new string?[input.Length];
            var prefix = Guid.NewGuid().ToString("N");
            for (var i = 0; i < input.Length; i++)
            {
                if (BlobCatalog.TextBytes(input.Column(0), i) is { } value)
                {
                    keys[i] = $"{prefix}{i}";
                    storage.WriteSingle("blob_store_values", keys[i]!, value);
                }
            }

            var lengths = new Int64Array.Builder();
            var hashes = new StringArray.Builder();
            foreach (var key in keys)
            {
                if (key is null)
                {
                    BlobCatalog.AppendChecksum(null, lengths, hashes);
                    continue;
                }

                var stored = storage.ReadSingle("blob_store_values", key)
                    ?? throw new InvalidOperationException($"blob_store_values: storage value {key} is missing straight after the write");
                storage.DeleteSingle("blob_store_values", key);
                BlobCatalog.AppendChecksum(stored, lengths, hashes);
            }

            output.Emit(BlobCatalog.Append(input, Enumerable.Range(1, input.ColumnCount - 1), lengths.Build(), hashes.Build()));
        }
    }
}
