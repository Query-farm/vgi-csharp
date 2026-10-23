using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.ExampleWorker.Cache;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// Continued-output fixtures (<see cref="ContinuedOutput"/>): <c>blended_paged</c>,
/// <c>blended_sliced</c> and <c>paged_explode</c>, each answering one input batch with several
/// replies, the way a function backed by a paged API would. Their contracts are the "Fixture
/// contract" headers of the extension's <c>table_in_out/{lateral_continuation,input_rows_per_call,
/// continuation_classic,continuation_resume_error_revalidate}.test</c>; ported from vgi-python's
/// <c>BlendedPagedFunction</c>/<c>BlendedSlicedFunction</c>/<c>PagedExplodeFunction</c>. The
/// processor keeps only the input's counts and a position in them, never output rows.
/// </summary>
internal static class Paged
{
    /// <summary>The named options every paged fixture shares (<c>blended_sliced</c> takes only the
    /// first two).</summary>
    public static Field[] NamedArgs(bool full) => full
        ?
        [
            TableArgFields.Named("page_size", Int64Type.Default),
            TableArgFields.Named("cache_ttl", Int64Type.Default),
            TableArgFields.Named("fail_on_page", Int64Type.Default),
            TableArgFields.Named("revalidate", BooleanType.Default),
            TableArgFields.Named("not_modified_more", BooleanType.Default),
        ]
        :
        [
            TableArgFields.Named("page_size", Int64Type.Default),
            TableArgFields.Named("cache_ttl", Int64Type.Default),
        ];

    /// <summary>The bind-time checks every paged fixture shares.</summary>
    public static void ValidateBind(TableArguments arguments, long defaultPageSize)
    {
        var pageSize = arguments.Int64Named("page_size", defaultPageSize);
        if (pageSize < 1)
        {
            throw new ArgumentException($"page_size must be at least 1, got {pageSize}");
        }

        var cacheTtl = arguments.Int64Named("cache_ttl", 0);
        if (cacheTtl < 0)
        {
            throw new ArgumentException($"cache_ttl must not be negative, got {cacheTtl}");
        }
    }

    /// <summary>An input column's fan-out counts, NULL or negative counting as 0.</summary>
    public static long[] Counts(IArrowArray? column)
    {
        if (column is null)
        {
            return [];
        }

        var counts = new long[column.Length];
        for (var r = 0; r < counts.Length; r++)
        {
            counts[r] = ScalarArgCodec.ReadScalar(column, r) is { } v ? Math.Max(0, Convert.ToInt64(v)) : 0;
        }

        return counts;
    }

    /// <summary><c>vgi_rpc.parent_row#b64</c> for a non-empty reply: base64 of the raw little-endian
    /// int32 input-row index of each output row.</summary>
    public static Dictionary<string, string> ParentRows(List<int> parentRows, Dictionary<string, string>? metadata)
    {
        var merged = metadata ?? [];
        if (parentRows.Count > 0)
        {
            var bytes = new byte[parentRows.Count * sizeof(int)];
            for (var i = 0; i < parentRows.Count; i++)
            {
                BitConverter.TryWriteBytes(bytes.AsSpan(i * sizeof(int), sizeof(int)), parentRows[i]);
            }

            merged["vgi_rpc.parent_row#b64"] = Convert.ToBase64String(bytes);
        }

        return merged;
    }
}

/// <summary>Where the next reply resumes: the input's fan-out counts and a position in them —
/// vgi-python's <c>_PagedState</c>, plus its <c>_paged_begin</c>/<c>_paged_next</c>/<c>_next_page</c>.</summary>
internal sealed class PagedCursor(TableArguments arguments, long defaultPageSize)
{
    private readonly long _pageSize = arguments.Int64Named("page_size", defaultPageSize);
    private readonly long _failOnPage = arguments.Int64Named("fail_on_page", 0);
    private readonly bool _revalidate = arguments.BoolNamed("revalidate", false);
    private readonly bool _notModifiedMore = arguments.BoolNamed("not_modified_more", false);

    private long[] _counts = [];
    private int _row;
    private long _offset;
    private long _page; // replies sent for the current input
    private bool _emptyFirst; // not_modified_more: the first reply is empty and says more
    private long _bump; // added to every output value (revalidate: the data changed); -1 = one reply

    public long CacheTtl { get; } = arguments.Int64Named("cache_ttl", 0);

    /// <summary>Starts a new input; returns its first reply's cache control.
    /// <para><c>revalidate</c> makes the output depend on the request alone. A plain request gets
    /// the whole output in one reply, cacheable but immediately stale (ttl 0, etag <c>v1</c>,
    /// revalidatable). A conditional request (<c>vgi.cache.if_none_match</c> on the input) is told
    /// the data changed: every value is 1000 larger, the etag is <c>v2</c>, and the output is paged.
    /// <c>not_modified_more</c> breaks the protocol on purpose: the conditional request is answered
    /// with an empty <c>not_modified</c> reply that also says more, then the changed pages.</para></summary>
    public Dictionary<string, string>? Begin(long[] counts, IReadOnlyDictionary<string, string>? inputMetadata, Dictionary<string, string>? cacheControl)
    {
        _counts = counts;
        _row = 0;
        _offset = 0;
        _page = 0;
        _bump = 0;
        _emptyFirst = false;
        if (!_revalidate)
        {
            return cacheControl;
        }

        if (inputMetadata is null || !inputMetadata.ContainsKey("vgi.cache.if_none_match"))
        {
            _bump = -1;
            return CacheMetadata.Revalidatable("v1");
        }

        _bump = 1000;
        if (_notModifiedMore)
        {
            _emptyFirst = true;
            var notModified = CacheMetadata.Revalidatable("v1");
            notModified["vgi.cache.not_modified"] = "1";
            return notModified;
        }

        return CacheMetadata.Revalidatable("v2");
    }

    /// <summary>The next reply's values and parent rows, and whether more follow — or the failure
    /// <c>fail_on_page</c> asks for (replies counted per input batch, from 1).</summary>
    public (List<long> Values, List<int> ParentRows, bool More) Next()
    {
        _page++;
        if (_page == _failOnPage)
        {
            throw new InvalidOperationException($"failing on page {_page} as asked");
        }

        if (_page == 1 && _emptyFirst)
        {
            return ([], [], true);
        }

        if (_bump < 0)
        {
            return NextPage(long.MaxValue);
        }

        var (values, parentRows, more) = NextPage(_pageSize);
        for (var i = 0; i < values.Count; i++)
        {
            values[i] += _bump;
        }

        return (values, parentRows, more);
    }

    /// <summary>The next <paramref name="pageSize"/> rows (<c>0..n-1</c> per input row in order, so
    /// a page may span input rows), and whether more follow. Input rows with no output after the
    /// last non-empty page are skipped, so the last page is never followed by an empty one.</summary>
    private (List<long> Values, List<int> ParentRows, bool More) NextPage(long pageSize)
    {
        var values = new List<long>();
        var parentRows = new List<int>();
        while (_row < _counts.Length && values.Count < pageSize)
        {
            var take = Math.Min(_counts[_row] - _offset, pageSize - values.Count);
            for (var k = 0L; k < take; k++)
            {
                values.Add(_offset + k);
                parentRows.Add(_row);
            }

            _offset += take;
            if (_offset == _counts[_row])
            {
                _row++;
                _offset = 0;
            }
        }

        while (_row < _counts.Length && _counts[_row] == 0)
        {
            _row++;
        }

        return (values, parentRows, _row < _counts.Length);
    }
}

/// <summary><c>ex.blended_paged(n, page_size := 2, cache_ttl := 0, fail_on_page := 0,
/// revalidate := false, not_modified_more := false)</c> — <see cref="BlendedExplodeFunction"/>'s
/// output (rows <c>0..n-1</c> per input row, with parent-row provenance) returned
/// <c>page_size</c> rows per reply: <see cref="ITableInOutProcessor.Process"/> emits the first with
/// <c>more: true</c> and <see cref="ITableInOutProcessor.Resume"/> the rest. <c>cache_ttl &gt; 0</c>
/// makes each input's first reply cacheable. Backs <c>table_in_out/lateral_continuation.test</c>.</summary>
public sealed class BlendedPagedFunction : ITableInOutFunction
{
    public string Name => "blended_paged";

    public string Description => "Blended 1->N fan-out (emit 0..n-1 per input row), page_size rows per reply";

    public IReadOnlyList<string> Categories => ["blended", "test"];

    public bool InputFromArgs => true;

    public bool ContinuesOutput => true;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("n", Int64Type.Default), .. Paged.NamedArgs(full: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("i", Int64Type.Default, nullable: true)], metadata: null);

    public void Bind(TableInOutBindParams bindParams) => Paged.ValidateBind(bindParams.Arguments, 2);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(new PagedCursor(initParams.Arguments, 2), initParams.OutputSchema);

    private sealed class Processor(PagedCursor cursor, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var ttl = cursor.CacheTtl;
            var cacheControl = cursor.Begin(Paged.Counts(input.Column(0)), output.InputMetadata, ttl > 0 ? CacheMetadata.Ttl(ttl) : null);
            EmitPage(output, cacheControl);
        }

        public void Resume(OutputCollector output) => EmitPage(output, null);

        private void EmitPage(OutputCollector output, Dictionary<string, string>? cacheControl)
        {
            var (values, parentRows, more) = cursor.Next();
            var builder = new Int64Array.Builder();
            builder.AppendRange(values);
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], values.Count), Paged.ParentRows(parentRows, cacheControl), more);
        }
    }
}

/// <summary><c>ex.blended_sliced(n, page_size := 1000000, cache_ttl := 0)</c> —
/// <see cref="BlendedPagedFunction"/> declaring <see cref="ITableInOutFunction.InputRowsPerCall"/>
/// = 3, so the client must split every input chunk into calls of at most 3 rows; it fails a call
/// given more. Each output row carries <c>call_rows</c>, the row count of the call that produced it.
/// <c>cache_ttl &gt; 0</c> advertises per-value caching on each call's first reply. Backs
/// <c>table_in_out/input_rows_per_call.test</c>.</summary>
public sealed class BlendedSlicedFunction : ITableInOutFunction
{
    public string Name => "blended_sliced";

    public string Description => "Blended 1->N fan-out taking at most 3 input rows per call, reporting each call's size";

    public IReadOnlyList<string> Categories => ["blended", "test"];

    public bool InputFromArgs => true;

    public bool ContinuesOutput => true;

    public long? InputRowsPerCall => 3;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("n", Int64Type.Default), .. Paged.NamedArgs(full: false)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("i", Int64Type.Default, nullable: true), new Field("call_rows", Int64Type.Default, nullable: true)],
        metadata: null);

    public void Bind(TableInOutBindParams bindParams) => Paged.ValidateBind(bindParams.Arguments, 1_000_000);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(new PagedCursor(initParams.Arguments, 1_000_000), initParams.OutputSchema);

    private sealed class Processor(PagedCursor cursor, Schema outputSchema) : ITableInOutProcessor
    {
        private long _callRows;

        public void Process(RecordBatch input, OutputCollector output)
        {
            if (input.Length > 3)
            {
                throw new InvalidOperationException($"blended_sliced declares input_rows_per_call = 3 but got {input.Length} rows");
            }

            _callRows = input.Length;
            var ttl = cursor.CacheTtl;
            cursor.Begin(Paged.Counts(input.Column(0)), inputMetadata: null, cacheControl: null);
            EmitPage(output, ttl > 0 ? CacheMetadata.PerValue(ttl) : null);
        }

        public void Resume(OutputCollector output) => EmitPage(output, null);

        private void EmitPage(OutputCollector output, Dictionary<string, string>? cacheControl)
        {
            var (values, parentRows, more) = cursor.Next();
            var i = new Int64Array.Builder();
            i.AppendRange(values);
            var callRows = new Int64Array.Builder();
            for (var k = 0; k < values.Count; k++)
            {
                callRows.Append(_callRows);
            }

            output.Emit(
                new RecordBatch(outputSchema, [i.Build(), callRows.Build()], values.Count),
                Paged.ParentRows(parentRows, cacheControl),
                more);
        }
    }
}

/// <summary><c>ex.paged_explode(data TABLE, page_size := 2, cache_ttl := 0, fail_on_page := 0,
/// revalidate := false, not_modified_more := false)</c> — the classic (TABLE-input) counterpart of
/// <see cref="BlendedPagedFunction"/>: for each input row with count <c>n</c> (the table's first
/// column), emits <c>i = 0..n-1</c>, <c>page_size</c> per reply, through the extension's stock
/// in-out path on a per-substream connection. Backs <c>table_in_out/continuation_classic.test</c>.</summary>
public sealed class PagedExplodeFunction : ITableInOutFunction
{
    public string Name => "paged_explode";

    public string Description => "Classic table-in-out 1->N fan-out, page_size rows per reply";

    public IReadOnlyList<string> Categories => ["table_in_out", "test"];

    public bool ContinuesOutput => true;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Table("data"), .. Paged.NamedArgs(full: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("i", Int64Type.Default, nullable: true)], metadata: null);

    public void Bind(TableInOutBindParams bindParams) => Paged.ValidateBind(bindParams.Arguments, 2);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(new PagedCursor(initParams.Arguments, 2), initParams.OutputSchema);

    private sealed class Processor(PagedCursor cursor, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var ttl = cursor.CacheTtl;
            var counts = Paged.Counts(input.ColumnCount > 0 ? input.Column(0) : null);
            EmitPage(output, cursor.Begin(counts, output.InputMetadata, ttl > 0 ? CacheMetadata.Ttl(ttl) : null));
        }

        public void Resume(OutputCollector output) => EmitPage(output, null);

        private void EmitPage(OutputCollector output, Dictionary<string, string>? cacheControl)
        {
            var (values, _, more) = cursor.Next();
            var builder = new Int64Array.Builder();
            builder.AppendRange(values);
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], values.Count), cacheControl, more);
        }
    }
}
