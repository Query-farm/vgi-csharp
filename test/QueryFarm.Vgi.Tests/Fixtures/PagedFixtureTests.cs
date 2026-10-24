using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.ExampleWorker.TableInOut;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;
using Xunit;

namespace QueryFarm.Vgi.Tests.Fixtures;

/// <summary>The continued-output fixtures (<c>blended_paged</c>, <c>paged_explode</c>) against the
/// "Fixture contract" headers of the extension's
/// <c>table_in_out/{lateral_continuation,continuation_classic,continuation_resume_error_revalidate}.test</c>,
/// driven through the real exchange state so every reply is checked by the same rules the worker
/// applies on the wire.</summary>
public class PagedFixtureTests
{
    private static readonly Schema s_input = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    private const string Status = ContinuedOutput.StatusKey;

    public static TheoryData<string> Functions => new() { "blended_paged", "paged_explode" };

    private static ITableInOutFunction Function(string name) => name switch
    {
        "blended_paged" => new BlendedPagedFunction(),
        "paged_explode" => new PagedExplodeFunction(),
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    private sealed record Reply(List<long> Values, List<int>? ParentRows, IReadOnlyDictionary<string, string> Metadata)
    {
        public bool More => Metadata.TryGetValue(Status, out var s) && s == ContinuedOutput.HaveMoreOutput;

        public IEnumerable<string> CacheKeys => Metadata.Keys.Where(k => k.StartsWith("vgi.cache.", StringComparison.Ordinal)).Order(StringComparer.Ordinal);
    }

    private sealed class Exchange
    {
        private readonly TableInOutExchangeStreamState _state;
        private readonly Schema _output;

        public Exchange(string name, long pageSize = 2, long cacheTtl = 0, long failOnPage = 0, bool revalidate = false, bool notModifiedMore = false)
        {
            var function = Function(name);
            var named = new Dictionary<string, IArrowArray>
            {
                ["page_size"] = new Int64Array.Builder().Append(pageSize).Build(),
                ["cache_ttl"] = new Int64Array.Builder().Append(cacheTtl).Build(),
                ["fail_on_page"] = new Int64Array.Builder().Append(failOnPage).Build(),
                ["revalidate"] = new BooleanArray.Builder().Append(revalidate).Build(),
                ["not_modified_more"] = new BooleanArray.Builder().Append(notModifiedMore).Build(),
            };
            _output = function.OutputSchema;
            var processor = function.CreateProcessor(new TableInOutInitParams
            {
                FunctionName = function.Name,
                Arguments = new TableArguments([], named),
                InputSchema = s_input,
                OutputSchema = _output,
            });
            _state = new TableInOutExchangeStreamState(function, processor);
        }

        public Reply InputWith(IReadOnlyDictionary<string, string>? metadata, params long?[] counts)
        {
            var builder = new Int64Array.Builder();
            foreach (var count in counts)
            {
                builder.Append(count);
            }

            return Turn(new AnnotatedBatch(new RecordBatch(s_input, [builder.Build()], counts.Length), metadata));
        }

        public Reply Input(params long?[] counts) => InputWith(null, counts);

        public Reply Tick() => Turn(new AnnotatedBatch(
            new RecordBatch(s_input, [new Int64Array.Builder().Build()], 0),
            new Dictionary<string, string> { [Status] = ContinuedOutput.Continue }));

        /// <summary>Ticks until a reply says no more; returns every reply, the first included.</summary>
        public List<Reply> Drain(Reply first)
        {
            var replies = new List<Reply> { first };
            while (replies[^1].More)
            {
                replies.Add(Tick());
                Assert.True(replies.Count < 1000, "the fixture never stopped saying more");
            }

            return replies;
        }

        private Reply Turn(AnnotatedBatch input)
        {
            using var output = new OutputCollector(_output);
            _state.ExchangeAsync(input, output, null, CancellationToken.None).GetAwaiter().GetResult();
            var batch = Assert.IsType<RecordBatch>(output.EmittedBatch);
            var values = ((Int64Array)batch.Column("i")).Values.ToArray().ToList();
            var metadata = output.EmittedMetadata ?? new Dictionary<string, string>();
            List<int>? parentRows = null;
            if (metadata.TryGetValue("vgi_rpc.parent_row#b64", out var b64))
            {
                var bytes = Convert.FromBase64String(b64);
                parentRows = [.. Enumerable.Range(0, bytes.Length / sizeof(int)).Select(i => BitConverter.ToInt32(bytes, i * sizeof(int)))];
            }

            return new Reply(values, parentRows, metadata);
        }
    }

    private static readonly Dictionary<string, string> s_conditional = new() { ["vgi.cache.if_none_match"] = "v1" };

    [Theory]
    [MemberData(nameof(Functions))]
    public void OutputIsCutIntoPageSizeRepliesThatMaySpanInputRows(string name)
    {
        var exchange = new Exchange(name, pageSize: 2);
        var replies = exchange.Drain(exchange.Input(3, 2));

        Assert.Equal([[0L, 1L], [2L, 0L], [1L]], replies.Select(r => r.Values));
        Assert.Equal([true, true, false], replies.Select(r => r.More));
        if (name == "blended_paged")
        {
            Assert.Equal([[0, 0], [0, 1], [1]], replies.Select(r => r.ParentRows!));
        }

        // No cache_ttl: no cache control on any reply.
        Assert.All(replies, r => Assert.Empty(r.CacheKeys));
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void NullAndNegativeCountsProduceNothing(string name)
    {
        var exchange = new Exchange(name, pageSize: 10);
        var reply = Assert.Single(exchange.Drain(exchange.Input(null, 2, -4, 1)));
        Assert.Equal([0L, 1L, 0L], reply.Values);
        if (name == "blended_paged")
        {
            Assert.Equal([1, 1, 3], reply.ParentRows!);
        }
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void ABatchWithNoOutputIsOneEmptyReply(string name)
    {
        var exchange = new Exchange(name);
        var reply = Assert.Single(exchange.Drain(exchange.Input(0, null, 0)));
        Assert.Empty(reply.Values);
        Assert.False(reply.More);
        Assert.Null(reply.ParentRows);
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void TrailingZeroCountRowsAddNoReply(string name)
    {
        // The last non-empty page ends exactly at row 0's end; rows 1 and 2 have no output, so
        // that page is the last and is not followed by an empty one.
        var exchange = new Exchange(name, pageSize: 2);
        var replies = exchange.Drain(exchange.Input(4, 0, 0));
        Assert.Equal([[0L, 1L], [2L, 3L]], replies.Select(r => r.Values));
        Assert.Equal([true, false], replies.Select(r => r.More));
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void CacheTtlRidesOnlyEachInputsFirstReply(string name)
    {
        var exchange = new Exchange(name, pageSize: 1, cacheTtl: 30);
        var replies = exchange.Drain(exchange.Input(3));
        Assert.Equal("30", replies[0].Metadata["vgi.cache.ttl"]);
        Assert.All(replies.Skip(1), r => Assert.Empty(r.CacheKeys));

        // A new input batch gets it again, on its own first reply.
        var next = exchange.Drain(exchange.Input(2));
        Assert.Equal("30", next[0].Metadata["vgi.cache.ttl"]);
        Assert.All(next.Skip(1), r => Assert.Empty(r.CacheKeys));
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void FailOnPageFailsInsteadOfSendingThatReply_CountedPerInputBatch(string name)
    {
        var exchange = new Exchange(name, pageSize: 1, failOnPage: 3);
        var first = exchange.Input(5);
        Assert.Equal([0L], first.Values);
        Assert.Equal([1L], exchange.Tick().Values);
        var error = Assert.Throws<InvalidOperationException>(exchange.Tick);
        Assert.Equal("failing on page 3 as asked", error.Message);

        // Pages are counted per input batch, not per input row: a batch whose output fits in two
        // replies never fails.
        var fresh = new Exchange(name, pageSize: 1, failOnPage: 3);
        Assert.Equal([[0L], [0L]], fresh.Drain(fresh.Input(1, 1)).Select(r => r.Values));
        Assert.Equal([[0L], [0L]], fresh.Drain(fresh.Input(1, 1)).Select(r => r.Values));
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void FailOnTheFirstPageFailsTheProcessCall(string name)
    {
        var exchange = new Exchange(name, failOnPage: 1);
        var error = Assert.Throws<InvalidOperationException>(() => exchange.Input(2));
        Assert.Equal("failing on page 1 as asked", error.Message);
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void RevalidateWithoutAConditionalRequest_IsOneV1ReplyIgnoringCacheTtl(string name)
    {
        var exchange = new Exchange(name, pageSize: 1, cacheTtl: 99, revalidate: true);
        var reply = Assert.Single(exchange.Drain(exchange.Input(3, 2)));
        Assert.Equal([0L, 1L, 2L, 0L, 1L], reply.Values);
        Assert.Equal(["vgi.cache.etag", "vgi.cache.revalidatable", "vgi.cache.ttl"], reply.CacheKeys);
        Assert.Equal(("0", "v1", "1"), (reply.Metadata["vgi.cache.ttl"], reply.Metadata["vgi.cache.etag"], reply.Metadata["vgi.cache.revalidatable"]));
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void RevalidateWithAConditionalRequest_IsChangedV2OutputInPages(string name)
    {
        var exchange = new Exchange(name, pageSize: 2, revalidate: true);
        var replies = exchange.Drain(exchange.InputWith(s_conditional, 3));
        Assert.Equal([[1000L, 1001L], [1002L]], replies.Select(r => r.Values));
        Assert.Equal(("0", "v2", "1"), (replies[0].Metadata["vgi.cache.ttl"], replies[0].Metadata["vgi.cache.etag"], replies[0].Metadata["vgi.cache.revalidatable"]));
        Assert.False(replies[0].Metadata.ContainsKey("vgi.cache.not_modified"));
        Assert.Empty(replies[1].CacheKeys);

        // The validators belong to one input: the next plain input is a v1 request again.
        var plain = Assert.Single(exchange.Drain(exchange.Input(2)));
        Assert.Equal([0L, 1L], plain.Values);
        Assert.Equal("v1", plain.Metadata["vgi.cache.etag"]);
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void NotModifiedMore_AnswersAConditionalRequestWithAnEmptyNotModifiedReplyThatSaysMore(string name)
    {
        var exchange = new Exchange(name, pageSize: 2, revalidate: true, notModifiedMore: true);
        var replies = exchange.Drain(exchange.InputWith(s_conditional, 3));

        Assert.Empty(replies[0].Values);
        Assert.True(replies[0].More);
        Assert.Equal(("1", "0", "v1", "1"), (
            replies[0].Metadata["vgi.cache.not_modified"], replies[0].Metadata["vgi.cache.ttl"],
            replies[0].Metadata["vgi.cache.etag"], replies[0].Metadata["vgi.cache.revalidatable"]));
        Assert.Equal([[1000L, 1001L], [1002L]], replies.Skip(1).Select(r => r.Values));
        Assert.All(replies.Skip(1), r => Assert.Empty(r.CacheKeys));
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void NotModifiedMore_OnlyAppliesToAConditionalRequest(string name)
    {
        var exchange = new Exchange(name, revalidate: true, notModifiedMore: true);
        var reply = Assert.Single(exchange.Drain(exchange.Input(2)));
        Assert.Equal([0L, 1L], reply.Values);
        Assert.Equal("v1", reply.Metadata["vgi.cache.etag"]);
        Assert.False(reply.Metadata.ContainsKey("vgi.cache.not_modified"));
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void NotModifiedMoresEmptyFirstReplyCountsTowardFailOnPage(string name)
    {
        var exchange = new Exchange(name, pageSize: 2, failOnPage: 2, revalidate: true, notModifiedMore: true);
        Assert.Empty(exchange.InputWith(s_conditional, 3).Values);
        var error = Assert.Throws<InvalidOperationException>(exchange.Tick);
        Assert.Equal("failing on page 2 as asked", error.Message);
    }

    [Theory]
    [MemberData(nameof(Functions))]
    public void BindRefusesABadPageSizeOrCacheTtl(string name)
    {
        var function = Function(name);
        void Bind(string arg, long value) => function.Bind(new TableInOutBindParams
        {
            FunctionName = name,
            ArgumentsBytes = [],
            Arguments = new TableArguments([], new Dictionary<string, IArrowArray> { [arg] = new Int64Array.Builder().Append(value).Build() }),
            Settings = null,
            Secrets = new SecretsAccessor(null, false),
            InputSchema = s_input,
        });

        Assert.Equal("page_size must be at least 1, got 0", Assert.Throws<ArgumentException>(() => Bind("page_size", 0)).Message);
        Assert.Equal("cache_ttl must not be negative, got -1", Assert.Throws<ArgumentException>(() => Bind("cache_ttl", -1)).Message);
    }
}
