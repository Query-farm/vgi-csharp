using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using QueryFarm.Vgi.ExampleWorker.TableInOut;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Client;
using QueryFarm.VgiRpc.Client.Http;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Reflection;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Transport;
using QueryFarm.VgiRpc.Wire;
using Xunit;

namespace QueryFarm.Vgi.Tests;

/// <summary>Continued table-in-out output end to end, through a real client over HTTP (whose
/// stream turns are separate requests against server-held state) and over an AF_UNIX socket (the
/// pipe framing the launcher uses): a reply saying more is continued by ticks, an error raised
/// while output is pending ends that stream and leaves the connection serving, and a turn that
/// emits nothing is answered with an error rather than no reply.</summary>
[Collection("environment")]
public sealed class ContinuedOutputTransportTests
{
    private const string CatalogName = "test_catalog";

    private static readonly Schema s_input = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    private static readonly Schema s_requestParams = new([new Field("request", BinaryType.Default, nullable: false)], metadata: null);

    /// <summary>A blended function whose Process emits nothing.</summary>
    private sealed class SilentFunction : ITableInOutFunction
    {
        public string Name => "silent";

        public bool InputFromArgs => true;

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("i", Int64Type.Default, nullable: true)], metadata: null);

        public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor();

        private sealed class Processor : ITableInOutProcessor
        {
            public void Process(RecordBatch input, OutputCollector output)
            {
            }
        }
    }

    private static Worker NewWorker() => new Worker().CatalogName(CatalogName).DefaultSchema("main")
        .RegisterTableInOut(new BlendedPagedFunction())
        .RegisterTableInOut(new SilentFunction());

    public interface IAttachClient
    {
        Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request);
    }

    private static byte[] NamedArgs(params (string Name, long Value)[] named)
    {
        var fields = named.Select(n => new Field(TableArgCodec.NamedPrefix + n.Name, Int64Type.Default, nullable: true)).ToList();
        var children = named.Select(n => (IArrowArray)new Int64Array.Builder().Append(n.Value).Build()).ToList();
        var args = new StructArray(new StructType(fields), 1, children, ArrowBuffer.Empty, nullCount: 0);
        var schema = new Schema([new Field("args", args.Data.DataType, nullable: false)], metadata: null);
        return RecordBatchIpc.Write(new RecordBatch(schema, [args], 1));
    }

    private static RecordBatch Counts(params long[] counts)
    {
        var builder = new Int64Array.Builder();
        builder.AppendRange(counts);
        return new RecordBatch(s_input, [builder.Build()], counts.Length);
    }

    private static readonly Dictionary<string, string> s_tick = new() { [ContinuedOutput.StatusKey] = ContinuedOutput.Continue };

    private static RecordBatch TickBatch() => new(s_input, [new Int64Array.Builder().Build()], 0);

    private static bool SaysMore(AnnotatedBatch reply) =>
        reply.GetMetadata(ContinuedOutput.StatusKey) == ContinuedOutput.HaveMoreOutput;

    private static List<long> Values(AnnotatedBatch reply) => ((Int64Array)reply.Batch.Column(0)).Values.ToArray().ToList();

    /// <summary>Opens a table-in-out INPUT exchange for <paramref name="function"/>.</summary>
    private static async Task<IRpcExchangeSession> OpenAsync(IRpcClient client, byte[] attach, string function, byte[] arguments)
    {
        var bind = new BindRequest
        {
            FunctionName = function,
            FunctionType = FunctionType.Table,
            Arguments = arguments,
            InputSchema = SchemaIpc.WriteSchemaOnly(s_input),
            AttachOpaqueData = attach,
        };
        var init = new InitRequest
        {
            BindCall = EmbeddedIpc.Encode(bind),
            Phase = VgiInitPhase.Input,
            ExecutionId = Guid.NewGuid().ToByteArray(),
        };
        using var parameters = ValueCodec.BuildRow(s_requestParams, [init]);
        return await client.OpenExchangeAsync("init", parameters, hasHeader: true);
    }

    /// <summary>The scenario every transport must pass.</summary>
    private static async Task RunScenarioAsync(IRpcClient client, byte[] attach)
    {
        var timeout = TimeSpan.FromSeconds(30);

        // 1. Continued output: page_size 2 over counts [3, 2] is three replies, then real input again.
        await using (var paged = await OpenAsync(client, attach, "blended_paged", NamedArgs(("page_size", 2))))
        {
            var replies = new List<AnnotatedBatch> { (await paged.ExchangeAsync(Counts(3, 2)).WaitAsync(timeout))! };
            while (SaysMore(replies[^1]))
            {
                replies.Add((await paged.ExchangeAsync(TickBatch(), s_tick).WaitAsync(timeout))!);
                Assert.True(replies.Count < 10);
            }

            Assert.Equal([[0L, 1L], [2L, 0L], [1L]], replies.Select(Values));
            Assert.Equal([true, true, false], replies.Select(SaysMore));

            var next = (await paged.ExchangeAsync(Counts(1)).WaitAsync(timeout))!;
            Assert.Equal([0L], Values(next));
            Assert.False(SaysMore(next));
        }

        // 2. An error while output is pending ends the stream with that error.
        await using (var failing = await OpenAsync(client, attach, "blended_paged", NamedArgs(("page_size", 1), ("fail_on_page", 2))))
        {
            var first = (await failing.ExchangeAsync(Counts(3)).WaitAsync(timeout))!;
            Assert.Equal([0L], Values(first));
            Assert.True(SaysMore(first));

            var error = await Assert.ThrowsAnyAsync<RpcException>(() => failing.ExchangeAsync(TickBatch(), s_tick).WaitAsync(timeout));
            Assert.Contains("failing on page 2 as asked", error.ErrorMessage, StringComparison.Ordinal);
        }

        // 3. A turn that emits nothing is an error reply, not a missing one (a pipe client would
        // otherwise wait forever).
        await using (var silent = await OpenAsync(client, attach, "silent", []))
        {
            var error = await Assert.ThrowsAnyAsync<RpcException>(() => silent.ExchangeAsync(Counts(1)).WaitAsync(timeout));
            Assert.Contains("No data batch was emitted", error.ErrorMessage, StringComparison.Ordinal);
        }

        // 4. The connection still serves: a fresh continued stream gives correct results.
        await using (var again = await OpenAsync(client, attach, "blended_paged", NamedArgs(("page_size", 5))))
        {
            var reply = (await again.ExchangeAsync(Counts(2, 1)).WaitAsync(timeout))!;
            Assert.Equal([0L, 1L, 0L], Values(reply));
            Assert.False(SaysMore(reply));
        }
    }

    [Fact]
    public async Task ContinuesOverHttp()
    {
        var builder = WebApplication.CreateSlimBuilder(Worker.WorkerHostOptions());
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        await using var app = builder.Build();
        NewWorker().MapHttp(app);
        await app.StartAsync(TestContext.Current.CancellationToken);
        var address = new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single());

        await using var client = new HttpRpcClient(address, new HttpRpcClientOptions
        {
            Protocol = VgiProtocol.Name,
            ProtocolVersion = Worker.DefaultProtocolVersion,
        });
        var attach = await client.CreateProxy<IAttachClient>().CatalogAttachAsync(new CatalogAttachRequest { Name = CatalogName });
        await RunScenarioAsync(client, attach.AttachOpaqueData ?? []);
    }

    [Fact]
    public async Task ContinuesOverAUnixSocket()
    {
        var path = Path.Combine(Path.GetTempPath(), $"vgi-csharp-test-{Guid.NewGuid():N}.sock");
        using var cts = new CancellationTokenSource();
        var serveTask = NewWorker().RunUnixSocketAsync(path, idleTimeoutSeconds: 60, cts.Token);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!File.Exists(path) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            var transport = await SocketTransport.ConnectUnixAsync(path);
            await using var client = new RpcClient(transport, new RpcClientOptions
            {
                Protocol = VgiProtocol.Name,
                ProtocolVersion = Worker.DefaultProtocolVersion,
            });
            var attach = await client.CreateProxy<IAttachClient>().CatalogAttachAsync(new CatalogAttachRequest { Name = CatalogName });
            await RunScenarioAsync(client, attach.AttachOpaqueData ?? []);
        }
        finally
        {
            cts.Cancel();
            await serveTask;
        }
    }
}
