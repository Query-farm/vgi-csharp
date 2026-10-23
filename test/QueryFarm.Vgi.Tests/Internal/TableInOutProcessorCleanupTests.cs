using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>The INPUT-phase processor <see cref="VgiServiceImpl"/> keeps for a FINALIZE init is kept
/// only when a FINALIZE will come, and released when it will not: a function without a finalize
/// phase never leaves one behind, and one with a finalize phase drops it on FINALIZE, on a failed
/// turn, or on cancel — but keeps it across a normal end of input.</summary>
public class TableInOutProcessorCleanupTests
{
    private static readonly Schema s_input = new([new Field("x", Int64Type.Default, nullable: true)], metadata: null);
    private static readonly Schema s_output = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    /// <summary>Echoes <c>x</c>; a negative value fails the turn.</summary>
    private sealed class Function(bool hasFinalize) : ITableInOutFunction
    {
        public string Name => "cleanup_probe";

        public bool HasFinalize => hasFinalize;

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

        public Schema OutputSchema => s_output;

        public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor();

        private sealed class Processor : ITableInOutProcessor
        {
            public void Process(RecordBatch input, OutputCollector output)
            {
                var x = (Int64Array)input.Column(0);
                if (x.Length > 0 && x.GetValue(0) < 0)
                {
                    throw new InvalidOperationException("negative input");
                }

                output.Emit(new RecordBatch(s_output, [x], x.Length));
            }

            public void Finalize(OutputCollector output) => output.Finish();
        }
    }

    private static async Task<(VgiServiceImpl Service, byte[] BindCall)> OpenAsync(bool hasFinalize)
    {
        var registry = new CatalogRegistry();
        registry.RegisterTableInOut(new Function(hasFinalize));
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData ?? [];
        var bind = new BindRequest
        {
            FunctionName = "cleanup_probe",
            FunctionType = FunctionType.Table,
            Arguments = [],
            InputSchema = SchemaIpc.WriteSchemaOnly(s_input),
            AttachOpaqueData = attach,
        };
        return (service, EmbeddedIpc.Encode(bind));
    }

    private static Task<RpcStream<StreamState>> InitAsync(VgiServiceImpl service, byte[] bindCall, byte[] executionId, VgiInitPhase phase) =>
        service.InitAsync(new InitRequest { BindCall = bindCall, Phase = phase, ExecutionId = executionId });

    private static Task TurnAsync(RpcStream<StreamState> stream, long x)
    {
        using var output = new OutputCollector(s_output);
        var batch = new RecordBatch(s_input, [new Int64Array.Builder().Append(x).Build()], 1);
        return stream.State.ProcessAsync(new AnnotatedBatch(batch, null), output, null, CancellationToken.None);
    }

    [Fact]
    public async Task AFunctionWithoutFinalizeKeepsNoProcessor()
    {
        var (service, bindCall) = await OpenAsync(hasFinalize: false);
        for (var query = 0; query < 3; query++)
        {
            var stream = await InitAsync(service, bindCall, Guid.NewGuid().ToByteArray(), VgiInitPhase.Input);
            await TurnAsync(stream, query);
        }

        Assert.Equal(0, service.PendingFinalizeProcessorCount);

        // And a FINALIZE it never declared is refused rather than looked up.
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            InitAsync(service, bindCall, Guid.NewGuid().ToByteArray(), VgiInitPhase.Finalize));
        Assert.Contains("declares no finalize phase", error.Message);
    }

    [Fact]
    public async Task AFinalizeFunctionKeepsItsProcessorAcrossEndOfInputUntilFinalize()
    {
        var (service, bindCall) = await OpenAsync(hasFinalize: true);
        var executionId = Guid.NewGuid().ToByteArray();
        var stream = await InitAsync(service, bindCall, executionId, VgiInitPhase.Input);
        await TurnAsync(stream, 1);

        // End of input: nothing is told, and the processor waits for its FINALIZE.
        Assert.Equal(1, service.PendingFinalizeProcessorCount);

        await InitAsync(service, bindCall, executionId, VgiInitPhase.Finalize);
        Assert.Equal(0, service.PendingFinalizeProcessorCount);
    }

    [Fact]
    public async Task AFailedTurnReleasesTheProcessor()
    {
        var (service, bindCall) = await OpenAsync(hasFinalize: true);
        var stream = await InitAsync(service, bindCall, Guid.NewGuid().ToByteArray(), VgiInitPhase.Input);
        await TurnAsync(stream, 1);
        Assert.Equal(1, service.PendingFinalizeProcessorCount);

        await Assert.ThrowsAsync<InvalidOperationException>(() => TurnAsync(stream, -1));
        Assert.Equal(0, service.PendingFinalizeProcessorCount);
    }

    [Fact]
    public async Task ACancelReleasesTheProcessor()
    {
        var (service, bindCall) = await OpenAsync(hasFinalize: true);
        var stream = await InitAsync(service, bindCall, Guid.NewGuid().ToByteArray(), VgiInitPhase.Input);
        Assert.Equal(1, service.PendingFinalizeProcessorCount);

        stream.State.OnCancel(null);
        Assert.Equal(0, service.PendingFinalizeProcessorCount);
    }

    [Fact]
    public async Task ReleasingOneSubstreamLeavesAnotherSubstreamsProcessor()
    {
        var (service, bindCall) = await OpenAsync(hasFinalize: true);
        var failing = await InitAsync(service, bindCall, Guid.NewGuid().ToByteArray(), VgiInitPhase.Input);
        var other = await InitAsync(service, bindCall, Guid.NewGuid().ToByteArray(), VgiInitPhase.Input);
        Assert.Equal(2, service.PendingFinalizeProcessorCount);

        await Assert.ThrowsAsync<InvalidOperationException>(() => TurnAsync(failing, -1));
        Assert.Equal(1, service.PendingFinalizeProcessorCount);
        await TurnAsync(other, 1);
        Assert.Equal(1, service.PendingFinalizeProcessorCount);
    }
}
