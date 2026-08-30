using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Regression tests: calling a function via the wrong RPC method shape.
///
/// Found live against a real deployed worker: calling a table-in-out function via the plain
/// producer path (<c>table_function()</c> -- no input schema, no init phase) instead of the
/// exchange path (<c>table_in_out_function(input=...)</c>), or vice versa, used to produce a
/// SILENT, NON-TERMINATING HANG rather than a clean error. Both sides were independently, locally
/// correct: the server only stops on the processor's own <c>Finish()</c> (never reached -- a
/// table-in-out processor is designed to consume input rows that never arrive when the wrong RPC
/// method is used), and the client only stops when the server stops sending a continuation token
/// (which never happens either, since the server-side handler for this function was never
/// designed to reach that state).
///
/// Root cause: <see cref="VgiServiceImpl"/> silently substituted an empty
/// <c>Apache.Arrow.Schema([], null)</c> when <c>BindRequest.InputSchema</c> was missing/null,
/// instead of treating "missing" as a red flag -- which disabled the later schema-conformance
/// check the exchange stream relies on to catch exactly this mismatch. These tests pin that both
/// directions of the confusion now fail immediately, with a message naming the function and the
/// actual fix -- not a hang, not a generic error.
/// </summary>
public class FunctionShapeDispatchGuardTests
{
    private static readonly Schema OutputSchema = new(
        [new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    private static VgiServiceImpl NewService(CatalogRegistry registry) => new(registry);

    private static async Task<byte[]> AttachAsync(VgiServiceImpl service)
    {
        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        return attach.AttachOpaqueData ?? [];
    }

    private static BindRequest MakePlainTableBindRequest(string name, byte[] attach, byte[]? inputSchema = null) => new()
    {
        FunctionName = name,
        FunctionType = FunctionType.Table,
        Arguments = [],
        InputSchema = inputSchema,
        AttachOpaqueData = attach,
    };

    private static BindRequest MakeTableInOutBindRequest(string name, byte[] attach, Schema? inputSchema) => new()
    {
        FunctionName = name,
        FunctionType = FunctionType.Table,
        Arguments = [],
        InputSchema = inputSchema is null ? null : SchemaIpc.WriteSchemaOnly(inputSchema),
        AttachOpaqueData = attach,
    };

    public class PlainTableFunctionCalledAsTableInOut
    {
        [Fact]
        public async Task BindAsync_RejectsAnInputSchema_InsteadOfSilentlyAccepting()
        {
            var registry = new CatalogRegistry();
            registry.RegisterTable(new StubTableFunction("sequence"));
            var service = NewService(registry);
            var attach = await AttachAsync(service);

            var stubInputSchema = SchemaIpc.WriteSchemaOnly(new Schema([new Field("x", Int64Type.Default, nullable: true)], metadata: null));
            var bindRequest = MakePlainTableBindRequest("sequence", attach, stubInputSchema);

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => service.BindAsync(bindRequest));
            Assert.Contains("sequence", exception.Message, StringComparison.Ordinal);
            Assert.Contains("table_function()", exception.Message, StringComparison.Ordinal);
        }

        [Fact]
        public async Task InitAsync_RejectsANonNullPhase_InsteadOfSilentlyRunningAsAProducer()
        {
            var registry = new CatalogRegistry();
            registry.RegisterTable(new StubTableFunction("sequence"));
            var service = NewService(registry);
            var attach = await AttachAsync(service);

            var bindRequest = MakePlainTableBindRequest("sequence", attach);
            var initRequest = new InitRequest
            {
                BindCall = EmbeddedIpc.Encode(bindRequest),
                Phase = VgiInitPhase.Input,
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => service.InitAsync(initRequest));
            Assert.Contains("sequence", exception.Message, StringComparison.Ordinal);
            Assert.Contains("table_function()", exception.Message, StringComparison.Ordinal);
        }

        [Fact]
        public async Task TheOrdinaryCall_WithNoInputSchemaAndNoPhase_StillWorks()
        {
            var registry = new CatalogRegistry();
            registry.RegisterTable(new StubTableFunction("sequence"));
            var service = NewService(registry);
            var attach = await AttachAsync(service);

            var bindResponse = await service.BindAsync(MakePlainTableBindRequest("sequence", attach));
            Assert.Equal(OutputSchema.FieldsList.Count, SchemaIpc.ReadSchemaOnly(bindResponse.OutputSchema).FieldsList.Count);

            var bindRequest = MakePlainTableBindRequest("sequence", attach);
            var initRequest = new InitRequest { BindCall = EmbeddedIpc.Encode(bindRequest) };
            var stream = await service.InitAsync(initRequest);
            Assert.Equal("n", Assert.Single(stream.OutputSchema.FieldsList).Name);
        }
    }

    public class TableInOutFunctionCalledAsPlainTable
    {
        private static readonly Schema StubInputSchema = new(
            [new Field("x", Int64Type.Default, nullable: true)], metadata: null);

        [Fact]
        public async Task BindAsync_RejectsAMissingInputSchema_InsteadOfSilentlySubstitutingEmpty()
        {
            var registry = new CatalogRegistry();
            registry.RegisterTableInOut(new StubTableInOutFunction("echo"));
            var service = NewService(registry);
            var attach = await AttachAsync(service);

            var bindRequest = MakeTableInOutBindRequest("echo", attach, inputSchema: null);

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => service.BindAsync(bindRequest));
            Assert.Contains("echo", exception.Message, StringComparison.Ordinal);
            Assert.Contains("table_in_out_function(input=...)", exception.Message, StringComparison.Ordinal);
        }

        [Fact]
        public async Task InitAsync_RejectsAMissingInputSchema_OnTheInputPhase()
        {
            var registry = new CatalogRegistry();
            registry.RegisterTableInOut(new StubTableInOutFunction("echo"));
            var service = NewService(registry);
            var attach = await AttachAsync(service);

            // No InputSchema at all -- exactly what a caller sends via table_function().
            var bindRequest = MakeTableInOutBindRequest("echo", attach, inputSchema: null);
            var initRequest = new InitRequest
            {
                BindCall = EmbeddedIpc.Encode(bindRequest),
                Phase = null,
            };

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => service.InitAsync(initRequest));
            Assert.Contains("echo", exception.Message, StringComparison.Ordinal);
            Assert.Contains("table_in_out_function(input=...)", exception.Message, StringComparison.Ordinal);
        }

        [Fact]
        public async Task TheOrdinaryExchangeCall_WithARealInputSchema_StillWorks()
        {
            var registry = new CatalogRegistry();
            registry.RegisterTableInOut(new StubTableInOutFunction("echo"));
            var service = NewService(registry);
            var attach = await AttachAsync(service);

            var bindRequest = MakeTableInOutBindRequest("echo", attach, StubInputSchema);
            var bindResponse = await service.BindAsync(bindRequest);
            Assert.Single(SchemaIpc.ReadSchemaOnly(bindResponse.OutputSchema).FieldsList);

            var initRequest = new InitRequest
            {
                BindCall = EmbeddedIpc.Encode(MakeTableInOutBindRequest("echo", attach, StubInputSchema)),
                Phase = VgiInitPhase.Input,
            };
            var stream = await service.InitAsync(initRequest);
            Assert.Equal("n", Assert.Single(stream.OutputSchema.FieldsList).Name);
        }

        [Fact]
        public async Task InitAsync_FinalizePhase_IsNotSubjectToTheInputSchemaGuard()
        {
            // FINALIZE reuses the SAME bind_call the INPUT phase already validated, on the same
            // connection -- it must not be independently rejected just because a test constructs
            // it with no input schema of its own (mirroring how the C++ extension actually drives
            // FINALIZE: same request.bind_call bytes as the preceding INPUT-phase init).
            var registry = new CatalogRegistry();
            var function = new StubTableInOutFunction("echo") { HasFinalizeOverride = true };
            registry.RegisterTableInOut(function);
            var service = NewService(registry);
            var attach = await AttachAsync(service);

            var executionId = Guid.NewGuid().ToByteArray();
            var bindRequest = MakeTableInOutBindRequest("echo", attach, StubInputSchema);

            var inputInitRequest = new InitRequest
            {
                BindCall = EmbeddedIpc.Encode(bindRequest),
                Phase = VgiInitPhase.Input,
                ExecutionId = executionId,
            };
            await service.InitAsync(inputInitRequest);

            var finalizeInitRequest = new InitRequest
            {
                BindCall = EmbeddedIpc.Encode(bindRequest),
                Phase = VgiInitPhase.Finalize,
                ExecutionId = executionId,
            };

            // Must not throw the shape-mismatch guard -- FINALIZE legitimately reuses the same
            // bind_call and is looked up by cached processor, not re-validated against InputSchema.
            var stream = await service.InitAsync(finalizeInitRequest);
            Assert.Equal("n", Assert.Single(stream.OutputSchema.FieldsList).Name);
        }
    }
}

file sealed class StubTableFunction(string name) : ITableFunction
{
    public string Name => name;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new NullProducer();

    private sealed class NullProducer : ITableFunctionProducer
    {
        public void Produce(OutputCollector output) => output.Finish();
    }
}

file sealed class StubTableInOutFunction(string name) : ITableInOutFunction
{
    public string Name => name;

    public bool HasFinalizeOverride { get; init; }

    public bool HasFinalize => HasFinalizeOverride;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new NullProcessor();

    private sealed class NullProcessor : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
        }

        public void Finalize(OutputCollector output) => output.Finish();
    }
}
