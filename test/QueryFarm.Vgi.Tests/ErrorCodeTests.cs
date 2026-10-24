using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.ExampleWorker.Scalar;
using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Client;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Transport;
using Xunit;

namespace QueryFarm.Vgi.Tests;

/// <summary>
/// The SDK's own errors carry a canonical code (vgi-rpc WIRE_PROTOCOL.md §8), decoded by a real
/// vgi-rpc client over a real socket: wrong input is <c>INVALID_ARGUMENT</c>, an unknown object is
/// <c>NOT_FOUND</c>, a write against a read-only catalog is <c>FAILED_PRECONDITION</c>. The first
/// two cases are the ones <c>unary_error_propagation.test</c> drives through the DuckDB extension
/// against every SDK's example worker.
/// </summary>
public sealed class ErrorCodeTests
{
    private static Worker NewWorker() => new Worker()
        .CatalogName("example")
        .DefaultSchema("main")
        .RegisterScalar(new DoubleFunction())
        .RegisterTable(new SequenceFunction());

    /// <summary>Serves <see cref="NewWorker"/> over AF_UNIX and runs <paramref name="call"/>
    /// against a client proxy, returning the error it raised.</summary>
    private static async Task<RpcException> CallFailingAsync(Func<IVgiService, Task> call)
    {
        var path = Path.Combine(Path.GetTempPath(), $"vgi-csharp-codes-{Guid.NewGuid():N}.sock");
        using var cts = new CancellationTokenSource();
        var serveTask = NewWorker().RunUnixSocketAsync(path, idleTimeoutSeconds: 30, cts.Token);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!File.Exists(path) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            using var transport = (SocketTransport)await SocketTransport.ConnectUnixAsync(path);
            var connection = new RpcConnection<IVgiService>(
                transport, new RpcClientOptions { ProtocolVersion = Worker.DefaultProtocolVersion });
            return await Assert.ThrowsAnyAsync<RpcException>(() => call(connection.CreateProxy()));
        }
        finally
        {
            cts.Cancel();
            await serveTask;
        }
    }

    private static Int64Array Int64(long value) => new Int64Array.Builder().Append(value).Build();

    [Fact]
    public async Task ScalarTypeRejection_IsInvalidArgument()
    {
        // SELECT example.main.double('abc');
        var inputSchema = new Schema([new Field("value", StringType.Default, nullable: true)], metadata: null);
        var error = await CallFailingAsync(client => client.BindAsync(new BindRequest
        {
            FunctionName = "double",
            FunctionType = FunctionType.Scalar,
            InputSchema = SchemaIpc.WriteSchemaOnly(inputSchema),
        }));

        Assert.Equal(ErrorCodes.InvalidArgument, error.ErrorCode);
        Assert.Contains("rejects type", error.ErrorMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TableArgumentConstraint_IsInvalidArgument()
    {
        // SELECT * FROM example.main.sequence(10, batch_size := 0);
        var argsType = new StructType(
        [
            new Field(TableArgCodec.PositionalPrefix + "0", Int64Type.Default, nullable: true),
            new Field(TableArgCodec.NamedPrefix + "batch_size", Int64Type.Default, nullable: true),
        ]);
        var args = new RecordBatch(
            new Schema([new Field("args", argsType, nullable: false)], metadata: null),
            [new StructArray(argsType, 1, [Int64(10), Int64(0)], ArrowBuffer.Empty, nullCount: 0)],
            1);
        var error = await CallFailingAsync(client => client.BindAsync(new BindRequest
        {
            FunctionName = "sequence",
            FunctionType = FunctionType.Table,
            Arguments = RecordBatchIpc.Write(args),
        }));

        Assert.Equal(ErrorCodes.InvalidArgument, error.ErrorCode);
        Assert.Contains("must be >= 1", error.ErrorMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnknownFunction_IsNotFound()
    {
        var error = await CallFailingAsync(client => client.BindAsync(new BindRequest
        {
            FunctionName = "no_such_function",
            FunctionType = FunctionType.Table,
        }));

        Assert.Equal(ErrorCodes.NotFound, error.ErrorCode);
    }

    [Fact]
    public async Task DdlOnADeclarativeCatalog_IsFailedPrecondition()
    {
        var error = await CallFailingAsync(client => client.CatalogSchemaCreateAsync(
            [], ["new_schema"], OnConflict.Error, comment: null, tags: null, transactionOpaqueData: null));

        Assert.Equal(ErrorCodes.FailedPrecondition, error.ErrorCode);
        Assert.Contains("catalog is read-only", error.ErrorMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void UnclassifiedErrors_StayUnknown()
    {
        Assert.Equal(ErrorCodes.Unknown, ErrorModel.CodeOf(new InvalidOperationException("worker bug")));
        Assert.Equal(ErrorCodes.Unimplemented, ErrorModel.CodeOf(new VgiUnimplementedException("not supported")));
    }
}
