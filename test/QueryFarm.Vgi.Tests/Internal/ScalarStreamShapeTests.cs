using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// The declared WIRE SHAPE of a scalar function's stream.
///
/// <para>A scalar call is an exchange: DuckDB pushes one batch of argument columns per turn and
/// reads one batch of results back. The only thing on the wire that says so is the stream's
/// <c>InputSchema</c> — <c>QueryFarm.VgiRpc.Http.RpcHttpEndpoints.HandleStreamInitAsync</c>
/// classifies a stream with <c>stream.InputSchema is not { FieldsList.Count: &gt; 0 }</c> as a
/// PRODUCER, and a producer's first tick is folded into the <c>/init</c> request itself and driven
/// with a zero-COLUMN batch. This port declared <c>InputSchema: null</c>, so every scalar function
/// over HTTP was ticked with that batch before DuckDB had sent a single argument row, and the
/// function threw out of <c>RecordBatch.Column(0)</c> — 26 integration files failed there first,
/// across <c>scalar/</c>, <c>settings/</c>, <c>global_functions/</c>, <c>overload/</c>,
/// <c>aggregate/</c>, <c>cache/</c>, <c>connection_string.test</c> and
/// <c>unary_error_propagation.test</c>.</para>
///
/// <para>Nothing caught it because the pipe/launcher transport never synthesizes a turn — it only
/// delivers batches the client actually sent — and the repo's integration lane ran only that
/// transport. An HTTP lane now runs alongside it (<c>ci/run-integration.sh TRANSPORT=http</c>);
/// this test is the fast, DuckDB-free guard on the same invariant.</para>
/// </summary>
public class ScalarStreamShapeTests
{
    private sealed class DoublingScalar : ScalarFn
    {
        public override string Name => "doubling";

        private void Compute([Param] Int64Array value, Int64Array.Builder result)
        {
            for (var row = 0; row < value.Length; row++)
            {
                result.Append(value.GetValue(row) * 2);
            }
        }
    }

    private static async Task<(VgiServiceImpl Service, byte[] Attach)> NewAttachedServiceAsync()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new DoublingScalar());
        var service = new VgiServiceImpl(registry);
        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        return (service, attach.AttachOpaqueData ?? []);
    }

    [Fact]
    public async Task InitAsync_DeclaresTheScalarsArgumentColumns_AsTheStreamsInputSchema()
    {
        var (service, attach) = await NewAttachedServiceAsync();
        var arguments = new Schema([new Field("value", Int64Type.Default, nullable: true)], metadata: null);
        var bindRequest = new BindRequest
        {
            FunctionName = "doubling",
            FunctionType = FunctionType.Scalar,
            Arguments = [],
            InputSchema = SchemaIpc.WriteSchemaOnly(arguments),
            AttachOpaqueData = attach,
        };

        var stream = await service.InitAsync(new InitRequest { BindCall = EmbeddedIpc.Encode(bindRequest) });

        Assert.NotNull(stream.InputSchema);
        Assert.Equal(["value"], stream.InputSchema!.FieldsList.Select(f => f.Name));
        Assert.Equal(ArrowTypeId.Int64, stream.InputSchema.GetFieldByIndex(0).DataType.TypeId);
    }

    /// <summary>The predicate above, spelled exactly as the transport spells it. A scalar stream
    /// that answers <see langword="true"/> here is one every HTTP worker will tick as a producer,
    /// whatever the schema on it happens to look like.</summary>
    [Fact]
    public async Task InitAsync_ScalarStreamIsNeverClassifiedAsAProducerByTheHttpTransport()
    {
        var (service, attach) = await NewAttachedServiceAsync();
        var arguments = new Schema([new Field("value", Int64Type.Default, nullable: true)], metadata: null);
        var bindRequest = new BindRequest
        {
            FunctionName = "doubling",
            FunctionType = FunctionType.Scalar,
            Arguments = [],
            InputSchema = SchemaIpc.WriteSchemaOnly(arguments),
            AttachOpaqueData = attach,
        };

        var stream = await service.InitAsync(new InitRequest { BindCall = EmbeddedIpc.Encode(bindRequest) });

        var readsAsProducer = stream.InputSchema is not { FieldsList.Count: > 0 };
        Assert.False(readsAsProducer);
    }
}
