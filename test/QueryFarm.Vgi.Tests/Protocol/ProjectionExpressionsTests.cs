// Copyright 2026 Query Farm LLC - https://query.farm
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

public class ProjectionExpressionsTests
{
    private static readonly Schema Source =
        new([new Field("n", Int32Type.Default, true)], new Dictionary<string, string> { ["source"] = "retained" });
    private static readonly Field Result =
        new("wide", Int64Type.Default, true, new Dictionary<string, string> { ["requested"] = "retained" });
    private static TableFunctionProjectionRequest Proposal(List<string> owner) => new() {
        BindCall = EmbeddedIpc.Encode(new BindRequest { FunctionName = "projection_sequence",
                                                        FunctionType = FunctionType.Table, SchemaPath = owner }),
        BindResult =
            EmbeddedIpc.Encode(new BindResponse { OutputSchema = SchemaIpc.WriteSchemaOnly(Source),
                                                  ProjectionExpressionProfiles = [ProjectionExpressions.Profile] }),
        Expressions = [EmbeddedIpc.Encode(new SqlExpression {
            Sql = "CAST(\"n\" AS BIGINT)", Profile = ProjectionExpressions.Profile, SchemaPath = owner,
            EvaluationContext = new() { Timezone = "UTC", Collation = "binary", DefaultOrder = "asc",
                                        DefaultNullOrder = "nulls_last", PreserveInsertionOrder = true }
        })],
        ResultSchema = SchemaIpc.WriteSchemaOnly(new Schema([Result], null)),
    };
    private static TableFunctionProjectionResponse Accepted(Schema schema) => new() {
        AcceptedIndices = [0],
        BindResult =
            EmbeddedIpc.Encode(new BindResponse { OutputSchema = SchemaIpc.WriteSchemaOnly(schema), OpaqueData = [42],
                                                  ProjectionExpressionProfiles = [ProjectionExpressions.Profile] }),
    };
    [Fact]
    public void AcceptsExactAppendedSchemaAndDecline()
    {
        var proposal = Proposal(["main"]);
        var schema = new Schema(Source.FieldsList.Concat([Result]), Source.Metadata);
        ProjectionExpressions.ValidateResponse(proposal, Accepted(schema));
        ProjectionExpressions.ValidateResponse(proposal, new());
        var withoutMetadata =
            new Schema(Source.FieldsList.Concat([new Field("wide", Int64Type.Default, true)]), Source.Metadata);
        Assert.Throws<ArgumentException>(
            () => ProjectionExpressions.ValidateResponse(proposal, Accepted(withoutMetadata)));
    }
    [Fact]
    public void RejectsMalformedAcceptanceAndExpressionEnvelope()
    {
        var proposal = Proposal(["main"]);
        Assert.Throws<ArgumentException>(() => ProjectionExpressions.ValidateResponse(proposal, Accepted(Source)));
        Assert.Throws<ArgumentException>(
            () => ProjectionExpressions.ValidateResponse(
                proposal, new() { AcceptedIndices = [-1], BindResult = proposal.BindResult }));
        Assert.Throws<ArgumentException>(
            () => ProjectionExpressions.ValidateResponse(
                proposal, new() { AcceptedIndices = [0, 0], BindResult = proposal.BindResult }));
        Assert.Throws<ArgumentException>(
            () => ProjectionExpressions.ValidateResponse(proposal, new() { BindResult = proposal.BindResult }));
        proposal.Expressions[0] = [..proposal.Expressions[0], 0];
        Assert.Throws<ArgumentException>(() => ProjectionExpressions.ValidateRequest(proposal));
    }
    [Fact]
    public void ValidatesOffersAndExplicitRootOwner()
    {
        ProjectionExpressions.ValidateOffer([ProjectionExpressions.Profile, "provider.future.v2"]);
        Assert.Throws<ArgumentException>(() => ProjectionExpressions.ValidateOffer(["vgi.bad"]));
        Assert.Throws<ArgumentException>(
            () => ProjectionExpressions.ValidateOffer([ProjectionExpressions.Profile, ProjectionExpressions.Profile]));
        ProjectionExpressions.ValidateRequest(Proposal([]));
        var proposal = Proposal(["main"]);
        proposal.ResultSchema = new byte[8 * 1024 * 1024];
        Assert.Throws<ArgumentException>(() => ProjectionExpressions.ValidateRequest(proposal));
    }
    [Fact]
    public void IncludesArrowFramingInSizeLimitBeforeNestedDecode()
    {
        var request = Proposal(["main"]);
        int other = request.BindCall.Length + request.BindResult.Length + request.Expressions[0].Length;
        request.ResultSchema = new byte[8 * 1024 * 1024 - other - 16];
        var error = Assert.Throws<ArgumentException>(() => ProjectionExpressions.ValidateRequest(request));
        Assert.Contains("exceeds 8 MiB", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidatesRawOuterEnvelopeBeforeTypedDecoding()
    {
        var raw = EmbeddedIpc.Encode(Proposal(["main"]));
        Assert.Single(ProjectionExpressions.DecodeRequest(raw).Expressions);
        Assert.Throws<ArgumentException>(() => ProjectionExpressions.DecodeRequest([.. raw, 0]));
        using var source = new MemoryStream(raw);
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(source);
        using var row = reader.ReadNextRecordBatch()!;
        using var sink = new MemoryStream();
        using (var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(sink, row.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(row); writer.WriteRecordBatch(row); writer.WriteEnd();
        }
        Assert.Throws<ArgumentException>(() => ProjectionExpressions.DecodeRequest(sink.ToArray()));
    }

    [Fact]
    public void RepeatabilityDefaultsAndProjectionPreservation()
    {
        Assert.Equal(BindRepeatability.Unknown, new BindResponse().Repeatability);
        Assert.Throws<ArgumentException>(() => ProjectionExpressions.ValidateRepeatability((BindRepeatability)999));
        foreach (var promise in Enum.GetValues<BindRepeatability>())
        {
            var request = Proposal(["main"]);
            var original = EmbeddedIpc.Decode<BindResponse>(request.BindResult);
            original.Repeatability = promise;
            request.BindResult = EmbeddedIpc.Encode(original);
            var response = Accepted(new Schema(Source.FieldsList.Concat([Result]), Source.Metadata));
            var accepted = EmbeddedIpc.Decode<BindResponse>(response.BindResult!);
            accepted.Repeatability = promise;
            response.BindResult = EmbeddedIpc.Encode(accepted);
            ProjectionExpressions.ValidateResponse(request, response);
            accepted.Repeatability = promise == BindRepeatability.Unknown
                ? BindRepeatability.StableWithinQuery : BindRepeatability.Unknown;
            response.BindResult = EmbeddedIpc.Encode(accepted);
            Assert.Throws<ArgumentException>(() => ProjectionExpressions.ValidateResponse(request, response));
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("unknown")]
    [InlineData("stable_within_query")]
    [InlineData("0")]
    [InlineData("ALWAYS")]
    public void RejectsMalformedDictionaryPromise(string? wireValue)
    {
        var request = Proposal(["main"]);
        using var source = new MemoryStream(request.BindResult);
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(source);
        using var row = reader.ReadNextRecordBatch()!;
        int index = row.Schema.GetFieldIndex("repeatability");
        var dictionary = (DictionaryArray)row.Column(index);
        var original = new StringArray.Builder().Append(wireValue ?? "UNKNOWN").Build();
        // The valid index still references UNKNOWN's bytes, but the value is NULL.
        var values = wireValue is null
            ? new StringArray(1, original.Data.Buffers[1], original.Data.Buffers[2],
                              new ArrowBuffer(new byte[] { 0 }), nullCount: 1)
            : original;
        var columns = row.Arrays.ToArray();
        columns[index] = new DictionaryArray((DictionaryType)dictionary.Data.DataType,
            new Int16Array.Builder().Append((short)0).Build(), values);
        using var malformed = new RecordBatch(row.Schema, columns, 1);
        using var sink = new MemoryStream();
        using (var writer = new Apache.Arrow.Ipc.ArrowStreamWriter(sink, row.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(malformed);
            writer.WriteEnd();
        }
        request.BindResult = sink.ToArray();
        Assert.Throws<ArgumentException>(() => ProjectionExpressions.ValidateRequest(request));
    }

    private sealed class PerBindPromise : QueryFarm.Vgi.Table.ITableFunction
    {
        public string Name => "per_bind_promise";
        public Schema ArgumentsSchema { get; } = new([], null);
        public Schema OutputSchema => Source;
        public BindRepeatability Repeatability(QueryFarm.Vgi.Table.TableBindParams parameters) =>
            parameters.TransactionOpaqueData.Length == 0 ? BindRepeatability.Unknown : BindRepeatability.StableWithinQuery;
        public QueryFarm.Vgi.Table.ITableFunctionProducer CreateProducer(QueryFarm.Vgi.Table.TableInitParams parameters) =>
            throw new NotSupportedException();
    }

    [Fact]
    public async Task ProviderPromiseCanDependOnExistingBindTransactionAnchor()
    {
        var catalog = new CatalogRegistry();
        catalog.RegisterTable(new PerBindPromise());
        var service = new VgiServiceImpl(catalog);
        var request = new BindRequest { FunctionName = "per_bind_promise", FunctionType = FunctionType.Table,
                                        SchemaPath = ["main"] };
        Assert.Equal(BindRepeatability.Unknown, (await service.BindAsync(request)).Repeatability);
        request.TransactionOpaqueData = [1, 2, 3];
        Assert.Equal(BindRepeatability.StableWithinQuery, (await service.BindAsync(request)).Repeatability);
    }

}
