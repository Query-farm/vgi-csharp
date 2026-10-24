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

}
