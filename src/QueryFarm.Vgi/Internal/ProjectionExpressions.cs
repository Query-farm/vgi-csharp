// Copyright 2025, 2026 Query Farm LLC - https://query.farm
using Apache.Arrow;
using QueryFarm.Vgi.Protocol;
namespace QueryFarm.Vgi.Internal;
/// <summary>Validates optional scalar projection negotiation; never evaluates
/// SQL.</summary>
public static class ProjectionExpressions
{
    public const string Profile = "vgi.expression.duckdb.scalar.v1";
    public static void ValidateOffer(IReadOnlyList<string> profiles)
    {
        if (profiles.Count > 64 || profiles.Distinct(StringComparer.Ordinal).Count() != profiles.Count ||
            profiles.Any(p => p is null || p.Length > 256 ||
                              !System.Text.RegularExpressions.Regex.IsMatch(
                                  p, @"\A[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*\z")))
            throw new ArgumentException("Invalid projection profiles.");
    }
    private static string Identifier(string value) =>
        new(value.Select(c => c is >= 'A' and <= 'Z' ? (char)(c + 32) : c).ToArray());
    private static void Size(long length)
    {
        if (length > 8 * 1024 * 1024)
            throw new ArgumentException("Projection payload exceeds 8 MiB.");
    }
    /// <summary>Validate the original binary carrier before typed decoding can discard extra rows.</summary>
    public static TableFunctionProjectionRequest DecodeRequest(byte[] bytes)
    {
        Size(bytes.Length);
        using var stream = new MemoryStream(bytes);
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(stream);
        using var row = reader.ReadNextRecordBatch() ?? throw new ArgumentException("Projection record missing.");
        var expected = QueryFarm.VgiRpc.Reflection.SchemaDerivation.InnerSchemaFor(typeof(TableFunctionProjectionRequest));
        if (row.Length != 1 || !SchemaIpc.WriteSchemaOnly(row.Schema).SequenceEqual(SchemaIpc.WriteSchemaOnly(expected)))
            throw new ArgumentException("Projection requires one row with its exact schema.");
        foreach (var column in row.Arrays)
            if (column.NullCount != 0) throw new ArgumentException("Required projection field is NULL.");
        using var extra = reader.ReadNextRecordBatch();
        if (extra is not null || stream.Position != stream.Length)
            throw new ArgumentException("Projection requires one complete IPC batch.");
        return EmbeddedIpc.Decode<TableFunctionProjectionRequest>(bytes);
    }
    public static BindRequest ValidateRequest(TableFunctionProjectionRequest request)
    {
        Size((long)request.BindCall.Length + request.BindResult.Length + request.ResultSchema.Length +
             request.Expressions.Sum(b => (long)b.Length));
        Size(EmbeddedIpc.Encode(request).Length);
        var bind = EmbeddedIpc.Decode<BindRequest>(request.BindCall);
        var source = EmbeddedIpc.Decode<BindResponse>(request.BindResult);
        ValidateOffer(source.ProjectionExpressionProfiles);
        if (bind.FunctionType != FunctionType.Table || bind.InputSchema is not null || bind.SchemaPath is null)
            throw new ArgumentException("Projection requires an ordinary table and explicit owner.");
        if (source.LookupSecretTypes.Count + source.LookupScopes.Count + source.LookupNames.Count != 0)
            throw new ArgumentException("Projection cannot request secrets.");
        var schema = SchemaIpc.ReadSchemaOnly(source.OutputSchema);
        var result = SchemaIpc.ReadSchemaOnly(request.ResultSchema);
        if (request.Expressions.Count is<1 or> 1024 || result.FieldsList.Count != request.Expressions.Count ||
            result.Metadata?.Count > 0)
            throw new ArgumentException("Invalid projection result schema/count.");
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var field in schema.FieldsList.Concat(result.FieldsList))
            if (field.Name.Length == 0 || field.Name.Contains('\0') || !names.Add(Identifier(field.Name)))
                throw new ArgumentException("Ambiguous projection column names.");
        byte[]? context = null;
        foreach (var raw in request.Expressions)
        {
            ColumnExpressions.Encoded(Convert.ToBase64String(raw), bind.SchemaPath);
            var value = EmbeddedIpc.Decode<SqlExpression>(raw);
            if (!source.ProjectionExpressionProfiles.Contains(value.Profile))
                throw new ArgumentException("Projection profile was not offered.");
            var next = EmbeddedIpc.Encode(value.EvaluationContext);
            if (context is not null && !context.SequenceEqual(next))
                throw new ArgumentException("Projection contexts differ.");
            context = next;
        }
        return bind;
    }
    public static void ValidateResponse(TableFunctionProjectionRequest request,
                                        TableFunctionProjectionResponse response)
    {
        ValidateRequest(request);
        Size((long)response.AcceptedIndices.Count * 8 + (response.BindResult?.Length ?? 0));
        Size(EmbeddedIpc.Encode(response).Length);
        if ((response.AcceptedIndices.Count == 0) != (response.BindResult is null))
            throw new ArgumentException("Indices and accepted bind result must occur together.");
        long previous = -1;
        foreach (var i in response.AcceptedIndices)
        {
            if (i <= previous || i >= request.Expressions.Count)
                throw new ArgumentException("Invalid accepted projection index.");
            previous = i;
        }
        if (response.BindResult is null)
            return;
        var original = EmbeddedIpc.Decode<BindResponse>(request.BindResult);
        var accepted = EmbeddedIpc.Decode<BindResponse>(response.BindResult);
        ValidateOffer(accepted.ProjectionExpressionProfiles);
        if (!original.ProjectionExpressionProfiles.SequenceEqual(accepted.ProjectionExpressionProfiles) ||
            accepted.LookupSecretTypes.Count + accepted.LookupScopes.Count + accepted.LookupNames.Count != 0)
            throw new ArgumentException("Accepted projection changed bind offer/secrets.");
        var source = SchemaIpc.ReadSchemaOnly(original.OutputSchema);
        var result = SchemaIpc.ReadSchemaOnly(request.ResultSchema);
        var expected =
            new Schema(source.FieldsList.Concat(response.AcceptedIndices.Select(i => result.GetFieldByIndex((int)i))),
                       source.Metadata);
        // Canonical schema serialization includes recursive types, nullability and
        // metadata.
        if (!SchemaIpc.WriteSchemaOnly(expected).SequenceEqual(
                SchemaIpc.WriteSchemaOnly(SchemaIpc.ReadSchemaOnly(accepted.OutputSchema))))
            throw new ArgumentException("Accepted projection changed append-only schema.");
    }
}
