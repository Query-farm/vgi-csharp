// Copyright 2025, 2026 Query Farm LLC - https://query.farm
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Reflection;
using System.Text;

namespace QueryFarm.Vgi.Internal;

/// <summary>Canonical VIRTUAL expression carriers; validation never evaluates SQL.</summary>
public static class ColumnExpressions
{
    public const string GeneratedKey = "vgi.column.generated.v1";
    public const string GenerationKey = "vgi.column.generation.v1";
    private const string Profile = "vgi.expression.duckdb.scalar.v1";
    private const int Maximum = 8 * 1024 * 1024;
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private static void Text(string value, int maximum)
    {
        if (string.IsNullOrEmpty(value) || value.Contains('\0') || Utf8.GetByteCount(value) > maximum)
            throw new ArgumentException("Invalid generated expression text.");
    }
    private static void Expression(SqlExpression value, IReadOnlyList<string> owner)
    {
        Text(value.Sql, 1024 * 1024);
        if (string.IsNullOrWhiteSpace(value.Sql) || value.Profile != Profile)
            throw new ArgumentException("Unsupported generated expression profile or empty SQL.");
        if (value.SchemaPath is null || value.SchemaPath.Count is 0 or > 32)
            throw new ArgumentException("Invalid expression owner.");
        foreach (var part in value.SchemaPath)
            Text(part, 4096);
        if (!value.SchemaPath.SequenceEqual(owner))
            throw new ArgumentException("Generated expression owner differs from table schema.");
        var context = value.EvaluationContext ?? throw new ArgumentException("Missing expression context.");
        Text(context.Timezone, 256);
        Text(context.Collation, 256);
        if (context.DefaultOrder is not("asc" or "desc") ||
            context.DefaultNullOrder is not("nulls_first" or "nulls_last" or "nulls_first_on_asc_last_on_desc" or
                                            "nulls_last_on_asc_first_on_desc"))
            throw new ArgumentException("Invalid expression evaluation context.");
    }
    private static bool SameFields(IReadOnlyList<Field> actual, IReadOnlyList<Field> expected) =>
        actual.Count == expected.Count
        && actual.Zip(expected).All(pair => pair.First.Name == pair.Second.Name &&
                                            pair.First.IsNullable == pair.Second.IsNullable &&
                                            pair.First.DataType.TypeId == pair.Second.DataType.TypeId &&
                                            (pair.First.DataType is not NestedType nested ||
                                             pair.Second.DataType is NestedType target &&
                                                 SameFields(nested.Fields, target.Fields)));
    private static void Required(IArrowArray array)
    {
        if (array.NullCount != 0)
            throw new ArgumentException("Required expression value is NULL.");
        if (array is StringArray strings)
            for (int i = 0; i < strings.Length; i++)
                Utf8.GetCharCount(strings.GetBytes(i));
        if (array is StructArray structure)
            foreach (var child in structure.Fields)
                Required(child);
        if (array is ListArray list)
            Required(list.Values);
    }
    private static void Encoded(string encoded, IReadOnlyList<string> owner)
    {
        if (encoded.Length == 0 || encoded.Length > 4 * ((Maximum + 2) / 3))
            throw new ArgumentException("Invalid expression size.");
        byte[] bytes = Convert.FromBase64String(encoded);
        if (bytes.Length == 0 || bytes.Length > Maximum || Convert.ToBase64String(bytes) != encoded)
            throw new ArgumentException("Expression requires canonical base64 IPC.");
        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        using var row = reader.ReadNextRecordBatch() ?? throw new ArgumentException("Expression record is missing.");
        if (row.Length != 1 ||
            !SameFields(row.Schema.FieldsList, SchemaDerivation.InnerSchemaFor(typeof(SqlExpression)).FieldsList))
            throw new ArgumentException("Invalid SqlExpression record schema.");
        foreach (var column in row.Arrays)
            Required(column);
        using var extra = reader.ReadNextRecordBatch();
        if (extra is not null || stream.Position != stream.Length)
            throw new ArgumentException("Expression requires exactly one complete IPC batch.");
        Expression(EmbeddedIpc.Decode<SqlExpression>(bytes), owner);
    }
    public static string GeneratedMetadata(string sql, IReadOnlyList<string> owner)
    {
        var expression = new SqlExpression { Sql = sql, Profile = Profile, SchemaPath = owner.ToList(),
                                             EvaluationContext = new QueryEvaluationContext {
                                                 Timezone = "UTC", Collation = "binary", DefaultOrder = "asc",
                                                 DefaultNullOrder = "nulls_last", PreserveInsertionOrder = true
                                             } };
        Expression(expression, owner);
        return Convert.ToBase64String(EmbeddedIpc.Encode(expression));
    }
    public static void Validate(Schema schema, IReadOnlyList<string> owner, bool allowGenerated = true)
    {
        void Type(IArrowType type)
        {
            if (type is NestedType nested)
                foreach (var child in nested.Fields)
                    Field(child);
            if (type is DictionaryType dictionary)
                Type(dictionary.ValueType);
        }
        void Field(Field field)
        {
            var metadata = field.Metadata;
            if (metadata is not null)
            {
                foreach (var key in metadata.Keys)
                    if ((key.StartsWith("vgi.column.generated.", StringComparison.Ordinal) && key != GeneratedKey) ||
                        (key.StartsWith("vgi.column.generation.", StringComparison.Ordinal) && key != GenerationKey))
                        throw new ArgumentException("Unknown generated metadata version.");
                if (metadata.ContainsKey("generated_expression"))
                    throw new ArgumentException("Legacy generated expression requires explicit canonical translation.");
                bool generated = metadata.TryGetValue(GeneratedKey, out var encoded),
                     category = metadata.TryGetValue(GenerationKey, out var generation);
                if (generated != category)
                    throw new ArgumentException("Generated expression and category must occur together.");
                if (generated)
                {
                    if (generation != "virtual")
                        throw new ArgumentException("Only VIRTUAL generated columns are supported.");
                    if (metadata.ContainsKey("default") || metadata.ContainsKey("vgi.column.default.v1"))
                        throw new ArgumentException("Generated column cannot have a default.");
                    Encoded(encoded !, owner);
                    if (!allowGenerated)
                        throw new NotSupportedException("Generated columns are unsupported by this catalog.");
                }
            }
            Type(field.DataType);
        }
        foreach (var field in schema.FieldsList)
            Field(field);
    }
    public static void ValidateIpc(byte[] bytes, IReadOnlyList<string> owner,
                                   bool allowGenerated = true) => Validate(SchemaIpc.ReadSchemaOnly(bytes), owner,
                                                                           allowGenerated);
}
