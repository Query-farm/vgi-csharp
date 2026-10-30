// Copyright 2025, 2026 Query Farm LLC - https://query.farm
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Reflection;
using System.Text;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Canonical VIRTUAL generated-column carriers, checked the way the DuckDB extension reads them
/// (<c>ReadNativeColumnGenerated</c>/<c>ValidateNativeGeneratedColumns</c> in
/// <c>vgi_column_expression.cpp</c>). Validation parses but never evaluates SQL.
/// <para>A generated column is VIRTUAL: DuckDB computes it from the physical columns, so it must be
/// nullable, of a BOOLEAN, signed integer (TINYINT..BIGINT) or VARCHAR type, and its expression is
/// limited to literals of those types, unqualified column references, non-TRY casts to those types,
/// comparisons, AND/OR/NOT, IS [NOT] NULL, COALESCE, and the functions <c>+ - * % || abs lower
/// upper</c>. The table's scan emits only the physical (non-generated) columns.</para>
/// </summary>
public static class ColumnExpressions
{
    public const string GeneratedKey = "vgi.column.generated.v1";
    public const string GenerationKey = "vgi.column.generation.v1";

    /// <summary>The pre-v3 metadata key whose value was the raw SQL expression. Translated to
    /// <see cref="GeneratedKey"/>/<see cref="GenerationKey"/> by <see cref="TranslateLegacy"/>.</summary>
    public const string LegacyGeneratedKey = "generated_expression";
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
        if (value.SchemaPath is null || value.SchemaPath.Count > 32)
            throw new ArgumentException("Invalid expression owner.");
        foreach (var part in value.SchemaPath)
            Text(part, 4096);
        if (!value.SchemaPath.SequenceEqual(owner))
            throw new ArgumentException("Generated expression owner differs from table schema.");
        var context = value.EvaluationContext ?? throw new ArgumentException("Missing expression context.");
        Text(context.Timezone, 256);
        Text(context.Collation, 256);
        if (context.DefaultOrder is not ("asc" or "desc") ||
            context.DefaultNullOrder is not ("nulls_first" or "nulls_last" or "nulls_first_on_asc_last_on_desc" or
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
    public static void Encoded(string encoded, IReadOnlyList<string> owner)
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
        var expression = new SqlExpression
        {
            Sql = sql,
            Profile = Profile,
            SchemaPath = owner.ToList(),
            EvaluationContext = new QueryEvaluationContext
            {
                Timezone = "UTC",
                Collation = "binary",
                DefaultOrder = "asc",
                DefaultNullOrder = "nulls_last",
                PreserveInsertionOrder = true
            }
        };
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
                if (metadata.ContainsKey(LegacyGeneratedKey))
                    throw new ArgumentException(
                        $"Column '{field.Name}': legacy '{LegacyGeneratedKey}' metadata is only translated on top-level columns " +
                        "without other expression metadata; use CatalogTable.GeneratedColumns or ColumnExpressions.GeneratedMetadata.");
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
                    Encoded(encoded!, owner);
                    if (!allowGenerated)
                        throw new NotSupportedException("Generated columns are unsupported by this catalog.");
                }
            }
            Type(field.DataType);
        }
        foreach (var field in schema.FieldsList)
            Field(field);
        ValidateGenerated(schema);
    }

    /// <summary>The extension's per-column rules for top-level generated columns: nullable, a
    /// generated primitive type, the generated scalar grammar, and references only to existing
    /// columns of generated primitive types; a table with generated columns keeps at least one
    /// physical column.</summary>
    private static void ValidateGenerated(Schema schema)
    {
        var generated = schema.FieldsList.Where(field => field.Metadata?.ContainsKey(GeneratedKey) == true).ToList();
        if (generated.Count == 0)
            return;
        if (generated.Count == schema.FieldsList.Count)
            throw new ArgumentException("A table with generated columns requires at least one physical column.");
        foreach (var field in generated)
        {
            if (!field.IsNullable)
                throw new ArgumentException($"Generated column '{field.Name}' must be nullable (DuckDB computes VIRTUAL columns).");
            if (field.Metadata!.ContainsKey("ARROW:extension:name") || !GeneratedType(field.DataType))
                throw new ArgumentException(
                    $"Generated column '{field.Name}' has type {field.DataType.Name}; generated columns must be BOOLEAN, " +
                    "TINYINT, SMALLINT, INTEGER, BIGINT or VARCHAR.");
            var sql = EmbeddedIpc.Decode<SqlExpression>(Convert.FromBase64String(field.Metadata[GeneratedKey])).Sql;
            ScalarSqlGrammar.Node root;
            try
            {
                root = ScalarSqlGrammar.Parse(sql);
            }
            catch (FormatException error)
            {
                throw new ArgumentException($"Generated column '{field.Name}' ({sql}) is outside the supported grammar: {error.Message}");
            }

            var nodes = 0;
            void Walk(ScalarSqlGrammar.Node node, int depth)
            {
                if (depth > 64 || ++nodes > 4096)
                    throw new ArgumentException($"Generated column '{field.Name}' ({sql}) is too complex.");
                var problem = GeneratedNodeProblem(node, schema);
                if (problem is not null)
                    throw new ArgumentException($"Generated column '{field.Name}' ({sql}): {problem}.");
                foreach (var child in ScalarSqlGrammar.Children(node))
                    Walk(child, depth + 1);
            }
            Walk(root, 0);
        }
    }

    private static readonly HashSet<string> GeneratedFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "+", "-", "*", "%", "abs", "lower", "upper", "||",
    };

    private static string? GeneratedNodeProblem(ScalarSqlGrammar.Node node, Schema schema) => node switch
    {
        ScalarSqlGrammar.Constant constant => constant.Type is ScalarSqlGrammar.ConstantType.HugeInt or
            ScalarSqlGrammar.ConstantType.Decimal or ScalarSqlGrammar.ConstantType.Double
            ? "literals must be BOOLEAN, integer (up to BIGINT), VARCHAR or NULL" : null,
        ScalarSqlGrammar.ColumnRef column when column.Names.Count != 1 => "column references must be unqualified",
        ScalarSqlGrammar.ColumnRef column => schema.FieldsList.Where(f =>
                string.Equals(f.Name, column.Names[0], StringComparison.OrdinalIgnoreCase)).ToList() switch
        {
            [] => $"references unknown column {column.Names[0]}",
            [var target] when target.Metadata?.ContainsKey("ARROW:extension:name") == true || !GeneratedType(target.DataType) =>
                $"operand {target.Name} must be a BOOLEAN, signed integer or VARCHAR column",
            [_] => null,
            _ => $"column reference {column.Names[0]} is ambiguous",
        },
        ScalarSqlGrammar.Cast cast when cast.Try => "TRY_CAST is unsupported",
        ScalarSqlGrammar.Cast cast => ScalarSqlGrammar.PrimitiveTypeName(cast.TypeName) is "BOOLEAN" or "TINYINT" or "SMALLINT"
            or "INTEGER" or "BIGINT" or "VARCHAR" ? null : $"CAST to {string.Join('.', cast.TypeName)} is unsupported",
        ScalarSqlGrammar.Comparison or ScalarSqlGrammar.Conjunction or ScalarSqlGrammar.Operator => null,
        ScalarSqlGrammar.Function function => function.Name.Count == 1 && GeneratedFunctions.Contains(function.Name[0])
            ? null : $"function {string.Join('.', function.Name)} is unsupported (allowed: + - * % || abs lower upper)",
        _ => "expression is outside the supported deterministic scalar grammar",
    };

    private static bool GeneratedType(IArrowType type) =>
        type.TypeId is ArrowTypeId.Boolean or ArrowTypeId.Int8 or ArrowTypeId.Int16 or ArrowTypeId.Int32
            or ArrowTypeId.Int64 or ArrowTypeId.String;

    /// <summary>
    /// Translates top-level legacy <see cref="LegacyGeneratedKey"/> metadata to the canonical
    /// carrier when that is unambiguous: the field carries no canonical generated metadata and no
    /// default. A legacy value that already is canonical base64 SqlExpression IPC (written through
    /// the 0.22 value of <c>VgiWireMetadata.GeneratedExpressionKey</c>) moves as is; any other
    /// value is raw SQL, encoded with <see cref="GeneratedMetadata"/>. Returns
    /// <paramref name="schema"/> itself when nothing needs translating.
    /// </summary>
    public static Schema TranslateLegacy(Schema schema, IReadOnlyList<string> owner)
    {
        if (!schema.FieldsList.Any(field => field.Metadata?.ContainsKey(LegacyGeneratedKey) == true))
            return schema;
        var fields = schema.FieldsList.Select(field =>
        {
            if (field.Metadata is not { } metadata || !metadata.TryGetValue(LegacyGeneratedKey, out var legacy))
                return field;
            if (metadata.ContainsKey(GeneratedKey) || metadata.ContainsKey(GenerationKey) || metadata.ContainsKey("default") ||
                metadata.ContainsKey("vgi.column.default.v1"))
                throw new ArgumentException(
                    $"Column '{field.Name}' carries legacy '{LegacyGeneratedKey}' metadata together with canonical expression " +
                    "or default metadata; keep only the canonical keys.");
            string canonical;
            try
            {
                Encoded(legacy, owner);
                canonical = legacy;
            }
            catch (Exception)
            {
                // Not canonical IPC, so it is the legacy raw SQL.
                canonical = GeneratedMetadata(legacy, owner);
            }

            var translated = metadata.Where(pair => pair.Key != LegacyGeneratedKey)
                .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);
            translated[GeneratedKey] = canonical;
            translated[GenerationKey] = "virtual";
            return new Field(field.Name, field.DataType, field.IsNullable, translated);
        }).ToList();
        return new Schema(fields, schema.Metadata);
    }
    public static void ValidateIpc(byte[] bytes, IReadOnlyList<string> owner,
                                   bool allowGenerated = true) => Validate(SchemaIpc.ReadSchemaOnly(bytes), owner,
                                                                           allowGenerated);
}
