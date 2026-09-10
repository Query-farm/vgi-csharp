using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>Strict decoder for the VGI 2.0 <c>vgi.filters.v2</c> snapshot and delta format.</summary>
public static partial class PushdownFilterCodec
{
    public const string EncodingName = "vgi.filters.v2";
    public const string SemanticsName = "vgi.duckdb.standard.v1";
    public const string NoEvaluationContext = "vgi.none.v1";

    private const int MaxFilterIpcBytes = 16 << 20;
    private const int MaxExpressionNodes = 10_000;
    private const int MaxIdBytes = 128;

    private static readonly HashSet<string> KnownArrowExtensions = new(StringComparer.Ordinal)
    {
        "arrow.bool8", "arrow.json", "arrow.uuid",
        "geoarrow.linestring", "geoarrow.multilinestring", "geoarrow.multipoint",
        "geoarrow.multipolygon", "geoarrow.point", "geoarrow.polygon", "geoarrow.wkb",
    };

    [GeneratedRegex("^(?:value|type|artifact)_(?:0|[1-9][0-9]*)$", RegexOptions.CultureInvariant)]
    private static partial Regex PayloadNamePattern();

    [GeneratedRegex("^[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9_]*)*$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentityNamespacePattern();

    [GeneratedRegex("^[a-z][a-z0-9_]*$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentityNamePattern();

    public static DecodedFilters? Decode(byte[]? pushdownFilters, IReadOnlyList<byte[]>? joinKeys, Schema outputSchema,
        IReadOnlyList<FilterFunctionCapability>? extensionFunctions = null)
    {
        ArgumentNullException.ThrowIfNull(outputSchema);
        if (pushdownFilters is null || pushdownFilters.Length == 0)
        {
            return null;
        }

        if (pushdownFilters.Length > MaxFilterIpcBytes)
        {
            throw Error("VGI filter IPC payload exceeds the 16 MiB encoded-size limit.");
        }

        var batch = ReadSingleBatch(pushdownFilters, "filter payload");
        ValidateBatch(batch);
        var keys = (joinKeys ?? []).Select((bytes, i) => ReadSingleBatch(bytes, $"join_keys[{i}]")).ToList();
        var json = ((StringArray)batch.Column(0)).GetString(0)!;
        if (Encoding.UTF8.GetByteCount(json) > 1 << 20)
        {
            throw new InvalidDataException("VGI filter JSON exceeds 1 MiB.");
        }

        using var document = JsonDocument.Parse(json, new JsonDocumentOptions
        {
            AllowTrailingCommas = false,
            CommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = 64,
        });
        RejectDuplicateKeys(document.RootElement);
        var root = document.RootElement;
        AssertObject(root, "filter document");
        String(root, "encoding", EncodingName);
        String(root, "semantics", SemanticsName);
        var kind = String(root, "kind");
        if (kind is not ("snapshot" or "delta"))
        {
            throw Error("Filter document kind must be 'snapshot' or 'delta'.");
        }

        var member = kind == "snapshot" ? "predicates" : "updates";
        Exact(root, ["encoding", "semantics", "kind", member], "filter document");
        var entries = Array(root, member);
        if (entries.GetArrayLength() > 1024)
        {
            throw Error("Filter document exceeds the predicate limit.");
        }

        var payload = new Dictionary<string, (Field Field, IArrowArray Array)>(StringComparer.Ordinal);
        for (var i = 1; i < batch.ColumnCount; i++)
        {
            payload.Add(batch.Schema.GetFieldByIndex(i).Name, (batch.Schema.GetFieldByIndex(i), batch.Column(i)));
        }

        var predicates = new List<DecodedPredicate>();
        var updates = new List<DecodedFilterUpdate>();
        var ids = new HashSet<string>(StringComparer.Ordinal);
        int[] nodes = [0];
        foreach (var (entry, index) in entries.EnumerateArray().Select((v, i) => (v, i)))
        {
            var where = $"{member}[{index}]";
            AssertObject(entry, where);
            if (kind == "snapshot")
            {
                var predicate = Predicate(entry, payload, keys, outputSchema, extensionFunctions ?? [], where,
                    true, nodes);
                if (!ids.Add(predicate.Id))
                {
                    throw Error($"Duplicate predicate ID '{predicate.Id}'.");
                }

                predicates.Add(predicate);
                continue;
            }

            var operation = String(entry, "operation");
            if (operation == "remove")
            {
                Exact(entry, ["operation", "id", "revision"], where);
                var update = new DecodedFilterUpdate(operation, PredicateId(entry, where), UInt(entry, "revision"), null);
                if (!ids.Add(update.Id))
                {
                    throw Error($"Duplicate update ID '{update.Id}'.");
                }

                updates.Add(update);
            }
            else if (operation == "upsert")
            {
                Exact(entry, ["operation", "id", "revision", "mode", "source", "expression"], where);
                var predicate = Predicate(entry, payload, keys, outputSchema, extensionFunctions ?? [], where,
                    false, nodes, alreadyChecked: true);
                if (predicate.Mode != "advisory")
                {
                    throw Error("Delta upserts must be advisory.");
                }

                if (!ids.Add(predicate.Id))
                {
                    throw Error($"Duplicate update ID '{predicate.Id}'.");
                }

                updates.Add(new DecodedFilterUpdate(operation, predicate.Id, predicate.Revision, predicate));
                predicates.Add(predicate);
            }
            else
            {
                throw Error($"{where}.operation is invalid.");
            }
        }

        return new DecodedFilters(root.Clone(), kind, predicates, updates, payload, keys);
    }

    private static DecodedPredicate Predicate(JsonElement value,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload,
        IReadOnlyList<RecordBatch> keys, Schema outputSchema,
        IReadOnlyList<FilterFunctionCapability> extensionFunctions,
        string where, bool snapshot, int[] nodes, bool alreadyChecked = false)
    {
        if (!alreadyChecked)
        {
            Exact(value, ["id", "revision", "mode", "source", "expression"], where);
        }

        var id = PredicateId(value, where);
        var revision = UInt(value, "revision");
        if (snapshot && revision != 0)
        {
            throw Error("Snapshot predicate revisions must be zero.");
        }

        var mode = String(value, "mode");
        var source = String(value, "source");
        if (mode is not ("required" or "advisory") ||
            source is not ("query" or "join" or "top_n" or "split_refinement" or "other"))
        {
            throw Error($"{where} has an invalid mode or source.");
        }

        var expression = Object(value, "expression");
        ValidateExpression(expression, payload, keys, outputSchema, extensionFunctions, 0, nodes, root: true);
        if (!IsBooleanExpression(expression, payload, outputSchema))
        {
            throw Error("Predicate root must resolve to BOOLEAN.");
        }

        if (RequiresSessionContext(expression, payload, outputSchema))
        {
            throw Error($"Context-dependent expression requires a supported session profile, not {NoEvaluationContext}.");
        }

        return new DecodedPredicate(id, revision, mode, source, expression.Clone());
    }

    private static void ValidateExpression(JsonElement value,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload,
        IReadOnlyList<RecordBatch> keys, Schema outputSchema,
        IReadOnlyList<FilterFunctionCapability> extensionFunctions,
        int depth, int[] nodes, bool root = false)
    {
        if (depth >= 64)
        {
            throw Error("Filter expression exceeds the depth limit.");
        }

        if (++nodes[0] > MaxExpressionNodes)
        {
            throw Error("Filter document exceeds the expression-node limit.");
        }

        AssertObject(value, "expression");
        var node = String(value, "node");
        void Child(string name) => ValidateExpression(Object(value, name), payload, keys, outputSchema,
            extensionFunctions, depth + 1, nodes);
        switch (node)
        {
            case "column_ref":
                Exact(value, ["node", "column_index", "column_name"], node);
                var columnIndex = checked((int)UInt(value, "column_index"));
                var columnName = NonemptyString(value, "column_name");
                if (columnIndex >= outputSchema.FieldsList.Count || outputSchema.GetFieldByIndex(columnIndex).Name != columnName)
                {
                    throw Error($"column_ref '{columnName}' does not match bind-output index {columnIndex}.");
                }

                ValidateArrowExtensions(outputSchema.GetFieldByIndex(columnIndex));

                return;
            case "field_ref":
                Exact(value, ["node", "expression", "field_index", "field_name"], node);
                Child("expression");
                var fieldIndex = checked((int)UInt(value, "field_index"));
                var fieldName = NonemptyString(value, "field_name");
                var parent = ReferencedField(value.GetProperty("expression"), outputSchema);
                if (parent.DataType is not StructType structure || fieldIndex >= structure.Fields.Count ||
                    structure.Fields[fieldIndex].Name != fieldName)
                {
                    throw Error($"field_ref '{fieldName}' does not match struct field index {fieldIndex}.");
                }

                return;
            case "literal":
                Exact(value, ["node", "value_ref"], node);
                Payload(payload, "value", UInt(value, "value_ref"));
                return;
            case "comparison":
                Exact(value, ["node", "op", "left", "right"], node);
                if (String(value, "op") is not ("eq" or "ne" or "lt" or "le" or "gt" or "ge" or "distinct_from" or "not_distinct_from"))
                {
                    throw Error("Unknown comparison operator.");
                }

                Child("left");
                Child("right");
                if (!BindCompatible(ReferencedType(value.GetProperty("left"), payload, outputSchema),
                        ReferencedType(value.GetProperty("right"), payload, outputSchema)))
                {
                    throw Error("Comparison operands are not bind-compatible.");
                }

                return;
            case "and":
            case "or":
                Exact(value, ["node", "children"], node);
                var children = Array(value, "children");
                if (children.GetArrayLength() < 2)
                {
                    throw Error($"{node} requires at least two children.");
                }

                foreach (var child in children.EnumerateArray())
                {
                    ValidateExpression(child, payload, keys, outputSchema, extensionFunctions, depth + 1, nodes);
                    if (!IsBooleanExpression(child, payload, outputSchema))
                    {
                        throw Error($"{node} children must resolve to BOOLEAN.");
                    }
                }

                return;
            case "not":
                Exact(value, ["node", "expression"], node);
                Child("expression");
                if (!IsBooleanExpression(value.GetProperty("expression"), payload, outputSchema))
                {
                    throw Error("not input must resolve to BOOLEAN.");
                }

                return;
            case "negate":
                Exact(value, ["node", "expression"], node);
                Child("expression");
                if (!IsNumericType(ReferencedType(value.GetProperty("expression"), payload, outputSchema)))
                {
                    throw Error("negate input must resolve to a numeric type.");
                }

                return;
            case "is_null":
                Exact(value, ["node", "expression", "negated"], node);
                Child("expression");
                Boolean(value, "negated");
                return;
            case "in":
                Exact(value, ["node", "expression", "set", "negated"], node);
                Child("expression");
                Boolean(value, "negated");
                var testedType = ReferencedType(value.GetProperty("expression"), payload, outputSchema);
                var setType = ValidateSet(Object(value, "set"), payload, keys);
                if (!BindCompatible(testedType, setType))
                {
                    throw Error("IN expression and set element types are not bind-compatible.");
                }

                return;
            case "cast":
                Exact(value, ["node", "expression", "type_ref"], node);
                Child("expression");
                var type = Payload(payload, "type", UInt(value, "type_ref"));
                ValidateArrowExtensions(type.Field);
                if (!type.Array.IsNull(0))
                {
                    throw Error("A type_N payload value must be NULL.");
                }

                var castSource = ReferencedType(value.GetProperty("expression"), payload, outputSchema);
                var contextualCast = IsContextualType(castSource) && IsContextualType(type.Field.DataType);
                if (!contextualCast && (!IsNumericType(castSource) || !IsNumericType(type.Field.DataType)))
                {
                    throw Error("The C# v2 evaluator supports only context-independent numeric casts.");
                }

                return;
            case "arithmetic":
                Exact(value, ["node", "op", "left", "right"], node);
                if (String(value, "op") is not ("add" or "subtract" or "multiply" or "divide" or "modulo"))
                {
                    throw Error("Unknown arithmetic operator.");
                }

                Child("left");
                Child("right");
                if (!IsNumericType(ReferencedType(value.GetProperty("left"), payload, outputSchema)) ||
                    !IsNumericType(ReferencedType(value.GetProperty("right"), payload, outputSchema)))
                {
                    throw Error("Arithmetic operands must resolve to numeric types.");
                }

                return;
            case "call":
                var actual = value.EnumerateObject().Select(property => property.Name).ToHashSet(StringComparer.Ordinal);
                var required = new[] { "node", "function", "arguments" };
                if (required.Any(name => !actual.Contains(name)) || actual.Any(name => !required.Contains(name) && name != "options"))
                {
                    throw Error("call has missing or unknown properties.");
                }

                ValidateFunction(value.GetProperty("function"), value.TryGetProperty("options", out var options) ? options : null,
                    extensionFunctions);

                var arguments = Array(value, "arguments");
                if (arguments.GetArrayLength() > 256)
                {
                    throw Error("Filter call exceeds the argument limit.");
                }

                foreach (var argument in arguments.EnumerateArray())
                {
                    ValidateExpression(argument, payload, keys, outputSchema, extensionFunctions, depth + 1, nodes);
                }

                if (arguments.GetArrayLength() != 2)
                {
                    throw Error("Registered v2 filter functions require exactly two arguments.");
                }

                ValidateCallBinding(value.GetProperty("function"), arguments, payload, outputSchema);

                return;
            case "runtime_filter":
                if (!root)
                {
                    throw Error("runtime_filter may appear only as a predicate root.");
                }

                throw new NotSupportedException($"The C# worker does not advertise or accept '{node}'.");
            default:
                throw Error($"Unknown filter expression node '{node}'.");
        }
    }

    private static void ValidateFunction(JsonElement function, JsonElement? options,
        IReadOnlyList<FilterFunctionCapability> extensionFunctions)
    {
        if (function.ValueKind == JsonValueKind.String)
        {
            var name = function.GetString();
            if (name is not ("starts_with" or "ends_with" or "contains" or "list_contains"))
            {
                throw Error($"Unknown standard filter function '{name}'.");
            }

            if (options is not null)
            {
                throw Error("Standard filter functions do not accept options.");
            }

            return;
        }

        AssertObject(function, "call.function");
        Exact(function, ["namespace", "name", "version"], "call.function");
        var ns = NonemptyString(function, "namespace");
        var nameValue = NonemptyString(function, "name");
        var version = UInt(function, "version");
        if (!IdentityNamespacePattern().IsMatch(ns) || !IdentityNamePattern().IsMatch(nameValue) || version == 0)
        {
            throw Error("call.function has a noncanonical identity or version.");
        }

        if (ns != "duckdb.spatial" || nameValue != "intersects_extent" || version != 1)
        {
            throw Error($"Unknown extension filter function {ns}/{nameValue}@{version}.");
        }

        if (!extensionFunctions.Any(value => value.Namespace == ns && value.Name == nameValue && value.Version == version))
        {
            throw Error($"Extension filter function {ns}/{nameValue}@{version} was not advertised.");
        }

        if (options is { } extensionOptions)
        {
            AssertObject(extensionOptions, "call.options");
            if (extensionOptions.EnumerateObject().Any())
            {
                throw Error($"Extension filter function {ns}/{nameValue}@{version} does not accept options.");
            }
        }
    }

    private static bool IsBooleanExpression(JsonElement expression,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload, Schema outputSchema)
    {
        var node = String(expression, "node");
        if (node is "comparison" or "and" or "or" or "not" or "is_null" or "in" or "call" or "runtime_filter")
        {
            return true;
        }

        return ReferencedType(expression, payload, outputSchema)?.TypeId == ArrowTypeId.Boolean;
    }

    private static void ValidateCallBinding(JsonElement function, JsonElement arguments,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload, Schema outputSchema)
    {
        var values = arguments.EnumerateArray().ToArray();
        var left = ReferencedType(values[0], payload, outputSchema);
        var right = ReferencedType(values[1], payload, outputSchema);
        if (function.ValueKind == JsonValueKind.Object)
        {
            var leftField = ReferencedArrowField(values[0], payload, outputSchema);
            var rightField = ReferencedArrowField(values[1], payload, outputSchema);
            if (!IsWkb(leftField) || !IsWkb(rightField))
            {
                throw Error("duckdb.spatial/intersects_extent@1 requires two geoarrow.wkb arguments.");
            }

            return;
        }

        var name = function.GetString();
        if (name is "starts_with" or "ends_with" or "contains")
        {
            if (left is not StringType || right is not StringType)
            {
                throw Error($"{name} requires two UTF-8 string arguments.");
            }

            return;
        }

        if (name == "list_contains")
        {
            if (left is not ListType list || !BindCompatible(list.ValueField.DataType, right))
            {
                throw Error("list_contains requires LIST<T> and a bind-compatible T argument.");
            }

            return;
        }

        throw Error($"Unknown standard filter function '{name}'.");
    }

    private static Field? ReferencedArrowField(JsonElement expression,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload, Schema outputSchema)
    {
        return String(expression, "node") switch
        {
            "column_ref" => outputSchema.GetFieldByIndex(checked((int)UInt(expression, "column_index"))),
            "field_ref" => ReferencedField(expression, outputSchema),
            "literal" => Payload(payload, "value", UInt(expression, "value_ref")).Field,
            "cast" => Payload(payload, "type", UInt(expression, "type_ref")).Field,
            _ => null,
        };
    }

    private static bool IsWkb(Field? field) =>
        field?.DataType is BinaryType && field.Metadata is { } metadata &&
        metadata.TryGetValue("ARROW:extension:name", out var name) && name == "geoarrow.wkb";

    private static bool IsNumericType(IArrowType? type) =>
        type is Int8Type or Int16Type or Int32Type or Int64Type or
            UInt8Type or UInt16Type or UInt32Type or UInt64Type or FloatType or DoubleType ||
        type?.TypeId.ToString().StartsWith("Decimal", StringComparison.Ordinal) is true;

    private static bool BindCompatible(IArrowType? left, IArrowType? right)
    {
        left = LogicalValueType(left);
        right = LogicalValueType(right);
        return left is not null && right is not null &&
            (left.Equals(right) || IsNumericType(left) && IsNumericType(right));
    }

    private static IArrowType? LogicalValueType(IArrowType? type) =>
        type is DictionaryType dictionary ? dictionary.ValueType : type;

    private static IArrowType? ReferencedType(JsonElement expression,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload, Schema outputSchema)
    {
        return String(expression, "node") switch
        {
            "column_ref" => outputSchema.GetFieldByIndex(checked((int)UInt(expression, "column_index"))).DataType,
            "field_ref" => ReferencedField(expression, outputSchema).DataType,
            "literal" => Payload(payload, "value", UInt(expression, "value_ref")).Field.DataType,
            "cast" => Payload(payload, "type", UInt(expression, "type_ref")).Field.DataType,
            "arithmetic" or "negate" => ReferencedType(expression.GetProperty(
                String(expression, "node") == "arithmetic" ? "left" : "expression"), payload, outputSchema),
            "comparison" or "and" or "or" or "not" or "is_null" or "in" or "call" or "runtime_filter" =>
                BooleanType.Default,
            _ => null,
        };
    }

    private static bool RequiresSessionContext(JsonElement expression,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload, Schema outputSchema)
    {
        var node = String(expression, "node");
        bool Child(JsonElement value) => RequiresSessionContext(value, payload, outputSchema);
        if (node == "arithmetic")
        {
            return String(expression, "op") is "divide" or "modulo" ||
                   Child(expression.GetProperty("left")) || Child(expression.GetProperty("right"));
        }

        if (node == "cast")
        {
            var source = ReferencedType(expression.GetProperty("expression"), payload, outputSchema);
            var target = ReferencedType(expression, payload, outputSchema);
            return IsContextualType(source) && IsContextualType(target) || Child(expression.GetProperty("expression"));
        }

        return node switch
        {
            "field_ref" or "not" or "is_null" or "negate" => Child(expression.GetProperty("expression")),
            "comparison" => Child(expression.GetProperty("left")) || Child(expression.GetProperty("right")),
            "and" or "or" => expression.GetProperty("children").EnumerateArray().Any(Child),
            "in" => Child(expression.GetProperty("expression")),
            "call" => expression.GetProperty("arguments").EnumerateArray().Any(Child),
            _ => false,
        };
    }

    private static bool IsContextualType(IArrowType? type) =>
        type is StringType or LargeStringType or Date32Type or Date64Type or Time32Type or Time64Type or TimestampType;

    private static string PredicateId(JsonElement value, string where)
    {
        var id = NonemptyString(value, "id");
        if (Encoding.UTF8.GetByteCount(id) > MaxIdBytes)
        {
            throw Error($"{where}.id exceeds {MaxIdBytes} UTF-8 bytes.");
        }

        return id;
    }

    private static void ValidateArrowExtensions(Field field)
    {
        if (field.Metadata is { } metadata && metadata.TryGetValue("ARROW:extension:name", out var name) &&
            !KnownArrowExtensions.Contains(name))
        {
            throw Error($"Unknown Arrow extension type '{name}'.");
        }

        if (field.DataType is StructType structure)
        {
            foreach (var child in structure.Fields)
            {
                ValidateArrowExtensions(child);
            }
        }
        else if (field.DataType is ListType list)
        {
            ValidateArrowExtensions(list.ValueField);
        }
    }

    private static IArrowType ValidateSet(JsonElement set,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload, IReadOnlyList<RecordBatch> keys)
    {
        var kind = String(set, "kind");
        if (kind == "literal")
        {
            Exact(set, ["kind", "value_ref"], "in.set");
            var item = Payload(payload, "value", UInt(set, "value_ref"));
            if (item.Field.DataType is not ListType listType || item.Array is not ListArray || item.Array.IsNull(0))
            {
                throw Error("A literal IN payload must be a non-NULL Arrow list scalar.");
            }

            return listType.ValueField.DataType;
        }

        if (kind != "external")
        {
            throw Error("Unknown IN set kind.");
        }

        Exact(set, ["kind", "batch_index", "column_index", "column_name"], "in.set");
        var batchIndex = checked((int)UInt(set, "batch_index"));
        var columnIndex = checked((int)UInt(set, "column_index"));
        var name = NonemptyString(set, "column_name");
        if (batchIndex >= keys.Count || columnIndex >= keys[batchIndex].ColumnCount ||
            keys[batchIndex].Schema.GetFieldByIndex(columnIndex).Name != name)
        {
            throw Error("External IN coordinates do not identify the declared join-key column.");
        }

        ValidateArrowExtensions(keys[batchIndex].Schema.GetFieldByIndex(columnIndex));
        return keys[batchIndex].Schema.GetFieldByIndex(columnIndex).DataType;
    }

    private static Field ReferencedField(JsonElement expression, Schema outputSchema)
    {
        var node = String(expression, "node");
        if (node == "column_ref")
        {
            return outputSchema.GetFieldByIndex(checked((int)UInt(expression, "column_index")));
        }

        if (node == "field_ref")
        {
            var parent = ReferencedField(expression.GetProperty("expression"), outputSchema);
            if (parent.DataType is not StructType structure)
            {
                throw Error("field_ref input must resolve to a struct field.");
            }

            return structure.Fields[checked((int)UInt(expression, "field_index"))];
        }

        throw Error("DuckDB 1.5 field_ref input must be a column_ref or field_ref.");
    }

    private static void ValidateBatch(RecordBatch batch)
    {
        if (batch.Length != 1 || batch.ColumnCount == 0 ||
            batch.Schema.GetFieldByIndex(0) is not { Name: "filter_spec", IsNullable: false } first ||
            first.DataType.TypeId != ArrowTypeId.String || batch.Column(0) is not StringArray strings || strings.IsNull(0))
        {
            throw Error("Filter batch must be one row beginning with filter_spec: utf8 not null.");
        }

        var metadata = batch.Schema.Metadata ?? throw Error("Filter batch lacks schema metadata.");
        Metadata(metadata, "vgi_filter_encoding", EncodingName);
        Metadata(metadata, "vgi_filter_version", "2");
        Metadata(metadata, "vgi_evaluation_context", NoEvaluationContext);
        foreach (var key in new[]
        {
            "vgi_time_zone", "vgi_calendar", "vgi_default_collation", "vgi_ieee_floating_point_ops",
            "vgi_integer_division", "vgi_context_provider_fingerprint",
        })
        {
            if (metadata.ContainsKey(key))
            {
                throw Error($"{NoEvaluationContext} forbids session metadata '{key}'.");
            }
        }

        var names = new HashSet<string>(StringComparer.Ordinal) { "filter_spec" };
        for (var i = 1; i < batch.ColumnCount; i++)
        {
            var name = batch.Schema.GetFieldByIndex(i).Name;
            if (!names.Add(name) || !PayloadNamePattern().IsMatch(name))
            {
                throw Error($"Noncanonical or duplicate filter payload field '{name}'.");
            }

            if (name.StartsWith("type_", StringComparison.Ordinal) && !batch.Column(i).IsNull(0))
            {
                throw Error($"{name} must contain NULL.");
            }
        }
    }

    private static RecordBatch ReadSingleBatch(byte[] bytes, string where)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new ArrowStreamReader(stream);
        var batch = reader.ReadNextRecordBatch() ?? throw Error($"{where} has no RecordBatch.");
        if (reader.ReadNextRecordBatch() is not null)
        {
            throw Error($"{where} must contain exactly one RecordBatch.");
        }

        return batch;
    }

    private static (Field Field, IArrowArray Array) Payload(
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload, string prefix, ulong index)
    {
        if (!payload.TryGetValue($"{prefix}_{index}", out var result))
        {
            throw Error($"Missing {prefix}_{index} payload field.");
        }

        if (prefix != "artifact")
        {
            ValidateArrowExtensions(result.Field);
        }

        return result;
    }

    private static void Metadata(IReadOnlyDictionary<string, string> metadata, string key, string expected)
    {
        if (!metadata.TryGetValue(key, out var value) || value != expected)
        {
            throw Error($"Schema metadata '{key}' must be '{expected}'.");
        }
    }

    private static void RejectDuplicateKeys(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Object)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in value.EnumerateObject())
            {
                if (!names.Add(property.Name))
                {
                    throw Error($"Duplicate JSON property '{property.Name}'.");
                }

                RejectDuplicateKeys(property.Value);
            }
        }
        else if (value.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in value.EnumerateArray())
            {
                RejectDuplicateKeys(child);
            }
        }
    }

    private static void AssertObject(JsonElement value, string where)
    {
        if (value.ValueKind != JsonValueKind.Object)
        {
            throw Error($"{where} must be an object.");
        }

    }

    private static JsonElement Object(JsonElement parent, string name) =>
        parent.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Object
            ? value
            : throw Error($"'{name}' must be an object.");

    private static JsonElement Array(JsonElement parent, string name) =>
        parent.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Array
            ? value
            : throw Error($"'{name}' must be an array.");

    private static string String(JsonElement parent, string name) =>
        parent.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()!
            : throw Error($"'{name}' must be a string.");

    private static void String(JsonElement parent, string name, string expected)
    {
        if (String(parent, name) != expected)
        {
            throw Error($"'{name}' must be '{expected}'.");
        }
    }

    private static string NonemptyString(JsonElement parent, string name)
    {
        var value = String(parent, name);
        return value.Length != 0 ? value : throw Error($"'{name}' must not be empty.");
    }

    private static ulong UInt(JsonElement parent, string name) =>
        parent.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetUInt64(out var result)
            ? result
            : throw Error($"'{name}' must be an unsigned 64-bit integer.");

    private static bool Boolean(JsonElement parent, string name) =>
        parent.TryGetProperty(name, out var value) && value.ValueKind is JsonValueKind.True or JsonValueKind.False
            ? value.GetBoolean()
            : throw Error($"'{name}' must be a Boolean.");

    private static void Exact(JsonElement value, IReadOnlyList<string> required, string where)
    {
        var actual = value.EnumerateObject().Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
        if (required.Any(name => !actual.Contains(name)) || actual.Any(name => !required.Contains(name)))
        {
            throw Error($"{where} has missing or unknown properties.");
        }
    }

    private static InvalidDataException Error(string message) => new(message);
}

public sealed record DecodedPredicate(string Id, ulong Revision, string Mode, string Source, JsonElement Expression);

public sealed record DecodedFilterUpdate(string Operation, string Id, ulong Revision, DecodedPredicate? Predicate);

public sealed class DecodedFilters
{
    private readonly IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> _payload;
    private readonly IReadOnlyList<RecordBatch> _joinKeys;

    internal DecodedFilters(JsonElement root, string kind, IReadOnlyList<DecodedPredicate> predicates,
        IReadOnlyList<DecodedFilterUpdate> updates,
        IReadOnlyDictionary<string, (Field Field, IArrowArray Array)> payload,
        IReadOnlyList<RecordBatch> joinKeys)
    {
        Root = root;
        Kind = kind;
        Predicates = predicates;
        Updates = updates;
        _payload = payload;
        _joinKeys = joinKeys;
    }

    public JsonElement Root { get; }
    public string Kind { get; }
    public IReadOnlyList<DecodedPredicate> Predicates { get; }
    public IReadOnlyList<DecodedFilterUpdate> Updates { get; }
    public object? ValueRef(ulong index) => ScalarArgCodec.ReadScalar(Payload("value", index).Array);
    public object? ValueRef(int index) => index < 0 ? throw new ArgumentOutOfRangeException(nameof(index)) : ValueRef((ulong)index);
    public Field ValueField(ulong index) => Payload("value", index).Field;
    public Field ValueField(int index) => index < 0 ? throw new ArgumentOutOfRangeException(nameof(index)) : ValueField((ulong)index);

    public IReadOnlyList<object?> LiteralSet(ulong index)
    {
        var list = Payload("value", index).Array as ListArray ?? throw new InvalidDataException("IN payload is not a list.");
        var offsets = list.ValueOffsets;
        return Enumerable.Range(offsets[0], offsets[1] - offsets[0])
            .Select(i => ScalarArgCodec.ReadScalar(list.Values, i)).ToList();
    }

    public IReadOnlyList<object?> ExternalSet(int batchIndex, int columnIndex, string columnName)
    {
        if (batchIndex < 0 || batchIndex >= _joinKeys.Count || columnIndex < 0 ||
            columnIndex >= _joinKeys[batchIndex].ColumnCount ||
            _joinKeys[batchIndex].Schema.GetFieldByIndex(columnIndex).Name != columnName)
        {
            throw new InvalidDataException("External IN coordinates do not match a join-key column.");
        }

        var array = _joinKeys[batchIndex].Column(columnIndex);
        return Enumerable.Range(0, array.Length).Select(i => ScalarArgCodec.ReadScalar(array, i)).ToList();
    }

    public IReadOnlyList<object?> JoinKeyValues(string columnName)
    {
        for (var batchIndex = 0; batchIndex < _joinKeys.Count; batchIndex++)
        {
            for (var columnIndex = 0; columnIndex < _joinKeys[batchIndex].ColumnCount; columnIndex++)
            {
                if (_joinKeys[batchIndex].Schema.GetFieldByIndex(columnIndex).Name == columnName)
                {
                    return ExternalSet(batchIndex, columnIndex, columnName);
                }
            }
        }

        throw new InvalidDataException($"No join-key column named '{columnName}'.");
    }

    private (Field Field, IArrowArray Array) Payload(string prefix, ulong index) =>
        _payload.TryGetValue($"{prefix}_{index}", out var item)
            ? item
            : throw new InvalidDataException($"Missing {prefix}_{index} payload field.");
}
