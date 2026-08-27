using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using DuckDB.NET.Data;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Evaluates a <see cref="DecodedFilters"/> tree — including genuine <c>"expression"</c> nodes
/// (arbitrary function-call/spatial predicates DuckDB pushed down, per
/// <c>Table.ITableFunction.SupportedExpressionFilters</c>) — against a whole <see cref="RecordBatch"/>
/// at once, returning a boolean keep-mask.
///
/// <para><b>Why an embedded DuckDB engine.</b> An <c>"expression"</c> node is a recursive bound
/// expression tree (<c>column_ref</c>/<c>constant</c>/<c>function</c>/<c>comparison</c>/<c>conjunction</c>
/// — see <c>SerializeExpression</c> in <c>~/Development/vgi/src/vgi_table_function_impl.cpp</c>)
/// naming an arbitrary DuckDB function by string (<c>list_contains</c>, <c>starts_with</c>,
/// <c>&amp;&amp;</c>, <c>st_intersects_extent</c>, ...). Reimplementing each such function in C# would
/// mean re-deriving DuckDB/spatial semantics function-by-function and staying in sync forever.
/// vgi-python (<c>vgi/table_filter_pushdown.py</c>'s <c>ExpressionFilter.evaluate</c>) and vgi-go
/// (<c>vgi/expression_filter.go</c>) both instead render the tree back to SQL text and hand it to a
/// real embedded DuckDB connection — delegating ALL function semantics (including spatial ones, via
/// the <c>spatial</c> extension) to DuckDB itself. This mirrors that architecture using
/// <c>DuckDB.NET.Data.Full</c>.</para>
///
/// <para><b>Per-node, not per-batch.</b> Every node in <see cref="DecodedFilters"/>'s top-level array
/// — and every node inside an <c>"and"</c>/<c>"or"</c>/<c>"expression"</c> subtree — is anchored to
/// exactly ONE column (<c>SerializeFilterInto</c> in the C++ extension always propagates the same
/// <c>column_name</c>/<c>column_index</c> down through conjunction children, and <c>column_ref</c>
/// nodes inside an expression tree always resolve to that same anchor column — v1 has no
/// multi-column expression filters). So each top-level node is evaluated independently: its one
/// anchor column is loaded into a single DuckDB list parameter (<c>UNNEST($1)</c>), the node's
/// subtree is rendered to a SQL boolean expression referencing that unnested value, and the
/// per-node boolean results are ANDed together row-wise across all top-level nodes.</para>
///
/// <para><b>Constants are bound as real parameters</b>, not inlined SQL literals (unlike
/// vgi-python's <c>_arrow_scalar_to_sql</c>) — DuckDB.NET supports typed positional parameters
/// natively, so there's no literal-formatting/escaping surface to get wrong for strings, floats, or
/// binary/WKB values.</para>
///
/// <para><b>Spatial (WKB) columns/constants</b> are raw <c>binary</c> Arrow data on the wire (see
/// <c>ExampleWorker.Table.SpatialFilterExampleFunction</c>'s <c>geoarrow.wkb</c>-tagged <c>geom</c>
/// field); this evaluator does not use DuckDB's Arrow-extension-aware ingestion (it never loads a
/// whole Arrow batch into DuckDB — only per-node <c>List&lt;T&gt;</c> parameters), so it detects
/// <c>ARROW:extension:name=geoarrow.wkb</c> metadata itself (on the anchor column's
/// <see cref="Field"/> and, via <see cref="DecodedFilters.ValueField"/>, on each constant's field —
/// the latter set by the C++ extension's <c>ArrowTypeExtensionData::GetExtensionTypes</c> when a
/// pushed constant is itself a spatial <c>GEOMETRY</c> value, e.g. a constant-folded
/// <c>ST_MakeEnvelope(...)</c>) and wraps the corresponding SQL reference in
/// <c>ST_GeomFromWKB(...)</c>.</para>
/// </summary>
public static class ExpressionFilterEvaluator
{
    /// <summary>A cached, best-effort spatial-loaded DuckDB connection, created lazily (so a worker
    /// that never uses expression-filter pushdown never pays engine-startup cost) and per-thread —
    /// matching vgi-python's <c>_get_expression_eval_connection</c> exactly, and for the same reason:
    /// a <see cref="DuckDBConnection"/> is not thread-safe, and this evaluator makes no assumption
    /// about whether the C++ extension's RPC dispatch ever runs two producers concurrently on
    /// different threads within one worker process.</summary>
    [ThreadStatic]
    private static DuckDBConnection? s_connection;

    /// <summary>Evaluates <paramref name="filters"/> against every row of <paramref name="batch"/>,
    /// returning a keep-mask (<see langword="true"/> = row passes every top-level filter node).
    /// <paramref name="schema"/> is the batch's own schema (for resolving a node's anchor column by
    /// name and inspecting its WKB metadata) — pass <see langword="null"/> filters for "no filters,
    /// keep everything".</summary>
    public static bool[] EvaluateMask(DecodedFilters? filters, RecordBatch batch, Schema schema)
    {
        var mask = new bool[batch.Length];
        System.Array.Fill(mask, true);
        if (filters is null || filters.Root.ValueKind != JsonValueKind.Array)
        {
            return mask;
        }

        foreach (var node in filters.Root.EnumerateArray())
        {
            var nodeMask = EvaluateTopLevelNode(node, filters, batch, schema);
            for (var i = 0; i < mask.Length; i++)
            {
                mask[i] &= nodeMask[i];
            }
        }

        return mask;
    }

    private static bool[] EvaluateTopLevelNode(JsonElement node, DecodedFilters filters, RecordBatch batch, Schema schema)
    {
        var columnName = ColumnName(node);
        var fieldIndex = schema.GetFieldIndex(columnName);
        if (fieldIndex < 0)
        {
            throw new InvalidOperationException(
                $"Pushdown filter referenced unknown column '{columnName}' — not present in the emitted batch's schema.");
        }

        var field = schema.GetFieldByIndex(fieldIndex);
        var isWkbColumn = IsWkb(field);
        var parameters = new List<object?> { ToListParameter(batch.Column(fieldIndex)) };
        var columnSql = isWkbColumn ? "ST_GeomFromWKB(\"_col\")" : "\"_col\"";
        var sql = RenderNode(node, filters, columnSql, parameters);

        return RunMask(sql, parameters, batch.Length);
    }

    private static bool[] RunMask(string sql, List<object?> parameters, int rowCount)
    {
        using var cmd = GetConnection().CreateCommand();
        cmd.CommandText = $"SELECT ({sql})::BOOLEAN AS r FROM (SELECT UNNEST($1) AS \"_col\")";
        for (var i = 0; i < parameters.Count; i++)
        {
            var p = cmd.CreateParameter();
            p.Value = parameters[i] ?? DBNull.Value;
            cmd.Parameters.Add(p);
        }

        var result = new bool[rowCount];
        using var reader = cmd.ExecuteReader();
        var idx = 0;
        while (reader.Read())
        {
            result[idx++] = !reader.IsDBNull(0) && reader.GetBoolean(0);
        }

        if (idx != rowCount)
        {
            throw new InvalidOperationException(
                $"Expression-filter evaluation returned {idx} rows for a {rowCount}-row batch (expected exactly one boolean per row).");
        }

        return result;
    }

    /// <summary>Rebuilds <paramref name="batch"/> keeping only the rows where <paramref name="mask"/>
    /// is <see langword="true"/> — the companion to <see cref="EvaluateMask"/>: a producer calls this
    /// once on the batch it already built to apply the computed keep-mask before emitting. Covers
    /// the Arrow column types this repo's expression-filter-pushdown fixtures actually emit; add a
    /// case to <see cref="FilterColumn"/> before using this on a batch with some other column type.</summary>
    public static RecordBatch ApplyMask(RecordBatch batch, bool[] mask)
    {
        var keptRows = mask.Count(m => m);
        var columns = Enumerable.Range(0, batch.Schema.FieldsList.Count)
            .Select(i => FilterColumn(batch.Column(i), mask))
            .ToList();
        return new RecordBatch(batch.Schema, columns, keptRows);
    }

    private static IArrowArray FilterColumn(IArrowArray array, bool[] mask) => array switch
    {
        Int64Array a => FilterPrimitive<long, Int64Array, Int64Array.Builder>(a, mask, new Int64Array.Builder(), (b, v) => b.Append(v)),
        Int32Array a => FilterPrimitive<int, Int32Array, Int32Array.Builder>(a, mask, new Int32Array.Builder(), (b, v) => b.Append(v)),
        DoubleArray a => FilterPrimitive<double, DoubleArray, DoubleArray.Builder>(a, mask, new DoubleArray.Builder(), (b, v) => b.Append(v)),
        FloatArray a => FilterPrimitive<float, FloatArray, FloatArray.Builder>(a, mask, new FloatArray.Builder(), (b, v) => b.Append(v)),
        BooleanArray a => FilterBooleans(a, mask),
        StringArray a => FilterStrings(a, mask),
        BinaryArray a => FilterBinary(a, mask),
        ListArray a when a.Values is StringArray sv => FilterStringListArray(a, sv, mask),
        _ => throw new NotSupportedException(
            $"ExpressionFilterEvaluator.ApplyMask has no filter implementation for Arrow array type '{array.GetType().Name}' — add a case to FilterColumn."),
    };

    /// <summary>Filters any fixed-width Arrow array (<c>PrimitiveArray&lt;T&gt;</c>, whose
    /// <see cref="PrimitiveArray{T}.GetValue"/> already returns a nullable <c>T?</c>) by
    /// re-appending kept rows — <c>Builder.Append(T? value)</c> handles nulls itself.</summary>
    private static TArray FilterPrimitive<T, TArray, TBuilder>(PrimitiveArray<T> array, bool[] mask, TBuilder builder, Func<TBuilder, T?, TBuilder> append)
        where T : struct, IEquatable<T>
        where TArray : IArrowArray
        where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>
    {
        for (var i = 0; i < array.Length; i++)
        {
            if (mask[i])
            {
                append(builder, array.GetValue(i));
            }
        }

        return builder.Build();
    }

    private static BooleanArray FilterBooleans(BooleanArray array, bool[] mask)
    {
        var builder = new BooleanArray.Builder();
        for (var i = 0; i < array.Length; i++)
        {
            if (!mask[i])
            {
                continue;
            }

            if (array.IsNull(i))
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(array.GetValue(i)!.Value);
            }
        }

        return builder.Build();
    }

    private static StringArray FilterStrings(StringArray array, bool[] mask)
    {
        var builder = new StringArray.Builder();
        for (var i = 0; i < array.Length; i++)
        {
            if (!mask[i])
            {
                continue;
            }

            if (array.IsNull(i))
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(array.GetString(i));
            }
        }

        return builder.Build();
    }

    private static BinaryArray FilterBinary(BinaryArray array, bool[] mask)
    {
        var builder = new BinaryArray.Builder();
        for (var i = 0; i < array.Length; i++)
        {
            if (!mask[i])
            {
                continue;
            }

            if (array.IsNull(i))
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(array.GetBytes(i));
            }
        }

        return builder.Build();
    }

    private static ListArray FilterStringListArray(ListArray array, StringArray values, bool[] mask)
    {
        var builder = new ListArray.Builder(StringType.Default);
        var valueBuilder = (StringArray.Builder)builder.ValueBuilder;
        var offsets = array.ValueOffsets;
        for (var i = 0; i < array.Length; i++)
        {
            if (!mask[i])
            {
                continue;
            }

            if (array.IsNull(i))
            {
                builder.AppendNull();
                continue;
            }

            builder.Append();
            for (var j = offsets[i]; j < offsets[i + 1]; j++)
            {
                if (values.IsNull(j))
                {
                    valueBuilder.AppendNull();
                }
                else
                {
                    valueBuilder.Append(values.GetString(j));
                }
            }
        }

        return builder.Build();
    }

    // -------------------------------------------------------------------
    // Filter-node rendering — constant / is_null / is_not_null / and / or /
    // in / join_keys / expression (the same node vocabulary
    // PushdownFilterEvaluator.Eval handles, plus "expression").
    // -------------------------------------------------------------------

    private static string RenderNode(JsonElement node, DecodedFilters filters, string columnSql, List<object?> parameters)
    {
        var type = node.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : null;
        return type switch
        {
            "constant" => RenderConstantFilter(node, filters, columnSql, parameters),
            "is_null" => $"({columnSql} IS NULL)",
            "is_not_null" => $"({columnSql} IS NOT NULL)",
            "and" => Join(" AND ", Children(node), filters, columnSql, parameters),
            "or" => Join(" OR ", Children(node), filters, columnSql, parameters),
            "in" or "in_list" => RenderInFilter(node, filters, columnSql, parameters),
            "join_keys" => RenderJoinKeysFilter(node, filters, columnSql, parameters),
            "expression" => RenderExpr(node.GetProperty("expr"), filters, columnSql, parameters),
            _ => throw new NotSupportedException($"ExpressionFilterEvaluator cannot render pushdown filter node type '{type}'."),
        };
    }

    private static string Join(string joiner, IEnumerable<JsonElement> children, DecodedFilters filters, string columnSql, List<object?> parameters) =>
        "(" + string.Join(joiner, children.Select(c => RenderNode(c, filters, columnSql, parameters))) + ")";

    private static string RenderConstantFilter(JsonElement node, DecodedFilters filters, string columnSql, List<object?> parameters)
    {
        var op = node.TryGetProperty("op", out var opProp) ? opProp.GetString() : "eq";
        var valueRef = node.GetProperty("value_ref").GetInt32();
        var placeholder = AddScalarParam(parameters, filters.ValueRef(valueRef), IsWkb(filters.ValueField(valueRef)));
        return $"({columnSql} {SqlComparisonOp(op)} {placeholder})";
    }

    private static string RenderInFilter(JsonElement node, DecodedFilters filters, string columnSql, List<object?> parameters)
    {
        var refs = node.TryGetProperty("value_refs", out var r1) && r1.ValueKind == JsonValueKind.Array
            ? r1.EnumerateArray()
            : node.TryGetProperty("values", out var r2) && r2.ValueKind == JsonValueKind.Array
                ? r2.EnumerateArray()
                : [];
        var indices = refs.Select(r => r.GetInt32()).ToList();
        var values = indices.Select(filters.ValueRef).ToList();
        var placeholder = AddTypedListParam(parameters, values);
        return $"({columnSql} = ANY({placeholder}))";
    }

    private static string RenderJoinKeysFilter(JsonElement node, DecodedFilters filters, string columnSql, List<object?> parameters)
    {
        var keysColumn = node.TryGetProperty("keys_column", out var kc) ? kc.GetString() ?? "" : "";
        var values = filters.JoinKeyValues(keysColumn);
        var placeholder = AddTypedListParam(parameters, values);
        return $"({columnSql} = ANY({placeholder}))";
    }

    // -------------------------------------------------------------------
    // Bound-expression-tree rendering ("expression" node's "expr" subtree) —
    // mirrors vgi-python's ExpressionNode.to_sql / vgi-go's exprNode.toSQL.
    // -------------------------------------------------------------------

    private static string RenderExpr(JsonElement expr, DecodedFilters filters, string columnSql, List<object?> parameters)
    {
        var exprType = expr.TryGetProperty("expr_type", out var etProp) ? etProp.GetString() : null;
        switch (exprType)
        {
            case "column_ref":
                // v1: every column_ref in an expression filter refers to the same single anchor
                // column (see this class's doc comment) — the "index" field is unused here, matching
                // vgi-python's ColumnRefNode.to_sql.
                return columnSql;
            case "constant":
                var valueRef = expr.GetProperty("value_ref").GetInt32();
                return AddScalarParam(parameters, filters.ValueRef(valueRef), IsWkb(filters.ValueField(valueRef)));
            case "function":
                var functionName = expr.GetProperty("function_name").GetString() ?? throw new InvalidOperationException("expression function node missing function_name");
                var args = expr.GetProperty("children").EnumerateArray()
                    .Select(c => RenderExpr(c, filters, columnSql, parameters)).ToList();
                if (IsOperatorName(functionName) && args.Count == 2)
                {
                    return $"({args[0]} {functionName} {args[1]})";
                }

                return $"{functionName}({string.Join(", ", args)})";
            case "comparison":
                var op = expr.TryGetProperty("op", out var opProp) ? opProp.GetString() : "eq";
                var left = RenderExpr(expr.GetProperty("left"), filters, columnSql, parameters);
                var right = RenderExpr(expr.GetProperty("right"), filters, columnSql, parameters);
                return $"({left} {SqlComparisonOp(op)} {right})";
            case "conjunction":
                var conjunctionType = expr.TryGetProperty("conjunction_type", out var ctProp) ? ctProp.GetString() : "and";
                var joiner = conjunctionType == "or" ? " OR " : " AND ";
                var parts = expr.GetProperty("children").EnumerateArray().Select(c => RenderExpr(c, filters, columnSql, parameters));
                return "(" + string.Join(joiner, parts) + ")";
            default:
                throw new NotSupportedException($"ExpressionFilterEvaluator cannot render expression node type '{exprType}'.");
        }
    }

    /// <summary>Matches vgi-python's <c>_is_operator_name</c>: true for a symbolic infix operator
    /// like <c>&amp;&amp;</c> (every character non-alphanumeric/non-underscore), false for a plain
    /// function name like <c>list_contains</c>.</summary>
    private static bool IsOperatorName(string name) =>
        name.Length > 0 && name.All(c => !char.IsLetterOrDigit(c) && c != '_');

    private static string SqlComparisonOp(string? op) => op switch
    {
        "eq" => "=",
        "ne" => "!=",
        "gt" => ">",
        "ge" => ">=",
        "lt" => "<",
        "le" => "<=",
        _ => throw new NotSupportedException($"ExpressionFilterEvaluator cannot render comparison op '{op}'."),
    };

    private static IEnumerable<JsonElement> Children(JsonElement node) =>
        node.TryGetProperty("children", out var children) && children.ValueKind == JsonValueKind.Array
            ? children.EnumerateArray()
            : [];

    private static string ColumnName(JsonElement node) =>
        node.TryGetProperty("column_name", out var name) ? name.GetString() ?? "" : "";

    private static bool IsWkb(Field? field) =>
        field?.Metadata is { } metadata &&
        metadata.TryGetValue("ARROW:extension:name", out var name) &&
        name == "geoarrow.wkb";

    // -------------------------------------------------------------------
    // Parameter helpers — every DuckDB.NET parameter value here is a
    // statically/uniformly typed scalar or List<T?> (never a boxed
    // List<object?>, which segfaults the native binding on a mixed/null
    // element — verified empirically against DuckDB.NET.Data.Full 1.5.5).
    // -------------------------------------------------------------------

    private static string AddScalarParam(List<object?> parameters, object? value, bool isWkb)
    {
        parameters.Add(value);
        var placeholder = $"${parameters.Count}";
        return isWkb ? $"ST_GeomFromWKB({placeholder})" : placeholder;
    }

    private static string AddTypedListParam(List<object?> parameters, IReadOnlyList<object?> values)
    {
        parameters.Add(MakeTypedList(values));
        return $"${parameters.Count}";
    }

    /// <summary>Builds a properly, uniformly typed <c>List&lt;T?&gt;</c> from a homogeneous set of
    /// boxed CLR scalars (as produced by <see cref="ScalarArgCodec.ReadScalar"/>), inferring T from
    /// the first non-null element. Defaults to <c>List&lt;string?&gt;</c> when every value is null
    /// (an empty/all-null IN-list still needs SOME concrete element type to bind).</summary>
    private static object MakeTypedList(IReadOnlyList<object?> values)
    {
        var sample = values.FirstOrDefault(v => v is not null);
        return sample switch
        {
            long => values.Select(v => (long?)v).ToList(),
            int i => values.Select(v => (long?)(int?)v).ToList(),
            double => values.Select(v => (double?)v).ToList(),
            float => values.Select(v => (double?)(float?)v).ToList(),
            bool => values.Select(v => (bool?)v).ToList(),
            byte[] => values.Select(v => (byte[]?)v).ToList(),
            _ => values.Select(v => (string?)v?.ToString()).ToList(),
        };
    }

    /// <summary>Converts one Arrow column into a properly, uniformly typed <c>List&lt;T?&gt;</c> for
    /// binding as a single DuckDB list parameter (<c>UNNEST($1)</c>) — the per-row values of the
    /// filter's anchor column, in row order. Covers the Arrow types this repo's fixtures actually
    /// emit on a pushdown-filterable column; add a case here before declaring
    /// <see cref="Table.ITableFunction.SupportedExpressionFilters"/> on a function whose anchor
    /// column uses some other Arrow type.</summary>
    private static object ToListParameter(IArrowArray array) => array switch
    {
        Int64Array a => Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? (long?)null : a.GetValue(i)).ToList(),
        Int32Array a => Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? (long?)null : a.GetValue(i)).ToList(),
        DoubleArray a => Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? (double?)null : a.GetValue(i)).ToList(),
        FloatArray a => Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? (double?)null : (double?)a.GetValue(i)).ToList(),
        BooleanArray a => Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? (bool?)null : a.GetValue(i)).ToList(),
        StringArray a => Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? null : a.GetString(i)).ToList(),
        BinaryArray a => Enumerable.Range(0, a.Length).Select(i => a.IsNull(i) ? null : a.GetBytes(i).ToArray()).ToList(),
        ListArray a when a.Values is StringArray sv => ToStringListOfLists(a, sv),
        _ => throw new NotSupportedException(
            $"ExpressionFilterEvaluator has no DuckDB parameter conversion for Arrow array type '{array.GetType().Name}' — " +
            "add a case to ToListParameter."),
    };

    private static object ToStringListOfLists(ListArray array, StringArray values)
    {
        var result = new List<List<string?>?>(array.Length);
        var offsets = array.ValueOffsets;
        for (var i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i))
            {
                result.Add(null);
                continue;
            }

            var inner = new List<string?>(offsets[i + 1] - offsets[i]);
            for (var j = offsets[i]; j < offsets[i + 1]; j++)
            {
                inner.Add(values.IsNull(j) ? null : values.GetString(j));
            }

            result.Add(inner);
        }

        return result;
    }

    private static DuckDBConnection GetConnection()
    {
        if (s_connection is not null)
        {
            return s_connection;
        }

        var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        try
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "LOAD spatial;";
            cmd.ExecuteNonQuery();
        }
        catch
        {
            try
            {
                using var install = conn.CreateCommand();
                install.CommandText = "INSTALL spatial; LOAD spatial;";
                install.ExecuteNonQuery();
            }
            catch
            {
                // spatial not available in this environment — non-spatial expression filters
                // (list_contains, starts_with, contains, ...) still work fine without it.
            }
        }

        s_connection = conn;
        return conn;
    }
}
