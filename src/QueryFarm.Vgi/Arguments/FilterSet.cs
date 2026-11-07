using System.Text.Json;
using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Arguments;

/// <summary>One pushed-down predicate. <see cref="Mode"/> is <c>"exact"</c> (DuckDB relies on the
/// worker applying it) or <c>"advisory"</c> (DuckDB re-checks; applying it is an optimization).
/// <see cref="Expression"/> is the <c>vgi.filters.v2</c> JSON expression tree.</summary>
public sealed record FilterPredicate(string Id, ulong Revision, string Mode, string Source, JsonElement Expression);

/// <summary>
/// The filters DuckDB pushed down to a scan (<c>vgi.filters.v2</c>), decoded. Available as
/// <see cref="Table.TableInitParams.Filters"/> (and
/// <see cref="Buffering.TableBufferingFinalizeParams.Filters"/>) when the function advertises
/// <c>FilterPushdown</c>. The simplest use is row-at-a-time: skip rows for which
/// <see cref="Matches(IReadOnlyDictionary{string, object?})"/> is <see langword="false"/>. Code that
/// translates filters into a remote query walks <see cref="Predicates"/> instead.
/// </summary>
/// <example>
/// <code>
/// var filters = initParams.Filters;          // null when nothing was pushed down
/// for (var i = 0; i &lt; batch.Length; i++)
/// {
///     if (filters is null || filters.Matches(batch, i)) { /* keep row i */ }
/// }
/// </code>
/// </example>
public sealed class FilterSet
{
    private readonly DecodedFilters _decoded;
    private IReadOnlyList<FilterPredicate>? _predicates;

    private FilterSet(DecodedFilters decoded) => _decoded = decoded;

    /// <summary>Decodes raw pushdown-filter bytes. <paramref name="outputSchema"/> is the function's
    /// unprojected output schema (column references are validated against it).
    /// <see langword="null"/> when <paramref name="pushdownFilters"/> is null/empty.</summary>
    public static FilterSet? Decode(byte[]? pushdownFilters, IReadOnlyList<byte[]>? joinKeys, Schema outputSchema,
        IReadOnlyList<FilterFunctionCapability>? extensionFunctions = null) =>
        PushdownFilterCodec.Decode(pushdownFilters, joinKeys, outputSchema, extensionFunctions) is { } decoded
            ? new FilterSet(decoded)
            : null;

    /// <summary><c>"snapshot"</c> for an init-time filter set; <c>"delta"</c> for a dynamic update.</summary>
    public string Kind => _decoded.Kind;

    /// <summary>The whole decoded JSON document.</summary>
    public JsonElement Root => _decoded.Root;

    /// <summary>The predicates, implicitly AND-ed together.</summary>
    public IReadOnlyList<FilterPredicate> Predicates =>
        _predicates ??= _decoded.Predicates
            .Select(p => new FilterPredicate(p.Id, p.Revision, p.Mode, p.Source, p.Expression))
            .ToList();

    /// <summary>Evaluates every predicate against one row given as column name → boxed value
    /// (<see langword="null"/> for SQL NULL). Advisory predicates that cannot be evaluated pass.</summary>
    public bool Matches(IReadOnlyDictionary<string, object?> row) => PushdownFilterEvaluator.Matches(_decoded, row);

    /// <summary>Evaluates every predicate against row <paramref name="rowIndex"/> of
    /// <paramref name="batch"/>, reading columns by name.</summary>
    public bool Matches(RecordBatch batch, int rowIndex)
    {
        ArgumentNullException.ThrowIfNull(batch);
        var row = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (var c = 0; c < batch.ColumnCount; c++)
        {
            try
            {
                row[batch.Schema.GetFieldByIndex(c).Name] = ScalarArgCodec.ReadScalar(batch.Column(c), rowIndex);
            }
            catch (NotSupportedException)
            {
                // Column type the row evaluator cannot read — left out; a predicate that references
                // it fails with a "Row has no filter column" error instead of a silent mismatch.
            }
        }

        return Matches(row);
    }

    /// <summary>The literal referenced by an expression's <c>value_ref</c>.</summary>
    public object? ValueRef(int index) => _decoded.ValueRef(index);

    /// <summary>The literal list referenced by an <c>in</c> expression's <c>value_ref</c>.</summary>
    public IReadOnlyList<object?> LiteralSet(int index) =>
        index < 0 ? throw new ArgumentOutOfRangeException(nameof(index)) : _decoded.LiteralSet((ulong)index);

    /// <summary>The join-key values DuckDB shipped for column <paramref name="columnName"/>.</summary>
    public IReadOnlyList<object?> JoinKeyValues(string columnName) => _decoded.JoinKeyValues(columnName);

    /// <summary>The underlying decoder output, for interop with lower-level helpers.</summary>
    internal DecodedFilters Decoded => _decoded;
}
