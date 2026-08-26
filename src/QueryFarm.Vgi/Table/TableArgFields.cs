using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Table;

/// <summary>Small convenience factories for <see cref="ITableFunction.ArgumentsSchema"/> fields —
/// every table-function fixture needs a positional-vs-named field one way or another, so this
/// avoids each one hand-rolling the <c>vgi_arg=named</c> metadata dictionary (see
/// <see cref="VgiWireMetadata"/>).</summary>
public static class TableArgFields
{
    private static readonly IReadOnlyDictionary<string, string> NamedMetadata =
        new Dictionary<string, string> { [VgiWireMetadata.ArgKey] = VgiWireMetadata.ArgNamedValue };

    public static Field Positional(string name, IArrowType type, bool nullable = true) =>
        new(name, type, nullable);

    /// <summary>A positional field declaring a numeric range constraint (agent discovery via
    /// <c>vgi_function_arguments()</c>'s <c>arg_range</c> column) — each bound is
    /// <see cref="double.NaN"/> when unset (see <see cref="Attributes.ConstParamAttribute"/>'s
    /// identically-shaped bounds). Surfacing the constraint is purely declarative; a caller that
    /// also wants BIND-TIME enforcement (e.g. rejecting a negative count) still checks it itself.</summary>
    public static Field PositionalWithRange(
        string name, IArrowType type, double ge = double.NaN, double gt = double.NaN,
        double le = double.NaN, double lt = double.NaN, bool nullable = true)
    {
        var range = VgiWireMetadata.BuildRange(ge, gt, le, lt);
        return new Field(name, type, nullable, range is null ? null : new Dictionary<string, string> { [VgiWireMetadata.RangeKey] = range });
    }

    public static Field Named(string name, IArrowType type, bool nullable = true) =>
        new(name, type, nullable, NamedMetadata);

    /// <summary>A named field carrying human-readable documentation (<c>vgi_doc</c> metadata,
    /// e.g. a COPY TO/FROM format's <c>option_description</c>) alongside the ordinary
    /// <c>vgi_arg=named</c> marker.</summary>
    public static Field NamedWithDoc(string name, IArrowType type, string doc, bool nullable = true) => new(
        name,
        type,
        nullable,
        new Dictionary<string, string>
        {
            [VgiWireMetadata.ArgKey] = VgiWireMetadata.ArgNamedValue,
            [VgiWireMetadata.DocKey] = doc,
        });

    /// <summary>An ANY-typed varargs field (e.g. <c>constant_columns</c>'s trailing arguments) —
    /// <c>vgi_type=any</c> + <c>vgi_varargs=true</c> metadata.</summary>
    public static Field AnyVarargs(string name) => new(
        name,
        NullType.Default,
        nullable: true,
        new Dictionary<string, string>
        {
            [VgiWireMetadata.TypeKey] = VgiWireMetadata.TypeAnyValue,
            [VgiWireMetadata.VarargsKey] = VgiWireMetadata.VarargsTrueValue,
        });

    /// <summary>A TYPED varargs field (e.g. a blended <c>row_sum(v1, v2, ...)</c> function's
    /// per-row value columns) — <c>vgi_varargs=true</c> metadata on a field of the DECLARED type,
    /// unlike <see cref="AnyVarargs"/>'s <c>vgi_type=any</c> sentinel: every vararg at a call site
    /// must resolve to (or implicitly cast to) exactly <paramref name="type"/>.</summary>
    public static Field TypedVarargs(string name, IArrowType type, bool nullable = true) => new(
        name,
        type,
        nullable,
        new Dictionary<string, string> { [VgiWireMetadata.VarargsKey] = VgiWireMetadata.VarargsTrueValue });

    /// <summary>The TABLE-typed argument a table-in-out/table-buffering function's
    /// <see cref="ITableFunction.ArgumentsSchema"/>-equivalent declares (<c>vgi_type=table</c>
    /// metadata) — the field's own Arrow TYPE is irrelevant and never round-trips (the C++ side
    /// unconditionally overrides it to <c>LogicalType::TABLE</c> once this marker is seen, per
    /// <c>vgi_arrow_utils.cpp</c>'s <c>BuildArgumentSpecs</c>), only its NAME and POSITION matter
    /// (the name becomes the table-input's registered arg name; the position is excluded from the
    /// "positional_N" renumbering <see cref="Internal.TableArgCodec"/> reads back at bind — the
    /// C++ side skips the TABLE slot entirely when building <c>BindRequest.Arguments</c>, so the
    /// SURVIVING positional args are renumbered contiguously starting at 0 in their original
    /// relative order).</summary>
    public static Field Table(string name) => new(
        name,
        NullType.Default,
        nullable: true,
        new Dictionary<string, string> { [VgiWireMetadata.TypeKey] = VgiWireMetadata.TypeTableValue });
}
