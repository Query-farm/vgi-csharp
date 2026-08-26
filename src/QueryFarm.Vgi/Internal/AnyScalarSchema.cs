using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Types;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Builds the schema shapes an ANY-typed scalar argument/output uses on the wire: a single field
/// of <see cref="NullType"/> carrying <c>vgi_type=any</c> metadata — the C++ side
/// (<c>vgi_arrow_utils.cpp</c>'s <c>BuildArgumentSpecs</c>) maps that metadata to DuckDB's
/// <c>LogicalType::ANY</c> regardless of the field's nominal Arrow type, for both argument and
/// output-schema fields. Used by the dynamic-output-type fixtures (<c>add_values</c>,
/// <c>double</c>, <c>sum_values</c>) that implement <see cref="Scalar.IScalarFunction"/> directly
/// rather than through <see cref="Scalar.ScalarFn"/>'s reflection (which only derives fixed,
/// statically-typed schemas).
/// </summary>
public static class AnyScalarSchema
{
    private static readonly Dictionary<string, string> AnyMetadata = new() { [VgiWireMetadata.TypeKey] = VgiWireMetadata.TypeAnyValue };

    private static readonly Dictionary<string, string> AnyVarargsMetadata = new()
    {
        [VgiWireMetadata.TypeKey] = VgiWireMetadata.TypeAnyValue,
        [VgiWireMetadata.VarargsKey] = VgiWireMetadata.VarargsTrueValue,
    };

    public static Field AnyField(string name) => new(name, NullType.Default, nullable: true, AnyMetadata);

    /// <summary>A single field marking "consume every remaining positional column, any type" —
    /// the wire shape a fully dynamic (ANY-typed) varargs parameter uses (<c>sum_values</c>).</summary>
    public static Field AnyVarargsField(string name) => new(name, NullType.Default, nullable: true, AnyVarargsMetadata);

    public static Schema SingleArg(string name) => new([AnyField(name)], metadata: null);

    public static Schema Varargs(string name) => new([AnyVarargsField(name)], metadata: null);

    public static Schema SingleResult() => new([AnyField("result")], metadata: null);

    /// <summary>Rejects a non-addable (non-numeric, non-decimal) input type at bind time — mirrors
    /// vgi-java's temporal/bool/string/binary rejection for <c>double</c>/<c>add_values</c>/
    /// <c>sum_values</c>'s <c>IS_ADDABLE</c> type bound. The literal substring
    /// <c>_is_multipliable_type</c> is asserted verbatim by <c>double.test</c>'s regression cases.</summary>
    public static void RequireAddable(string functionName, IArrowType type)
    {
        if (!TypeRules.IsAddable(type))
        {
            throw new InvalidOperationException($"{functionName}: _is_multipliable_type rejects type '{type}'.");
        }
    }
}
