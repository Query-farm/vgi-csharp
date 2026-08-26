namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Arrow field-metadata keys/values the C++ extension inspects when parsing an argument schema
/// (<c>vgi_arrow_utils.cpp</c>'s <c>BuildArgumentSpecs</c>) — kept byte-for-byte in sync with
/// <c>vgi_protocol_constants.hpp</c>. Any argument schema this worker advertises (via
/// <see cref="Scalar.ScalarFn"/>'s reflection or a hand-rolled <see cref="Scalar.IScalarFunction"/>)
/// must use these exact key/value strings for const/varargs/any-typed fields to be recognized.
/// </summary>
public static class VgiWireMetadata
{
    public const string ArgKey = "vgi_arg";
    public const string ArgNamedValue = "named";

    public const string TypeKey = "vgi_type";
    public const string TypeTableValue = "table";
    public const string TypeAnyValue = "any";

    public const string VarargsKey = "vgi_varargs";
    public const string VarargsTrueValue = "true";

    public const string ConstKey = "vgi_const";
    public const string ConstTrueValue = "true";

    /// <summary>Presence-only human-readable documentation for an argument/option field (e.g. a
    /// <see cref="Table.TableArgFields"/> field the C++ side surfaces as
    /// <c>vgi_copy_formats()</c>'s <c>option_description</c> or an introspection view's per-
    /// argument doc column) — absent when a field has no doc.</summary>
    public const string DocKey = "vgi_doc";

    /// <summary>Marks an <see cref="Table.ITableFunction.OutputSchema"/> field as a
    /// <c>SINGLE_VALUE_PARTITIONS</c>/<see cref="Protocol.VgiPartitionKind"/> partition column (see
    /// <c>vgi_catalog_metadata.hpp</c>) — the C++ side then expects every non-empty data batch to
    /// also carry a <c>vgi_partition_values#b64</c> custom_metadata entry (see
    /// <see cref="PartitionValuesCodec"/>).</summary>
    public const string PartitionColumnKey = "vgi.partition_column";
    public const string PartitionColumnTrueValue = "true";

    /// <summary>Marks a <see cref="Catalog.CatalogTable.Columns"/> field as a GENERATED ALWAYS AS
    /// column — the value is the raw SQL expression DuckDB evaluates on read (e.g. <c>"n * 2"</c>).
    /// <c>VGI_GENERATED_EXPRESSION_METADATA_KEY</c> in <c>vgi_protocol_constants.hpp</c>.</summary>
    public const string GeneratedExpressionKey = "generated_expression";

    /// <summary>Per-argument constraint metadata (agent discovery), surfaced through
    /// <c>vgi_function_arguments()</c> — all presence-only and value-encoded as UTF-8. Kept
    /// byte-for-byte in sync with <c>vgi_protocol_constants.hpp</c>'s <c>VGI_DEFAULT_METADATA_KEY</c>/
    /// <c>VGI_CHOICES_METADATA_KEY</c>/<c>VGI_RANGE_METADATA_KEY</c>/<c>VGI_PATTERN_METADATA_KEY</c>.</summary>
    public const string DefaultKey = "vgi_default";
    public const string ChoicesKey = "vgi_choices";
    public const string RangeKey = "vgi_range";
    public const string PatternKey = "vgi_pattern";

    /// <summary>Builds the <see cref="RangeKey"/> interval-notation string from
    /// <see cref="Attributes.ConstParamAttribute"/>'s <c>Ge</c>/<c>Gt</c>/<c>Le</c>/<c>Lt</c> bounds
    /// (each <see cref="double.NaN"/> when unset) — e.g. <c>"[0, 10]"</c>, <c>"[0, +inf)"</c>,
    /// <c>"(0, 1)"</c>. <see langword="null"/> when neither bound is set.</summary>
    public static string? BuildRange(double ge, double gt, double le, double lt)
    {
        var hasLower = !double.IsNaN(ge) || !double.IsNaN(gt);
        var hasUpper = !double.IsNaN(le) || !double.IsNaN(lt);
        if (!hasLower && !hasUpper)
        {
            return null;
        }

        var lowerBracket = !double.IsNaN(gt) || !hasLower ? "(" : "[";
        var lowerValue = !double.IsNaN(gt) ? FormatBound(gt) : !double.IsNaN(ge) ? FormatBound(ge) : "-inf";
        var upperBracket = !double.IsNaN(lt) || !hasUpper ? ")" : "]";
        var upperValue = !double.IsNaN(lt) ? FormatBound(lt) : !double.IsNaN(le) ? FormatBound(le) : "+inf";
        return $"{lowerBracket}{lowerValue}, {upperValue}{upperBracket}";
    }

    private static string FormatBound(double value) =>
        value == Math.Floor(value) && !double.IsInfinity(value)
            ? ((long)value).ToString(System.Globalization.CultureInfo.InvariantCulture)
            : value.ToString(System.Globalization.CultureInfo.InvariantCulture);
}
