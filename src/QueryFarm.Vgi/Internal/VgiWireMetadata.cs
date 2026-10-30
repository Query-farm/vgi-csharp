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

    /// <summary>The pre-v3 GENERATED ALWAYS AS key, whose value is the raw SQL expression (e.g.
    /// <c>"n * 2"</c>). Still accepted: a top-level column carrying it (and no canonical expression
    /// or default metadata) is translated to the canonical <see cref="GeneratedKey"/>/
    /// <see cref="GenerationKey"/> pair when the table is served. QueryFarm.Vgi 0.22.0–0.22.2 gave
    /// this constant the canonical key's value; it is <c>"generated_expression"</c> again.</summary>
    [Obsolete("Declare generated columns with CatalogTable.GeneratedColumns, or set GeneratedKey (canonical base64 " +
              "SqlExpression IPC from ColumnExpressions.GeneratedMetadata) plus GenerationKey = \"virtual\". " +
              "Raw SQL under this legacy key is translated automatically.")]
    public const string GeneratedExpressionKey = ColumnExpressions.LegacyGeneratedKey;

    /// <summary>Marks a <see cref="Catalog.CatalogTable.Columns"/> field as a VIRTUAL generated
    /// column — the value is canonical base64 SqlExpression IPC
    /// (<see cref="ColumnExpressions.GeneratedMetadata"/>); <see cref="GenerationKey"/> must be
    /// <c>"virtual"</c> alongside it.</summary>
    public const string GeneratedKey = ColumnExpressions.GeneratedKey;

    /// <summary>The generation category of a <see cref="GeneratedKey"/> column; only
    /// <c>"virtual"</c> is supported.</summary>
    public const string GenerationKey = ColumnExpressions.GenerationKey;

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
