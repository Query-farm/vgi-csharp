namespace QueryFarm.Vgi.Attributes;

/// <summary>
/// Marks a <c>Compute</c> parameter as a per-row (columnar) argument — named after vgi-python's
/// <c>Param</c> marker (<c>Annotated[pa.Int64Array, Param(doc=...)]</c>), adapted for C#'s
/// attribute-on-parameter syntax. <see cref="ScalarFn"/>'s reflection dispatch resolves the
/// parameter's own CLR array type (e.g. <c>StringArray</c>) into the corresponding fixed Arrow
/// type UNLESS <see cref="Any"/> is set, in which case the parameter's declared CLR type must be
/// the wide <c>IArrowArray</c> and the column's actual on-wire type is accepted as-is (optionally
/// constrained by <see cref="TypeBound"/>).
/// </summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class ParamAttribute : Attribute
{
    /// <summary>Wire field name; defaults to the parameter's own (snake_cased) CLR name when
    /// left empty. Field names are cosmetic for scalar arguments — the C++ side keys by position,
    /// not name — but still surfaced for introspection/documentation.</summary>
    public string Name { get; set; } = "";

    public string Doc { get; set; } = "";

    /// <summary>Set to accept any Arrow-typed column (the parameter's CLR type must then be
    /// <c>IArrowArray</c>); <see cref="TypeBound"/> optionally narrows which actual types are
    /// accepted, enforced once at bind time.</summary>
    public bool Any { get; set; }

    public TypeBoundKind TypeBound { get; set; } = TypeBoundKind.None;

    /// <summary>Consumes every remaining positional column from this point on — the parameter's
    /// CLR type must be <c>IReadOnlyList&lt;IArrowArray&gt;</c> (or a same-shaped array type for a
    /// fixed-element-type vararg). Must be the last <see cref="ParamAttribute"/>/<see cref="ConstParamAttribute"/>-less
    /// vector parameter declared.</summary>
    public bool Varargs { get; set; }
}
