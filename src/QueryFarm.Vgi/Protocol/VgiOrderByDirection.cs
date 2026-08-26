namespace QueryFarm.Vgi.Protocol;

/// <summary>Wire values: ASC, DESC (<c>InitRequest.OrderByDirection</c>/<c>TableFunctionPlanRequest.OrderByDirection</c>).
/// A dictionary-encoded (<c>dictionary(int16, utf8)</c>) field decodes via <c>ValueCodec.ExtractEnum</c>
/// purely based on the incoming Arrow array's own type (a <see cref="Apache.Arrow.DictionaryArray"/>) —
/// declaring this field as a bare <c>string?</c> instead fails decode with "Enum 'System.String' has
/// no member matching wire name ...", so any non-always-null dictionary-encoded field needs its own
/// enum CLR type, not just a string.</summary>
public enum VgiOrderByDirection
{
    Asc,
    Desc,
}

/// <summary>Wire values: NULLS_FIRST, NULLS_LAST (<c>InitRequest.OrderByNullOrder</c>/
/// <c>TableFunctionPlanRequest.OrderByNullOrder</c>).</summary>
public enum VgiNullOrder
{
    NullsFirst,
    NullsLast,
}
