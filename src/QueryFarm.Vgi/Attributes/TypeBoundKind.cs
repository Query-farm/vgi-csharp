namespace QueryFarm.Vgi.Attributes;

/// <summary>
/// Bind-time constraints that can be attached to an "any"-typed <see cref="ParamAttribute"/>
/// (a parameter with no fixed Arrow type) — mirrors vgi-python's <c>TypeBoundPredicate</c> /
/// vgi-java's <c>TypeBoundPredicate</c> enum. Currently only one predicate exists upstream.
/// </summary>
public enum TypeBoundKind
{
    /// <summary>No constraint — any Arrow type is accepted.</summary>
    None = 0,

    /// <summary>The column's type must be numeric (integer/float) or decimal — see
    /// <see cref="QueryFarm.Vgi.Types.TypeRules.IsAddable"/>.</summary>
    IsAddable,
}
