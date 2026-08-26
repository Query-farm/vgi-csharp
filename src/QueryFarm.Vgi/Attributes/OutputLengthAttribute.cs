namespace QueryFarm.Vgi.Attributes;

/// <summary>
/// Marker attribute (no fields) — injects the current batch's row count (an <see cref="int"/>) into
/// a <c>Compute</c> parameter, for functions with no per-row column input at all (e.g. a
/// const-seeded generator that must still emit one value per row). Named after vgi-python's
/// <c>OutputLength</c> marker.
/// </summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class OutputLengthAttribute : Attribute
{
}
