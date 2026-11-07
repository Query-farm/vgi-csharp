namespace QueryFarm.Vgi.Attributes;

/// <summary>
/// Marks the method a <see cref="Scalar.ScalarFn"/> subclass dispatches each batch to. Optional:
/// without it, <see cref="Scalar.ScalarFn"/> uses the single method named <c>Compute</c> (instance
/// or <see langword="static"/>, any accessibility) declared on the most-derived class that has one,
/// so a <c>Compute</c> inherited from an intermediate base class works too. Use the attribute to
/// pick one of several <c>Compute</c> overloads, or to give the method another name. At most one
/// method in the class hierarchy may carry it.
/// </summary>
[AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
public sealed class ComputeAttribute : Attribute
{
}
