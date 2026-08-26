namespace QueryFarm.Vgi.Attributes;

/// <summary>
/// Marks a <c>Compute</c> parameter as a bind-time constant argument — named after vgi-python's
/// <c>ConstParam</c> marker. DuckDB requires the SQL call site to pass a foldable (constant)
/// expression for this positional argument; its value is extracted once at <c>bind</c> and
/// delivered to every batch of the call (never as a per-row column). <see cref="ScalarFn"/>
/// resolves const parameters by a SEPARATE positional counter from <see cref="ParamAttribute"/>
/// ones — the wire's <c>positional_&lt;i&gt;</c> struct fields inside <c>BindRequest.Arguments</c>
/// are indexed 0.. over CONST parameters only, in <c>Compute</c> declaration order.
/// </summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class ConstParamAttribute : Attribute
{
    public string Name { get; set; } = "";

    public string Doc { get; set; } = "";

    /// <summary>Inclusive lower bound (>=), surfaced as part of <c>vgi_function_arguments()</c>'s
    /// <c>arg_range</c> interval notation (e.g. <c>"[0, 10]"</c>) and — for a table-function
    /// positional/named argument built via the matching range on <see cref="Table.TableArgFields"/>
    /// — enforced at bind time. <see cref="double.NaN"/> (the default) means unset; at most one of
    /// <see cref="Ge"/>/<see cref="Gt"/> may be set.</summary>
    public double Ge { get; set; } = double.NaN;

    /// <summary>Exclusive lower bound (&gt;). See <see cref="Ge"/>.</summary>
    public double Gt { get; set; } = double.NaN;

    /// <summary>Inclusive upper bound (&lt;=). See <see cref="Ge"/>; at most one of
    /// <see cref="Le"/>/<see cref="Lt"/> may be set.</summary>
    public double Le { get; set; } = double.NaN;

    /// <summary>Exclusive upper bound (&lt;). See <see cref="Le"/>.</summary>
    public double Lt { get; set; } = double.NaN;
}
