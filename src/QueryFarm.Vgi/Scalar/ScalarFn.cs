using Apache.Arrow;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Scalar;

/// <summary>
/// Attribute-driven convenience base class for <see cref="IScalarFunction"/> — reflects a
/// subclass's own <c>Compute</c> method exactly once (see <see cref="ComputePlan"/>) and dispatches
/// <see cref="Process"/> against it, so a subclass just writes:
///
/// <code>
/// private void Compute([Param] StringArray value, StringArray.Builder result)
/// {
///     for (var i = 0; i &lt; value.Length; i++)
///     {
///         if (value.IsNull(i)) { result.AppendNull(); continue; }
///         result.Append(value.GetString(i).ToUpperInvariant());
///     }
/// }
/// </code>
///
/// See <see cref="ComputePlan"/>'s doc comment for exactly which parameter shapes are supported
/// (and which ANY-typed/varargs shapes are deliberately NOT — those implement
/// <see cref="IScalarFunction"/> directly instead).
/// </summary>
public abstract class ScalarFn : IScalarFunction
{
    private readonly ComputePlan _plan;

    protected ScalarFn()
    {
        _plan = ComputePlan.ForType(GetType());
    }

    public abstract string Name { get; }

    public virtual string SchemaName => "main";

    public virtual string Description => "";

    public virtual Schema ArgumentsSchema => _plan.ArgumentsSchema;

    public virtual Schema OutputSchema => _plan.OutputSchema;

    public virtual FunctionStability? Stability => null;

    public virtual FunctionNullHandling? NullHandling => null;

    public virtual IReadOnlyList<string> RequiredSettings => _plan.RequiredSettingNames;

    public virtual IReadOnlyList<RequiredSecret> RequiredSecrets => _plan.RequiredSecretsList;

    public virtual IReadOnlyDictionary<string, string>? CacheControlMetadata => null;

    public virtual void Bind(ScalarBindParams bindParams)
    {
    }

    public virtual Schema ResolveOutputSchema(Schema? inputSchema) => OutputSchema;

    public RecordBatch Process(ScalarProcessParams processParams) => _plan.Invoke(this, processParams);
}
