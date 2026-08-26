namespace QueryFarm.Vgi.Attributes;

/// <summary>
/// Marks a <c>Compute</c> parameter as bound to a DuckDB session/connection setting (<c>SET
/// &lt;key&gt; = ...</c>) rather than a SQL call argument — named after vgi-python's <c>Setting</c>
/// marker. Invisible in the function's SQL signature (not counted in
/// <see cref="IScalarFunction.ArgumentsSchema"/>/<c>duckdb_functions()</c>); the C++ extension
/// resolves the named setting's current value at bind time and ships it in
/// <c>BindRequest.Settings</c>, keyed by <see cref="Key"/> — the function must additionally
/// declare <see cref="Key"/> in <c>FunctionInfo.RequiredSettings</c> for the extension to bother
/// looking it up at all (see <see cref="Scalar.ScalarFn"/>'s <c>ComputePlan</c>, which derives
/// that list automatically from every <see cref="SettingAttribute"/> it finds).
/// </summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class SettingAttribute : Attribute
{
    /// <summary>The DuckDB setting name; defaults to the parameter's own (snake_cased) CLR name
    /// when left empty.</summary>
    public string Key { get; set; } = "";
}
