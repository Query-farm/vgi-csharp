using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>test_same_name_bind(value: BIGINT) -&gt; VARCHAR</c>, registered TWICE under the SAME
/// catalog identity but two different schemas (<c>main</c>/<c>data</c>) — proves
/// <see cref="Internal.CatalogRegistry"/> keys registrations by <c>(identity, schema, name)</c>,
/// not name alone (<c>same_name_schemas.test</c>). Each returns a schema-tagged VARCHAR so a
/// mis-routed call reads as the wrong tag instead of a plausible answer.
/// </summary>
public sealed class SameNameMainFunction : ScalarFn
{
    public override string Name => "test_same_name_bind";

    public override string SchemaName => "main";

    public override string Description => "Schema-disambiguation probe; the main-schema implementation";

    private void Compute([Param] Int64Array value, StringArray.Builder result) => SameNameFunctions.Write("main", value, result);
}

public sealed class SameNameDataFunction : ScalarFn
{
    public override string Name => "test_same_name_bind";

    public override string SchemaName => "data";

    public override string Description => "Schema-disambiguation probe; the data-schema implementation";

    private void Compute([Param] Int64Array value, StringArray.Builder result) => SameNameFunctions.Write("data", value, result);
}

internal static class SameNameFunctions
{
    public static void Write(string tag, Int64Array value, StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append($"{tag}:{value.GetValue(i)!.Value}");
        }
    }
}
