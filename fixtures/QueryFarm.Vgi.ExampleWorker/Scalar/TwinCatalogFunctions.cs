using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>test_same_name_catalog(value: BIGINT) -&gt; VARCHAR</c>, registered TWICE under the SAME
/// schema/name but two different attach IDENTITIES (<c>"twin_a"</c>/<c>"twin_b"</c>) — the same
/// worker BINARY attached twice as two different catalogs
/// (<c>ATTACH 'twin_a' AS a (...)</c> / <c>ATTACH 'twin_b' AS b (...)</c>) must still route each
/// call to the implementation matching the ATTACH it came through
/// (<c>same_name_catalogs.test</c>). See <c>Program.cs</c> for the
/// <c>Worker.RegisterScalar(function, identity: "twin_a")</c> registration that ties these to
/// their attach identity.
/// </summary>
public sealed class TwinAFunction : ScalarFn
{
    public override string Name => "test_same_name_catalog";

    public override string Description => "Catalog-disambiguation probe; the twin_a implementation";

    private void Compute([Param] Int64Array value, StringArray.Builder result) => TwinCatalogFunctions.Write("twin_a", value, result);
}

public sealed class TwinBFunction : ScalarFn
{
    public override string Name => "test_same_name_catalog";

    public override string Description => "Catalog-disambiguation probe; the twin_b implementation";

    private void Compute([Param] Int64Array value, StringArray.Builder result) => TwinCatalogFunctions.Write("twin_b", value, result);
}

internal static class TwinCatalogFunctions
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
