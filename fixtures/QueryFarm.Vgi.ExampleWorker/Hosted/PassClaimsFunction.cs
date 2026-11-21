using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Hosted;

/// <summary>
/// <c>pass_claims(x)</c>: the caller's verified Pass as a JSON object -- the Orchard hosted-mode
/// fixture function, the port of vgi-python's <c>vgi._test_fixtures.orchard_hosted.PassClaimsFunction</c>.
/// Registered only when <c>VGI_ORCHARD_HOSTED</c> is on, so the shared suite's function lists are
/// unchanged everywhere else.
/// </summary>
/// <remarks>
/// Returns <c>{"claims": {...}, "domain": ..., "principal": ...}</c> with sorted keys, one row per
/// input row. Claims keep their JSON types (<c>email_verified</c> a boolean, <c>exp</c> a number),
/// so a test can tell a real boolean from the string <c>"true"</c>. Only the Pass claims below are
/// copied, when present.
/// </remarks>
public sealed class PassClaimsFunction : IScalarFunction
{
    /// <summary>The Pass claims returned, when present.</summary>
    public static readonly IReadOnlyList<string> ClaimNames =
    [
        "sub", "kind", "email", "email_verified", "name", "org_id", "org_name",
        "federated_iss", "federated_sub", "client_id", "iat", "nbf", "exp", "jti",
    ];

    public string Name => "pass_claims";

    public string Description => "The caller's verified Orchard Pass claims as JSON (hosted-mode fixture).";

    public Schema ArgumentsSchema { get; } = new([new Field("x", Int64Type.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", StringType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var auth = processParams.Auth;
        var claims = new SortedDictionary<string, object?>(StringComparer.Ordinal);
        foreach (var name in ClaimNames)
        {
            if (auth.Claims.TryGetValue(name, out var value))
            {
                claims[name] = value;
            }
        }

        var payload = JsonSerializer.Serialize(new SortedDictionary<string, object?>(StringComparer.Ordinal)
        {
            ["claims"] = claims,
            ["domain"] = auth.Domain,
            ["principal"] = auth.Principal,
        });
        var rows = processParams.Input.Length;
        var builder = new StringArray.Builder();
        for (var i = 0; i < rows; i++)
        {
            builder.Append(payload);
        }

        return new RecordBatch(processParams.OutputSchema, [builder.Build()], rows);
    }
}
