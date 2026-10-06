using System.Text.Json;
using QueryFarm.VgiRpc.Attributes;
using QueryFarm.VgiRpc.Errors;

namespace QueryFarm.Vgi.ExampleWorker.Conformance;

/// <summary>
/// <c>conformance.Secondary.v1</c> -- the fixture protocol every vgi-rpc conformance worker and
/// every VGI SDK fixture worker hosts, so one shared test group
/// (<c>vgi-rpc-test-hosted</c>) serves them all. Copied from vgi-rpc's
/// <c>tools/cross-port/specs/MULTI_PROTOCOL_HOSTING.md</c> §2 (the port's own copy lives in its
/// unpublished conformance project). Hosted through <see cref="Worker.HostedProtocols"/>, never
/// special-cased.
/// </summary>
/// <remarks>
/// Declares no protocol version while <c>vgi.v2</c> declares one, so a server gating every call
/// against the primary's version would refuse these. Pinned hash
/// <c>58557cf1611546ad22d1c379bc3ce1b04166082f78375e9fc959f0086347eab6</c>.
/// </remarks>
[ProtocolName(Name)]
public interface ISecondary
{
    /// <summary>The fixture protocol's routing key.</summary>
    public const string Name = "conformance.Secondary.v1";

    /// <summary>Returns <c>"secondary:" + value</c>.</summary>
    string EchoString(string value);

    /// <summary>Always raises with <paramref name="code"/>, <paramref name="kind"/> (absent when
    /// empty) and the fixed details.</summary>
    void Fail(string code, string kind, double retryDelaySeconds);

    /// <summary>Always raises, with details over the 4 KiB cap.</summary>
    void FailOversized();
}

/// <summary>The reference behaviour of <see cref="ISecondary"/>.</summary>
public sealed class SecondaryImpl : ISecondary
{
    public string EchoString(string value) => "secondary:" + value;

    public void Fail(string code, string kind, double retryDelaySeconds)
    {
        if (!ErrorCodes.IsCanonical(code))
        {
            throw new StatusException(
                $"'{code}' is not a canonical error code", ErrorCodes.InvalidArgument, "invalid_code",
                [new BadRequest([new FieldViolation("code", "must be a canonical code name")])]);
        }

        var details = new List<JsonElement>
        {
            new ErrorInfo(new Dictionary<string, string> { ["fixture"] = ISecondary.Name }).ToJson(),
        };
        if (retryDelaySeconds > 0)
        {
            details.Add(new RetryInfo(retryDelaySeconds).ToJson());
        }

        details.Add(JsonSerializer.SerializeToElement(new Dictionary<string, string>
        {
            ["@type"] = ISecondary.Name + ".Probe",
            ["note"] = "clients ignore detail types they do not know",
        }));
        throw new StatusException($"{ISecondary.Name} fail: {code} {kind}".TrimEnd(), code, kind, details);
    }

    public void FailOversized() =>
        throw new StatusException(
            $"{ISecondary.Name} fail_oversized: details exceed 4 KiB", ErrorCodes.ResourceExhausted, "details_oversized",
            [new RetryInfo(1), new ErrorInfo(new Dictionary<string, string> { ["padding"] = new string('x', 5000) })]);
}
