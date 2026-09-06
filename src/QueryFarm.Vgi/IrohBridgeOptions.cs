namespace QueryFarm.Vgi;

/// <summary>Trust boundary between a loopback VGI worker and <c>vgi-iroh-bridge</c>.</summary>
public sealed record IrohBridgeOptions(
    string Issuer,
    IReadOnlyList<string>? TrustedProxyAddresses = null,
    bool Authenticate = true)
{
    internal IReadOnlyList<string> EffectiveTrustedProxyAddresses =>
        TrustedProxyAddresses is { Count: > 0 } ? TrustedProxyAddresses : ["127.0.0.1"];
}
