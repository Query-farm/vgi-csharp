namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The VGI application protocol's wire identity — the name this worker hosts the protocol under
/// and the name a client addresses it by.
/// </summary>
/// <remarks>
/// Declared, not derived. Left to the transport's default, the name is whatever each language's
/// local contract type happens to be called (<see cref="IVgiService"/> here, minus C#'s <c>I</c>
/// prefix, so <c>VgiService</c>), and the six implementations of this one protocol disagreed four
/// ways — Python <c>VgiProtocol</c>, Java and C# <c>VgiService</c>, Go the framework default
/// <c>Service</c>, TypeScript <c>vgi</c>. That was invisible until <c>vgi_rpc.protocol</c> became
/// a required routing key, at which point it became load-bearing: there is no single string the
/// DuckDB extension can send that all six answer.
/// </remarks>
public static class VgiProtocol
{
    /// <summary>
    /// The protocol's wire name: the <c>vgi_rpc.protocol</c> routing key on the raw transports and
    /// the protocol path segment over HTTP. Canonically declared in vgi-python
    /// (<c>VgiProtocol.protocol_name</c>) and emitted by the DuckDB C++ extension.
    /// </summary>
    /// <remarks>
    /// The one place this string is written down. It reaches the transport as
    /// <c>[ProtocolName]</c> on <see cref="IVgiService"/>, which is what both the server (hosting)
    /// and a typed client (addressing) resolve through, so the two cannot drift.
    ///
    /// <para>The major version is in the name deliberately: an incompatible major becomes a
    /// <em>different</em> name and therefore a 404 — an answer every proxy and load balancer
    /// understands without an Arrow parser — and <c>vgi.v2</c> can be served beside a future
    /// <c>vgi.v3</c> while clients migrate. That matters for this consumer specifically: the
    /// extension ships to users and cannot be flag-dayed.</para>
    ///
    /// <para>The VGI <em>secret</em> protocol (<c>vgi.secret.v1</c> in vgi-python) is a separate
    /// name on a separate major, and this port does not host it — nothing here serves the
    /// secret-provider surface; <c>Internal/SecretsAccessor.cs</c> is the worker <em>requesting</em>
    /// secrets through its own bind response, which rides the VGI protocol below.</para>
    /// </remarks>
    public const string Name = "vgi.v2";
}
