using System.Reflection;

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
    /// The major version is in the name deliberately: an incompatible major becomes a
    /// <em>different</em> name and therefore a 404 — an answer every proxy and load balancer
    /// understands without an Arrow parser — and <c>vgi.v2</c> can be served beside a future
    /// <c>vgi.v3</c> while clients migrate. That matters for this consumer specifically: the
    /// extension ships to users and cannot be flag-dayed.
    ///
    /// <para>The VGI <em>secret</em> protocol (<c>vgi.secret.v1</c> in vgi-python) is a separate
    /// name on a separate major, and this port does not host it — nothing here serves the
    /// secret-provider surface; <c>Internal/SecretsAccessor.cs</c> is the worker <em>requesting</em>
    /// secrets through its own bind response, which rides the VGI protocol below.</para>
    /// </remarks>
    public const string Name = "vgi.v2";

    /// <summary>
    /// The contract type to hand <c>QueryFarm.VgiRpc.Server.RpcServer</c> so it hosts
    /// <see cref="IVgiService"/> under <see cref="Name"/> rather than under the C# type's own name.
    /// </summary>
    /// <remarks>
    /// <c>RpcServer</c> takes its protocol name from <c>WireNaming.ForProtocol(serviceInterface)</c>,
    /// i.e. from <c>Type.Name</c>, and QueryFarm.VgiRpc 0.10.0 exposes no server-side override
    /// (only the client side has one, <c>RpcClientOptions.Protocol</c>). A wire name containing a
    /// dot is not expressible as a C# identifier, so the name is declared by handing the server a
    /// <see cref="TypeDelegator"/> over the real contract — every reflection query the transport
    /// makes (methods, parameters, attributes) still resolves against <see cref="IVgiService"/>
    /// itself; only the name it is published under differs. Should a future QueryFarm.VgiRpc grow
    /// an explicit server-side protocol name, this is the single call site to move to it.
    /// </remarks>
    public static Type ServiceContract { get; } = new DeclaredProtocolName(typeof(IVgiService), Name);

    /// <summary>A contract type that reports a declared wire name in place of its C# type name.</summary>
    private sealed class DeclaredProtocolName(Type contract, string wireName) : TypeDelegator(contract)
    {
        public override string Name { get; } = wireName;
    }
}
