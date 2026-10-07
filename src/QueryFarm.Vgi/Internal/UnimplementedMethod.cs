using System.Text.Json;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Wire;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The answer for a <c>vgi.v2</c> method this port registers but does not implement:
/// <c>UNIMPLEMENTED</c> / <c>method_not_implemented</c>, message <c>"&lt;method&gt; is not
/// implemented by this worker"</c>.
/// </summary>
/// <remarks>
/// <para>The protocol is the unit of optionality: every port hosts every <c>vgi.v2</c> method with
/// the reference's exact schemas, so <c>vgi_rpc.Reflection.v1</c> reports one protocol hash
/// everywhere. A method a port has no implementation for is still registered, and every call to it
/// answers with this error, never a silent success. The kind is vgi-rpc's own, the one an
/// unregistered name gets, so a client's "old server, fall back" detection keeps working.</para>
/// <para>Deliberately not vgi-rpc-csharp's <see cref="MethodNotImplementedException"/>: an
/// <see cref="RpcException"/>'s message carries its type as a prefix
/// (<c>"MethodNotImplementedException: ..."</c>), where the wire message is the detail alone. Named
/// <c>...Error</c> because the CLR type name is the wire type name, and this is the reference's
/// (<c>vgi_rpc.MethodNotImplementedError</c>).</para>
/// </remarks>
public sealed class MethodNotImplementedError(string method)
    : Exception($"{method} is not implemented by this worker"), IRpcErrorModel
{
    /// <summary>The method's wire name, e.g. <c>catalog_index_create</c>.</summary>
    public string Method { get; } = method;

    /// <inheritdoc/>
    public string ErrorCode => ErrorCodes.Unimplemented;

    /// <inheritdoc/>
    public string? ErrorKind => MetadataKeys.ErrorKinds.MethodNotImplemented;

    /// <inheritdoc/>
    public IReadOnlyList<JsonElement> ErrorDetails => [];
}

/// <summary>Builds <see cref="MethodNotImplementedError"/> for <see cref="Protocol.IVgiService"/>'s
/// registered-but-unimplemented methods.</summary>
internal static class UnimplementedMethod
{
    /// <summary>The exception to throw from a registered-but-unimplemented method.</summary>
    /// <param name="method">The method's wire name, e.g. <c>catalog_index_create</c>.</param>
    public static MethodNotImplementedError For(string method) => new(method);
}
