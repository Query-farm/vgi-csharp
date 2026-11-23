using QueryFarm.VgiRpc.Errors;

namespace QueryFarm.Vgi.Protocol;

public sealed partial class BindRequest
{
    /// <summary>
    /// The bound arguments IPC bytes after the turn's entry resolved any argument reference
    /// (protocol 3.4, <see cref="ArgumentsLocation"/>). Framework code reads arguments through this.
    /// </summary>
    /// <exception cref="RpcException"><c>arguments_not_resolved</c> (<c>FAILED_PRECONDITION</c>): the
    /// arguments are externalized and nothing resolved them, a framework bug.</exception>
    internal byte[] ResolvedArguments => Arguments ?? throw new RpcException(
        "ArgumentsNotResolvedError",
        "BindRequest arguments are externalized (arguments_location) and were not resolved",
        errorKind: Internal.BindArguments.ArgumentsNotResolvedKind,
        errorCode: ErrorCodes.FailedPrecondition);
}
