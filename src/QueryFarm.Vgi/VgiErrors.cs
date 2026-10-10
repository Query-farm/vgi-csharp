using System.Text.Json;
using QueryFarm.VgiRpc.Errors;

namespace QueryFarm.Vgi;

// The SDK's own classified errors (vgi-rpc WIRE_PROTOCOL.md §8, "Error model"). Each carries a
// canonical code through IRpcErrorModel, so the vgi-rpc server reports it on the wire and the
// DuckDB extension can tell "your input was wrong" from a worker bug. They set a code only: no
// error kind, no details. Each derives from the BCL type the same failure used to be thrown as, so
// an existing `catch` still catches it. Anything not thrown as one of these goes out as UNKNOWN.
//
// The mapping is shared by every VGI SDK:
//   argument validation / bad argument values / type-bound or type-mismatch on arguments or input
//   columns -> INVALID_ARGUMENT; unknown function, table, schema, catalog or object -> NOT_FOUND;
//   writes against a read-only catalog -> FAILED_PRECONDITION (Internal.CatalogReadOnlyException);
//   an operation the SDK explicitly does not support -> UNIMPLEMENTED.

/// <summary>
/// A call's arguments (or input columns) are wrong: a bad value, a violated constraint, or a type
/// a function does not accept. Reported as <c>INVALID_ARGUMENT</c>. Throw it from a function's
/// <c>Bind</c> to reject what the caller passed.
/// </summary>
public class VgiInvalidArgumentException : InvalidOperationException, IRpcErrorModel
{
    public VgiInvalidArgumentException(string message)
        : base(message)
    {
    }

    public VgiInvalidArgumentException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

    public string ErrorCode => ErrorCodes.InvalidArgument;

    public string? ErrorKind => null;

    public IReadOnlyList<JsonElement> ErrorDetails => [];
}

/// <summary>
/// A lookup named something that does not exist: a function, table, view, schema, catalog or other
/// catalog object. Reported as <c>NOT_FOUND</c>.
/// </summary>
public class VgiNotFoundException : InvalidOperationException, IRpcErrorModel
{
    public VgiNotFoundException(string message)
        : base(message)
    {
    }

    public VgiNotFoundException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

    public string ErrorCode => ErrorCodes.NotFound;

    public string? ErrorKind => null;

    public IReadOnlyList<JsonElement> ErrorDetails => [];
}

/// <summary>
/// An operation this SDK (or the object it was asked of) explicitly does not support. Reported as
/// <c>UNIMPLEMENTED</c>.
/// </summary>
public class VgiUnimplementedException : NotSupportedException, IRpcErrorModel
{
    public VgiUnimplementedException(string message)
        : base(message)
    {
    }

    public VgiUnimplementedException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

    public string ErrorCode => ErrorCodes.Unimplemented;

    public string? ErrorKind => null;

    public IReadOnlyList<JsonElement> ErrorDetails => [];
}
