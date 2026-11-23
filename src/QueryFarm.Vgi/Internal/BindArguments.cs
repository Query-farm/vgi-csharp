using System.Runtime.CompilerServices;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Errors;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Argument references (protocol 3.4, vgi <c>docs/large-values-3.4.md</c> §2, §5a): validating a
/// <see cref="BindRequest"/>'s <c>arguments</c> / <c>arguments_location</c> pair and resolving the
/// location at the turn's entry, so everything after reads resolved arguments.
/// </summary>
/// <remarks>
/// <para>The resolved value lives on the handler's own <see cref="BindRequest"/> instance (a copy
/// of the received <c>bind_call</c>, never the shared <see cref="Nested{T}.Value"/>), so worker code
/// that reads <c>Arguments</c> keeps working. The received bytes, which carry the location and not
/// the value, are what goes back on the wire or into storage.</para>
/// <para>vgi-rpc does the opening, the object cache (one fetch per <c>object_id</c> per process,
/// single-flight) and the size and SHA-256 checks
/// (<see cref="ICallContext.ResolveArgumentRefAsync"/>).</para>
/// </remarks>
internal static class BindArguments
{
    /// <summary>Kind of the refusal for a bind request that does not set exactly one of the two.</summary>
    public const string InvalidBindRequestKind = "invalid_bind_request";

    /// <summary>The spec's exact message for <see cref="InvalidBindRequestKind"/>.</summary>
    public const string InvalidBindRequestMessage =
        "BindRequest must set exactly one of 'arguments' and 'arguments_location'";

    /// <summary>The spec's exact message for an argument object storage reports gone (§5a,
    /// "Prepared statements"), copied from vgi-python <c>ARGUMENTS_REF_MISSING_MESSAGE</c>.</summary>
    public const string ArgumentsRefMissingMessage =
        "this prepared statement's large argument has expired; prepare it again";

    /// <summary>Kind of a framework bug: arguments read before the turn's entry resolved them.</summary>
    public const string ArgumentsNotResolvedKind = "arguments_not_resolved";

    /// <summary>Refuses a bind request that does not set exactly one of <c>arguments</c> and
    /// <c>arguments_location</c>, or whose location is malformed.</summary>
    /// <exception cref="RpcException"><c>invalid_bind_request</c> or <c>invalid_arguments_ref</c>
    /// (both <c>INVALID_ARGUMENT</c>).</exception>
    public static void Validate(BindRequest bind)
    {
        ArgumentNullException.ThrowIfNull(bind);
        if ((bind.Arguments is null) == (bind.ArgumentsLocation is null))
        {
            throw new RpcException(
                "InvalidBindRequestError", InvalidBindRequestMessage,
                errorKind: InvalidBindRequestKind, errorCode: ErrorCodes.InvalidArgument);
        }

        if (bind.ArgumentsLocation is { } location)
        {
            if (location.Size < 1)
            {
                throw Invalid("arguments_location.size must be a positive byte count");
            }

            if (!IsLowerHexSha256(location.Sha256))
            {
                throw Invalid("arguments_location.sha256 must be 64 lowercase hex digits");
            }
        }
    }

    /// <summary>
    /// Validates <paramref name="bind"/> and, when its arguments were externalized, resolves them
    /// into <see cref="BindRequest.Arguments"/> (keeping <see cref="BindRequest.ArgumentsLocation"/>
    /// set, which the split-token fingerprint uses).
    /// </summary>
    /// <param name="bind">The handler's own instance; it is updated in place.</param>
    /// <param name="ctx">The turn's call context (principal, token key, storage).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><paramref name="bind"/>.</returns>
    /// <exception cref="RpcException">See <see cref="Validate"/>; <c>invalid_arguments_ref</c> for a
    /// reference that does not open or match, no storage, or no context;
    /// <c>arguments_ref_integrity</c>; <c>arguments_ref_missing</c> with
    /// <see cref="ArgumentsRefMissingMessage"/>; <c>storage_unavailable</c>.</exception>
    public static async Task<BindRequest> ResolveAsync(
        BindRequest bind, ICallContext? ctx, CancellationToken cancellationToken = default)
    {
        if (IsResolved(bind))
        {
            return bind;
        }

        Validate(bind);
        if (bind.Arguments is not null)
        {
            MarkResolved(bind);
            return bind;
        }

        var location = bind.ArgumentsLocation!;
        if (ctx is null)
        {
            throw Invalid("argument references need the request's context to resolve");
        }

        byte[] data;
        try
        {
            data = await ctx.ResolveArgumentRefAsync(location.Ref, location.Size, location.Sha256, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RpcException exc) when (exc.ErrorKind == StateRefErrors.ArgumentsRefMissingKind)
        {
            throw new RpcException(
                "ArgumentsRefMissingError", ArgumentsRefMissingMessage,
                errorKind: StateRefErrors.ArgumentsRefMissingKind, errorCode: ErrorCodes.NotFound);
        }
        catch (RpcException exc) when (exc.ErrorKind is StateRefErrors.InvalidArgumentsRefKind or StateRefErrors.ArgumentsRefIntegrityKind)
        {
            // Same kind and code; the message names the kind too, because a client that shows only
            // "<type>: <message>" (the DuckDB extension) would otherwise hide the actionable reason.
            throw new RpcException(
                exc.ErrorType, $"{exc.ErrorKind}: {exc.ErrorMessage}",
                errorKind: exc.ErrorKind, errorCode: exc.ErrorCode);
        }

        bind.Arguments = data;
        MarkResolved(bind);
        return bind;
    }

    /// <summary>The 9-byte-or-longer arguments input of the split-token fingerprint (§5a): a location
    /// contributes <c>0x01 || u64le(size) || sha256[32]</c> (never the sealed <c>ref</c>, which holds
    /// a random nonce), inline arguments <c>0x00 ||</c> their IPC bytes.</summary>
    public static byte[] FingerprintInput(BindRequest bind)
    {
        if (bind.ArgumentsLocation is { } location)
        {
            var input = new byte[1 + 8 + 32];
            input[0] = 0x01;
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(input.AsSpan(1), location.Size);
            Convert.FromHexString(location.Sha256).CopyTo(input, 9);
            return input;
        }

        return [0x00, .. bind.Arguments ?? []];
    }

    private static readonly ConditionalWeakTable<BindRequest, object> s_resolved = [];

    private static bool IsResolved(BindRequest bind) => s_resolved.TryGetValue(bind, out _);

    private static void MarkResolved(BindRequest bind) => s_resolved.AddOrUpdate(bind, bind);

    private static RpcException Invalid(string message) =>
        new("InvalidArgumentsRefError", $"{StateRefErrors.InvalidArgumentsRefKind}: {message}",
            errorKind: StateRefErrors.InvalidArgumentsRefKind, errorCode: ErrorCodes.InvalidArgument);

    private static bool IsLowerHexSha256(string? value) =>
        value is { Length: 64 } && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
}

/// <summary>
/// The per-call overlay of an embedded <c>bind_call</c> (wrapper rule 11): the opened plaintext of
/// its sealed <c>attach_opaque_data</c> / <c>transaction_opaque_data</c>, kept beside the request's
/// <see cref="Nested{T}"/> wrapper and never written into it, so re-encoding the request still emits
/// the received bytes with their sealed envelopes.
/// </summary>
internal static class BindCalls
{
    private static readonly ConditionalWeakTable<object, BindRequest> s_opened = [];

    /// <summary>Records <paramref name="opened"/> as the opened view of <paramref name="request"/>'s
    /// <c>bind_call</c>.</summary>
    public static void SetOpened(object request, BindRequest opened) => s_opened.AddOrUpdate(request, opened);

    /// <summary>The handler's own copy of <paramref name="request"/>'s <c>bind_call</c>: the opened
    /// overlay when the sealing boundary made one, else a fresh decode of the held bytes.</summary>
    public static BindRequest Copy(object request, Nested<BindRequest> bindCall) =>
        s_opened.TryGetValue(request, out var opened) ? opened : bindCall.DecodeCopy();

    /// <summary><see cref="Copy"/>, with its arguments resolved at the turn's entry.</summary>
    public static Task<BindRequest> OpenAsync(object request, Nested<BindRequest> bindCall, ICallContext? ctx) =>
        BindArguments.ResolveAsync(Copy(request, bindCall), ctx);
}
