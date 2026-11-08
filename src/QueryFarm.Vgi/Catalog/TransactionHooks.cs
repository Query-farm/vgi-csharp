using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// A worker's catalog transaction lifecycle — the C# counterpart of overriding vgi-typescript's
/// <c>transactionBegin</c>/<c>transactionCommit</c>/<c>transactionRollback</c> (or vgi-python's
/// <c>catalog_transaction_*</c>). Register one with <see cref="Worker.TransactionHooks"/>, and turn
/// transactions on per attach with <see cref="Protocol.AttachContext.SupportsTransactions"/>: DuckDB
/// only calls these for an attach whose result advertised it.
/// </summary>
/// <remarks>
/// <para>Every SQL transaction on such an attach (an autocommit statement included) calls
/// <see cref="BeginAsync"/> once; the id it returns rides every function call of that transaction
/// as <c>TransactionOpaqueData</c> (<see cref="Table.TableBindParams.TransactionOpaqueData"/>,
/// <see cref="Table.TableInitParams.TransactionOpaqueData"/>,
/// <see cref="TableInOut.TableInOutBindParams.TransactionOpaqueData"/>,
/// <see cref="TableInOut.TableInOutInitParams.TransactionOpaqueData"/>) and comes back to exactly one
/// of <see cref="CommitAsync"/> or <see cref="RollbackAsync"/>. The hooks and the functions always
/// see the same <b>plaintext</b> bytes: on HTTP the SDK seals the value on the wire and opens it
/// before either runs.</para>
/// <para>An exception thrown by <see cref="CommitAsync"/> fails the client's <c>COMMIT</c> with its
/// message; the hook should discard the transaction's staged state before throwing rather than
/// count on a rollback call following the failed commit. Calls for one transaction can reach different worker processes
/// (a stdio pool, a launcher pool, several HTTP replicas), so state staged under a transaction id
/// must live somewhere every process can reach (a database, a file, the provider's own API).</para>
/// <para>After either hook returns (or throws) the SDK also clears the transaction's
/// <c>Internal.FunctionStorage</c> namespace, as it always has.</para>
/// </remarks>
public abstract class TransactionHooks
{
    /// <summary>Starts a transaction and returns its id, which becomes the transaction's
    /// <c>TransactionOpaqueData</c>. Return <see langword="null"/> (the default) to have the SDK mint
    /// 16 cryptographically random bytes. An id returned here must be unpredictable (it is a
    /// client-held value; see vgi-opaque-data-sealing.md rule 6) and must never contain a secret.</summary>
    public virtual Task<byte[]?> BeginAsync(TransactionContext context) => Task.FromResult<byte[]?>(null);

    /// <summary>Commits the transaction <see cref="TransactionContext.TransactionOpaqueData"/>
    /// names. Throw to refuse the commit: the client's <c>COMMIT</c> fails with the exception's
    /// message.</summary>
    public virtual Task CommitAsync(TransactionContext context) => Task.CompletedTask;

    /// <summary>Discards the transaction <see cref="TransactionContext.TransactionOpaqueData"/>
    /// names.</summary>
    public virtual Task RollbackAsync(TransactionContext context) => Task.CompletedTask;
}

/// <summary>What a <see cref="TransactionHooks"/> call is about.</summary>
public sealed class TransactionContext
{
    private AttachData? _attach;

    /// <summary>The attach's plaintext <c>attach_opaque_data</c> — the same bytes every function of
    /// the attach sees as <c>AttachOpaqueData</c>. <see cref="Attach"/> decodes it.</summary>
    public required byte[] AttachOpaqueData { get; init; }

    /// <summary>The transaction's plaintext id: what <see cref="TransactionHooks.BeginAsync"/>
    /// returned (or the SDK minted). Empty in <see cref="TransactionHooks.BeginAsync"/>.</summary>
    public byte[] TransactionOpaqueData { get; init; } = [];

    /// <summary>The decoded attach envelope: routing identity, per-attach id and the
    /// <see cref="Protocol.AttachContext.ExtraOpaqueData"/> the <see cref="Worker.OnAttach"/> handler
    /// stored.</summary>
    public AttachData Attach => _attach ??= AttachData.Parse(AttachOpaqueData);

    /// <summary>The attach's ATTACH options, <c>secret</c> ones included — <c>Attach.Options</c>.</summary>
    public AttachOptions AttachOptions => Attach.Options;

    /// <summary>The RPC call context (caller identity on HTTP), when the transport supplies one.</summary>
    public ICallContext? CallContext { get; init; }
}
