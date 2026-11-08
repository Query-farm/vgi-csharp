using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.TableInOut;

/// <summary>Parameters an <see cref="ITableInOutFunction"/>/<see cref="Buffering.ITableBufferingFunction"/>
/// sees at bind time — the table-in-out analog of <see cref="TableBindParams"/>, with one addition:
/// <see cref="InputSchema"/> is always populated (the TABLE argument's per-call column schema),
/// since a table-in-out/table-buffering function without a TABLE input is meaningless.</summary>
public sealed class TableInOutBindParams
{
    public required string FunctionName { get; init; }

    /// <summary>Opaque, not-yet-decoded serialized argument bytes for the NON-table positional/named
    /// arguments — decode with <see cref="TableArgCodec.Decode"/> (or use <see cref="Arguments"/>).
    /// The TABLE argument itself never appears here (the C++ side omits it entirely when building
    /// this struct — see <see cref="Table.TableArgFields.Table"/>'s doc comment).</summary>
    public byte[] ArgumentsBytes { get; init; } = [];

    public required QueryFarm.Vgi.Arguments.TableArguments Arguments { get; init; }

    public byte[]? Settings { get; init; }

    /// <summary>Secrets access for this bind attempt — see <see cref="Table.TableBindParams.Secrets"/>'s
    /// doc comment (same static-vs-dynamic split, same two-phase retry mechanism).</summary>
    public required Internal.SecretsAccessor Secrets { get; init; }

    private QueryFarm.Vgi.Arguments.SecretValues? _secretValues;

    /// <summary>Typed view of <see cref="Secrets"/> (same object, same two-phase semantics):
    /// <see cref="QueryFarm.Vgi.Arguments.SecretValues.Get"/> returns <see langword="null"/> on the
    /// first attempt for a dynamically scoped lookup and the framework retries the bind with the
    /// secret resolved.</summary>
    public QueryFarm.Vgi.Arguments.SecretValues SecretValues =>
        _secretValues ??= new QueryFarm.Vgi.Arguments.SecretValues(Secrets);

    /// <summary>The TABLE argument's per-call column schema (its actual runtime column names/types
    /// — dynamic, since callers can pass any query as the TABLE argument).</summary>
    public required Schema InputSchema { get; init; }

    public IReadOnlyList<string?>? ArgumentNames { get; init; }

    /// <summary>Raw <c>BindRequest.AttachOpaqueData</c> — see
    /// <see cref="Table.TableBindParams.AttachOpaqueData"/>'s doc comment.</summary>
    public byte[] AttachOpaqueData { get; init; } = [];

    private Catalog.AttachOptions? _attachOptions;

    /// <summary>This attach's ATTACH options, <c>secret</c> ones included (decrypted on first
    /// access) — <see cref="Catalog.AttachOptions.FromAttachData"/> over <see cref="AttachOpaqueData"/>.</summary>
    public Catalog.AttachOptions AttachOptions => _attachOptions ??= Catalog.AttachOptions.FromAttachData(AttachOpaqueData);

    /// <summary>The transaction's plaintext id (<c>BindRequest.TransactionOpaqueData</c>) — non-empty
    /// only on an attach that advertised <see cref="Protocol.CatalogAttachResult.SupportsTransactions"/>
    /// (see <see cref="Protocol.AttachContext.SupportsTransactions"/>), where it is the id
    /// <see cref="Catalog.TransactionHooks.BeginAsync"/> returned for the SQL transaction this call
    /// runs in. The same bytes reach the commit/rollback hook, so a write function (an UPDATE
    /// delegate, say) can stage its changes under it and let the commit hook apply them. See
    /// <see cref="Table.TableBindParams.TransactionOpaqueData"/>.</summary>
    public byte[] TransactionOpaqueData { get; init; } = [];

    /// <summary>Non-<see langword="null"/> only when this bind opened a <c>COPY ... TO (FORMAT
    /// '&lt;this function's name&gt;', ...)</c> — the destination path (no expected-schema field;
    /// the source columns ride <see cref="InputSchema"/> instead). See
    /// <see cref="Buffering.CopyToFunction"/>.</summary>
    public Protocol.CopyToContext? CopyTo { get; init; }
}
