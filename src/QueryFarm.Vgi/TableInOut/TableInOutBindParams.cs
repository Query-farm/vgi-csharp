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

    public required TableArguments Arguments { get; init; }

    public byte[]? Settings { get; init; }

    /// <summary>Secrets access for this bind attempt — see <see cref="Table.TableBindParams.Secrets"/>'s
    /// doc comment (same static-vs-dynamic split, same two-phase retry mechanism).</summary>
    public required Internal.SecretsAccessor Secrets { get; init; }

    /// <summary>The TABLE argument's per-call column schema (its actual runtime column names/types
    /// — dynamic, since callers can pass any query as the TABLE argument).</summary>
    public required Schema InputSchema { get; init; }

    /// <summary>Raw <c>BindRequest.AttachOpaqueData</c> — see
    /// <see cref="Table.TableBindParams.AttachOpaqueData"/>'s doc comment.</summary>
    public byte[] AttachOpaqueData { get; init; } = [];

    /// <summary>Non-<see langword="null"/> only when this bind opened a <c>COPY ... TO (FORMAT
    /// '&lt;this function's name&gt;', ...)</c> — the destination path (no expected-schema field;
    /// the source columns ride <see cref="InputSchema"/> instead). See
    /// <see cref="Buffering.CopyToFunction"/>.</summary>
    public Protocol.CopyToContext? CopyTo { get; init; }
}
