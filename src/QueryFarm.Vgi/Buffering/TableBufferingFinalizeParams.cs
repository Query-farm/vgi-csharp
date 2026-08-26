using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;

namespace QueryFarm.Vgi.Buffering;

/// <summary>Parameters an <see cref="ITableBufferingFunction"/> sees when building the FINALIZE
/// (Source-phase) producer for one <see cref="FinalizeStateId"/> — mirrors
/// <c>init(phase=TABLE_BUFFERING_FINALIZE)</c>. Unlike Process/Combine, this call DOES ride the
/// full <c>bind_call</c> on the wire (the shared <c>init</c> RPC's <c>InitRequest.BindCall</c>), so
/// <see cref="Arguments"/>/<see cref="Settings"/> are decoded directly rather than recovered from
/// storage — but reading them back from <see cref="Storage"/> works too if that's more convenient.</summary>
public sealed class TableBufferingFinalizeParams
{
    public required string FunctionName { get; init; }

    public required byte[] ExecutionId { get; init; }

    /// <summary>One of the ids <see cref="ITableBufferingFunction.Combine"/> returned — names which
    /// output stream this producer drains.</summary>
    public required byte[] FinalizeStateId { get; init; }

    public required TableArguments Arguments { get; init; }

    public byte[]? Settings { get; init; }

    public required Schema OutputSchema { get; init; }

    public IReadOnlyList<long>? ProjectionIds { get; init; }

    /// <summary>Convenience: <see cref="OutputSchema"/> narrowed to <see cref="ProjectionIds"/> (or
    /// the full schema when <see cref="ProjectionIds"/> is <see langword="null"/>) — the schema a
    /// projection-pushdown-aware FINALIZE producer should actually emit. Mirrors
    /// <see cref="Table.TableInitParams.ProjectedSchema"/>.</summary>
    public Schema ProjectedSchema =>
        ProjectionIds is null
            ? OutputSchema
            : new Schema(ProjectionIds.Select(i => OutputSchema.GetFieldByIndex((int)i)), metadata: null);

    /// <summary>Raw embedded-IPC pushdown-filter bytes (<c>InitRequest.PushdownFilters</c>) —
    /// <see langword="null"/> when DuckDB pushed no filters down. Only meaningful when this
    /// function advertised <see cref="ITableBufferingFunction.FilterPushdown"/>. Decode with
    /// <see cref="Internal.PushdownFilterCodec"/>. Mirrors <see cref="Table.TableInitParams.PushdownFilters"/>.</summary>
    public byte[]? PushdownFilters { get; init; }

    /// <summary>One embedded-IPC single-column batch per IN-filter/join-key column
    /// (<c>InitRequest.JoinKeys</c>) — mirrors <see cref="Table.TableInitParams.JoinKeys"/>.</summary>
    public IReadOnlyList<byte[]>? JoinKeys { get; init; }

    public required IFunctionStorage Storage { get; init; }

    /// <summary>Raw <c>BindRequest.AttachOpaqueData</c> — see <see cref="Table.TableBindParams.AttachOpaqueData"/>'s
    /// doc comment. Needed by a function whose durable state is scoped to the ATTACH session rather
    /// than (or in addition to) this execution id — e.g. a persistent, cross-call collection keyed
    /// by attach identity.</summary>
    public byte[]? AttachOpaqueData { get; init; }
}
