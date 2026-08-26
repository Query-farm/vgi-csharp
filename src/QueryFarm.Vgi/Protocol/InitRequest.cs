namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>init</c> RPC's packed request — the method parameter itself, so <c>ValueCodec</c> auto-
/// embeds THIS type as one outer <c>binary</c> IPC stream. Its own <see cref="BindCall"/> field is
///, independently, ANOTHER embedded IPC stream (a serialized <see cref="BindRequest"/>) — a
/// "binary containing an embedded IPC stream nested inside an outer embedded IPC stream" shape
/// that <c>SchemaDerivation</c>'s normal two-tier rule doesn't cover on its own, which is why
/// <see cref="BindCall"/> is declared as plain <c>byte[]</c> here rather than typed as
/// <see cref="BindRequest"/> — decode it with <see cref="Internal.EmbeddedIpc.Decode{T}"/>.
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING (see <see cref="BindRequest"/>'s doc comment for
/// why) — matches the C++ extension's <c>BuildInitRequest</c>/<c>InitRequestSchema</c> field
/// order exactly, 19 fields. Most of these are irrelevant to a plain scalar-function exchange
/// (they matter for table functions/pushdown/ordering/finalize) and are always null on that path.
/// <see cref="Phase"/> is non-null for table-in-out/table-buffering (see <see cref="VgiInitPhase"/>) —
/// a dictionary-encoded field decodes via the incoming array's OWN type regardless of declared CLR
/// type, so any field that can carry a real (non-null) enum value needs an actual enum CLR type, not
/// a bare <c>string?</c> (see <see cref="VgiOrderByDirection"/>'s doc comment).
/// </summary>
public sealed class InitRequest
{
    public byte[] BindCall { get; set; } = [];

    public byte[] OutputSchema { get; set; } = [];

    public byte[]? BindOpaqueData { get; set; }

    public List<long>? ProjectionIds { get; set; }

    public byte[]? PushdownFilters { get; set; }

    public List<byte[]>? JoinKeys { get; set; }

    public List<byte[]>? SplitTokens { get; set; }

    public long? RowLimit { get; set; }

    public VgiInitPhase? Phase { get; set; }

    public byte[]? FinalizeStateId { get; set; }

    public byte[]? ExecutionId { get; set; }

    public byte[]? InitOpaqueData { get; set; }

    public byte[]? SubstreamId { get; set; }

    public string? OrderByColumnName { get; set; }

    public VgiOrderByDirection? OrderByDirection { get; set; }

    public VgiNullOrder? OrderByNullOrder { get; set; }

    public long? OrderByLimit { get; set; }

    public double? TablesamplePercentage { get; set; }

    public long? TablesampleSeed { get; set; }
}
