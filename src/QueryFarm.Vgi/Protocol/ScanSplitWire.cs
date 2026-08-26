namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One entry of <see cref="TableFunctionPlanResult.Splits"/> — a named, independently redeemable
/// unit of scan work, serialized as its own self-contained 1-row embedded IPC stream (see
/// <see cref="Internal.EmbeddedIpc.Encode{T}"/>/<see cref="Internal.EmbeddedIpc.Decode{T}"/>), the
/// same "nested embedded IPC" shape as <see cref="InitRequest.BindCall"/>. Field ORDER and
/// nullability are wire-significant — matches the C++ extension's generated
/// <c>ScanSplitSchema()</c> exactly, 10 fields.
///
/// The author-facing equivalent is <see cref="Table.ScanSplit"/> — a worker sets only
/// <see cref="Table.ScanSplit.Payload"/> (and optional estimates); <see cref="Internal.VgiServiceImpl"/>
/// stamps <see cref="Token"/> from it via <see cref="Internal.SplitToken.Build"/> and clears
/// <see cref="Payload"/> before serializing (the payload rides sealed inside the token; shipping
/// the plaintext beside it would make the seal decorative — see vgi-java's identical
/// <c>ScanSplit.withToken</c> comment).
/// </summary>
public sealed class ScanSplitWire
{
    public byte[] Payload { get; set; } = [];

    public byte[] Token { get; set; } = [];

    public long? EstimatedRows { get; set; }

    public bool RowsExact { get; set; }

    public long? EstimatedBytes { get; set; }

    public byte[]? PartitionBounds { get; set; }

    public byte[]? ColumnStatistics { get; set; }

    /// <summary>Nullable-item list — the C++ schema declares <c>list(int64)</c> with a nullable
    /// item type (see the port-wide "nullable list-element" gotcha), so this is
    /// <c>List&lt;long?&gt;</c> rather than <c>List&lt;long&gt;</c> even though this worker never
    /// actually populates a null entry.</summary>
    public List<long?>? LocationIds { get; set; }

    public byte[]? StartPosition { get; set; }

    public byte[]? EndPosition { get; set; }
}
