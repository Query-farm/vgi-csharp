namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// Wire values: INPUT, FINALIZE, TABLE_BUFFERING, TABLE_BUFFERING_FINALIZE (<c>InitRequest.Phase</c>) —
/// the C++ extension's <c>phase_values</c> array in <c>BuildInitRequest</c> (<c>vgi_rpc_types.cpp</c>).
/// A dictionary-encoded (<c>dictionary(int16, utf8)</c>) field decodes via <c>ValueCodec.ExtractEnum</c>
/// purely based on the incoming Arrow array's own type — declaring this field as a bare <c>string?</c>
/// (as M1-M3 did, since <c>phase</c> was always null on the scalar/plain-table path) fails decode with
/// "Enum 'System.String' has no member matching wire name ..." the moment a real value ever rides the
/// wire — see <see cref="VgiOrderByDirection"/>'s doc comment for the same lesson learned earlier.
///
/// Meaning:
/// <list type="bullet">
/// <item><b>Input</b> — table-in-out streaming exchange phase (<see cref="ExchangeState"/>-shaped):
/// the client writes input batches, this replies with output batches.</item>
/// <item><b>Finalize</b> — table-in-out per-substream finalize (<see cref="ProducerState"/>-shaped,
/// tick-driven), run on the SAME connection that just finished the Input phase.</item>
/// <item><b>TableBuffering</b> — table-buffering's Sink-phase init: mints/joins the query's
/// <c>execution_id</c> on a fresh connection, which then immediately closes its (empty) input writer
/// so the exchange completes; all real Sink traffic afterward is the standalone
/// <c>table_buffering_process</c>/<c>table_buffering_combine</c>/<c>table_buffering_destructor</c>
/// unary RPCs (each independently worker-pool-acquired — NOT guaranteed to land on this connection).</item>
/// <item><b>TableBufferingFinalize</b> — table-buffering's Source-phase init (<see cref="ProducerState"/>-shaped):
/// carries <c>InitRequest.FinalizeStateId</c>, opened on whatever pooled connection the Source
/// operator acquired for that one <c>finalize_state_id</c>.</item>
/// </list>
/// </summary>
public enum VgiInitPhase
{
    Input,
    Finalize,
    TableBuffering,
    TableBufferingFinalize,
}
