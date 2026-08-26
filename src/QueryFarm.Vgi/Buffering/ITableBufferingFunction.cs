using Apache.Arrow;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;

namespace QueryFarm.Vgi.Buffering;

/// <summary>
/// The raw contract a table-buffering (Sink+Source) function implements — a table-in-out-shaped
/// call site (<c>f(data TABLE, ...)</c>) that, unlike <see cref="ITableInOutFunction"/>, must see
/// EVERY input row across EVERY substream before producing any output. Ported from vgi-python's
/// <c>TableBufferingFunction</c>/vgi-java's <c>TableBufferingFunction</c>.
///
/// Three phases, matching the C++ <c>PhysicalVgiTableBuffering</c> Sink+Source operator:
/// <list type="number">
/// <item><b>Sink</b> — <see cref="Process"/>, once per input batch (parallel across threads/processes
/// unless <see cref="SinkOrderDependent"/>) — stash the batch and return an opaque <c>state_id</c>.</item>
/// <item><b>Combine</b> — <see cref="Combine"/>, once, after every Sink call completes — group/merge
/// the collected <c>state_id</c>s into <c>finalize_state_id</c>s, one per Source output stream.</item>
/// <item><b>Source</b> — <see cref="CreateFinalizeProducer"/>, once per <c>finalize_state_id</c> —
/// builds an <see cref="ITableFunctionProducer"/> the framework ticks until it finishes.</item>
/// </list>
///
/// This is what makes a GLOBALLY correct "sum every row" (or sort/dedupe/etc.) possible: unlike
/// <see cref="ITableInOutFunction"/>'s per-substream FINALIZE (which only ever sees its own
/// substream's share of the input — wrong for a query-wide aggregate), Combine sees every Sink
/// call's result before Source ever runs.
///
/// Cross-process invariant: <see cref="Process"/>/<see cref="Combine"/> are each independently
/// worker-pool-acquired unary RPCs (see <see cref="TableBufferingProcessParams"/>'s doc comment) —
/// any state one call needs to hand to a later call/phase MUST go through
/// <see cref="TableBufferingProcessParams.Storage"/>/<see cref="TableBufferingCombineParams.Storage"/>,
/// never an in-memory field on the function instance (which is only ever visible within the ONE
/// worker process that happens to run a given call).
/// </summary>
public interface ITableBufferingFunction
{
    string Name { get; }

    string SchemaName => "main";

    string Description => "";

    /// <summary>Optional free-text comment surfaced via <c>duckdb_functions().comment</c>
    /// (<see cref="Protocol.FunctionInfo.Comment"/>). <see langword="null"/> (the default) reports
    /// no comment.</summary>
    string? Comment => null;

    /// <summary>Optional key/value tags surfaced via <c>duckdb_functions().tags</c>
    /// (<see cref="Protocol.FunctionInfo.Tags"/>). Empty (the default) reports no tags.</summary>
    IReadOnlyDictionary<string, string> Tags => new Dictionary<string, string>();

    IReadOnlyList<string> Categories => [];

    /// <summary>Same shape as <see cref="ITableInOutFunction.ArgumentsSchema"/> — the non-table
    /// positional/named arguments plus exactly one <see cref="Table.TableArgFields.Table"/>-marked
    /// field for the TABLE input.</summary>
    Schema ArgumentsSchema { get; }

    Schema OutputSchema { get; }

    FunctionStability? Stability => null;

    IReadOnlyList<string> RequiredSettings => [];

    /// <summary>Secrets this function needs statically resolved and shipped on every bind — see
    /// <see cref="Table.ITableFunction.RequiredSecrets"/>'s doc comment.</summary>
    IReadOnlyList<RequiredSecret> RequiredSecrets => [];

    bool? ProjectionPushdown => null;

    /// <summary>Advertises whether this function inspects <see cref="TableBufferingFinalizeParams.PushdownFilters"/>
    /// and actually drops non-matching rows in its FINALIZE producer — see
    /// <see cref="Internal.PushdownFilterEvaluator"/>'s doc comment for why this is load-bearing:
    /// unlike a plain <see cref="Table.ITableFunction"/> scan, the C++ Sink+Source operator installs
    /// NO residual post-scan filter when this is <see langword="true"/> (the operator fully
    /// materializes its output, so there is nowhere for DuckDB to re-check afterward), so a function
    /// that advertises this MUST apply the pushed filters itself or rows will leak through
    /// unfiltered.</summary>
    bool? FilterPushdown => null;

    int? MaxWorkers => null;

    /// <summary>Forces ordered, single-threaded Sink ingest (<c>ParallelSink=false</c> on the C++
    /// operator) — <see cref="Process"/> then sees input batches in source order.</summary>
    bool SinkOrderDependent => false;

    /// <summary>Forces ordered, single-stream Source draining in <see cref="Combine"/>'s returned
    /// order (<c>ParallelSource=false</c>).</summary>
    bool SourceOrderDependent => false;

    /// <summary>Whether <see cref="TableBufferingProcessParams.BatchIndex"/> should be populated.
    /// Mutually exclusive with <see cref="SinkOrderDependent"/> (a single-threaded sink already sees
    /// input in order; batch_index only matters under parallel ingest).</summary>
    bool RequiresInputBatchIndex => false;

    /// <summary>Called once per bound call (mirrors the <c>bind</c> RPC) — a no-op by default;
    /// override to validate arguments or compute a dynamic output type.</summary>
    void Bind(TableInOutBindParams bindParams)
    {
    }

    /// <summary>Resolves the schema to use for THIS specific call. Defaults to the static
    /// <see cref="OutputSchema"/>.</summary>
    Schema ResolveOutputSchema(TableInOutBindParams bindParams) => OutputSchema;

    /// <summary>Sink phase: ingest one input batch, return an opaque <c>state_id</c> naming where it
    /// (or a derived summary) was stashed in <see cref="TableBufferingProcessParams.Storage"/>. A
    /// common pattern for "one accumulator for the whole execution" is to always return
    /// <paramref name="processParams"/>.ExecutionId — <see cref="Combine"/> then just needs to
    /// collapse the (identical, duplicated) ids down to a single one.</summary>
    byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams);

    /// <summary>Combine phase: groups/merges the collected <paramref name="stateIds"/> (every id
    /// returned by every <see cref="Process"/> call, in arbitrary order, NOT deduplicated by the
    /// framework) into the <c>finalize_state_id</c>s the Source phase will drain, one
    /// <see cref="CreateFinalizeProducer"/> stream per returned id.</summary>
    IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams);

    /// <summary>Source phase: builds the producer that drains output for one
    /// <paramref name="finalizeStateId"/> — reuses <see cref="ITableFunctionProducer"/> since the
    /// tick/emit/finish contract is identical to a plain table function's producer.</summary>
    ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams);
}
