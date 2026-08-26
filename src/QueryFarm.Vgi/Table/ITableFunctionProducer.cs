using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Table;

/// <summary>
/// The per-call cursor an <see cref="ITableFunction.CreateProducer"/> returns — driven once per
/// client "tick" by <see cref="Internal.TableProducerStreamState"/> (the <c>ProducerState</c>
/// dispatch glue). Mirrors vgi-java's <c>TableProducerState.produceTick(OutputCollector, ...)</c>.
/// </summary>
public interface ITableFunctionProducer
{
    /// <summary>Produces this turn's contribution: call <paramref name="output"/>.Emit(...) at most
    /// once (a batch's row count may be anything from 1 to however large this function wants a
    /// "batch" to be — DuckDB internally re-chunks to its own vector size regardless; every
    /// emitted batch's schema must match the <see cref="TableInitParams.OutputSchema"/>, or
    /// <see cref="TableInitParams.ProjectedSchema"/> when this function honors projection
    /// pushdown), and/or call <paramref name="output"/>.Finish() once there is nothing left to
    /// produce (ends the stream). Optionally call <paramref name="output"/>.ClientLog(...) any
    /// number of times to emit worker log messages visible via DuckDB's <c>duckdb_logs()</c>.</summary>
    void Produce(OutputCollector output);
}
