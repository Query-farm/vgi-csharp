using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Table;

/// <summary>
/// An <see cref="ITableFunctionProducer"/> whose turn is asynchronous — for a producer that awaits
/// I/O (an HTTP API, a database driver) instead of blocking a thread on it. Return one from
/// <see cref="ITableFunction.CreateProducer"/> exactly as a synchronous producer: the SDK awaits
/// <see cref="ProduceAsync"/> once per client tick, with the same contract as
/// <see cref="ITableFunctionProducer.Produce"/> (emit at most once and/or finish).
/// </summary>
/// <remarks>
/// <see cref="ITableFunctionProducer.Produce"/> is implemented for you (it blocks on
/// <see cref="ProduceAsync"/>), so code that drives a producer synchronously keeps working; the
/// SDK's own dispatch never calls it for an asynchronous producer.
/// </remarks>
public interface IAsyncTableFunctionProducer : ITableFunctionProducer
{
    /// <summary>Produces this turn's contribution — see <see cref="ITableFunctionProducer.Produce"/>.</summary>
    /// <param name="output">Where to emit the batch and/or finish.</param>
    /// <param name="cancellationToken">Cancelled when the client abandons the stream.</param>
    Task ProduceAsync(OutputCollector output, CancellationToken cancellationToken);

    void ITableFunctionProducer.Produce(OutputCollector output) =>
        ProduceAsync(output, CancellationToken.None).GetAwaiter().GetResult();
}
