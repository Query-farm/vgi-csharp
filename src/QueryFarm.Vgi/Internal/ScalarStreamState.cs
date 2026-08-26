using Apache.Arrow;
using QueryFarm.Vgi.Scalar;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The per-call streaming state for a scalar function's <c>init</c>-opened exchange: the client
/// (DuckDB) sends one batch of positional argument columns per turn, and this replies with
/// exactly one batch of the function's single output column — the lockstep shape
/// <see cref="ExchangeState"/> models directly. Never calls <see cref="OutputCollector.Finish"/>
/// itself; the client ends the exchange by closing its input stream (DuckDB does this in the
/// scalar function's local-state destructor).
///
/// Carries the RESOLVED (per-call) output schema and the raw const-argument/settings bytes from
/// THIS bind call, rather than reaching back into <paramref name="function"/>'s own state — the
/// same <see cref="IScalarFunction"/> instance is shared across every call site of that function,
/// so nothing about one call (its const arguments, its dynamically-resolved output type) may be
/// cached on the singleton itself; it all lives here, one <see cref="ScalarStreamState"/> per
/// <c>init</c>.
/// </summary>
public sealed class ScalarStreamState(
    IScalarFunction function,
    Schema outputSchema,
    byte[] arguments,
    byte[]? settings,
    byte[]? secrets) : ExchangeState
{
    public override Task ExchangeAsync(AnnotatedBatch input, OutputCollector output, ICallContext? ctx, CancellationToken cancellationToken)
    {
        var result = function.Process(new ScalarProcessParams
        {
            Input = input.Batch,
            OutputSchema = outputSchema,
            Arguments = arguments,
            Settings = settings,
            Secrets = secrets,
        });
        output.Emit(result, function.CacheControlMetadata);
        return Task.CompletedTask;
    }
}
