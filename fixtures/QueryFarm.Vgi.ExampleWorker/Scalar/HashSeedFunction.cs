using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>hash_seed(seed: BIGINT [const]) -&gt; BIGINT</c> — despite the name, not a real hash: emits
/// <c>seed + row_index</c> for each row of the batch. <c>test/sql/integration/scalar/hash_seed.test</c>
/// only ever observes <c>row_index == 0</c> (DuckDB folds a call with an all-constant argument list
/// down to a single invocation and broadcasts the one result), but the row_index term is
/// implemented regardless to match the documented/vgi-java behavior.
/// </summary>
public sealed class HashSeedFunction : ScalarFn
{
    public override string Name => "hash_seed";

    public override string Description => "Generate deterministic integers from a constant seed";

    private void Compute([ConstParam] long seed, [OutputLength] int rows, Int64Array.Builder result)
    {
        for (var i = 0; i < rows; i++)
        {
            result.Append(seed + i);
        }
    }
}
