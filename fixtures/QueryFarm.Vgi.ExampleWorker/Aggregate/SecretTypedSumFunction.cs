using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary><c>secret_typed_sum(value BIGINT)</c> — sums an integer column, but the RESULT TYPE is
/// chosen from the statically-resolved <c>vgi_example</c> secret's <c>use_ssl</c> field: DOUBLE when
/// true, BIGINT when false. Declares a static <see cref="RequiredSecrets"/> requirement, so the C++
/// extension pre-resolves the secret BEFORE the very first <c>aggregate_bind</c> call — an aggregate
/// supports ONLY this static path, never the table/table-in-out-only dynamic two-phase retry (secret
/// *values* are bind-time-only; <c>Update</c>/<c>Combine</c>/<c>Finalize</c> never see them again, so
/// the bind-time type decision is threaded through the output schema instead). Backs
/// <c>secret/secret_aggregate.test</c>.</summary>
public sealed class SecretTypedSumFunction : IAggregateFunction
{
    private const string SecretType = "vgi_example";

    public string Name => "secret_typed_sum";

    public string Description => "Sum an integer column; the result type is chosen from a secret";

    public IReadOnlyList<string> Categories => ["aggregation", "secret"];

    public Schema ArgumentsSchema { get; } = new([new Field("value", Int64Type.Default, nullable: true)], metadata: null);

    /// <summary>Static declared return type is the dynamic-return-type marker (reported as
    /// <c>ANY</c> in <c>duckdb_functions()</c> — <c>aggregate/function_registration.test</c> pins
    /// this) since the REAL type is only known once the secret is resolved at bind time — see
    /// <see cref="ResolveOutputSchema"/>.</summary>
    public Schema OutputSchema { get; } = Internal.AnyScalarSchema.SingleResult();

    public IReadOnlyList<RequiredSecret> RequiredSecrets => [new RequiredSecret { SecretType = SecretType }];

    public Schema ResolveOutputSchema(AggregateBindParams bindParams)
    {
        var resolved = SecretArgCodec.Decode(bindParams.Secrets);
        var secret = SecretArgCodec.FindByType(resolved, SecretType);
        var useSsl = SecretArgCodec.FieldValue(secret, "use_ssl") is true;
        var resultType = useSsl ? (IArrowType)DoubleType.Default : Int64Type.Default;
        return new Schema([new Field("result", resultType, nullable: true)], metadata: null);
    }

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var column = (Int64Array)inputColumns.Column(0);
        for (var i = 0; i < groupIds.Length; i++)
        {
            var value = column.GetValue(i);
            if (value is null)
            {
                continue;
            }

            var gid = groupIds[i];
            var current = states.TryGetValue(gid, out var bytes) ? BitConverter.ToDouble(bytes) : 0.0;
            states[gid] = BitConverter.GetBytes(current + value.Value);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams) =>
        BitConverter.GetBytes(BitConverter.ToDouble(source) + (target is null ? 0.0 : BitConverter.ToDouble(target)));

    public IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams)
    {
        var type = outputSchema.GetFieldByIndex(0).DataType;
        var values = states.Select(s => s is null ? (object?)null : BitConverter.ToDouble(s)).ToList();
        return AnyArrayBuilder.Build(type, values);
    }
}
