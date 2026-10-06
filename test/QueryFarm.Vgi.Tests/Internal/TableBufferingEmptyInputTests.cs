using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Empty input to a table-buffering function (vgi 63eb257): the extension inits the
/// TABLE_BUFFERING phase itself, then calls combine with an EMPTY <c>state_ids</c> list, then
/// finalizes -- so a whole-input reduction can answer for no input. combine must accept the
/// empty list and reach the function, not refuse it or skip it.
/// </summary>
public class TableBufferingEmptyInputTests
{
    private sealed class RecordingReduction : ITableBufferingFunction
    {
        public List<int> CombinedCounts { get; } = [];

        public string Name => "empty_reduction";

        public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

        public Schema ResolveOutputSchema(TableInOutBindParams bindParams) => OutputSchema;

        public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams) => processParams.ExecutionId;

        public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams)
        {
            CombinedCounts.Add(stateIds.Count);
            return [combineParams.ExecutionId];
        }

        public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
            throw new NotSupportedException();
    }

    [Fact]
    public async Task CombineAcceptsAnEmptyStateIdList()
    {
        var function = new RecordingReduction();
        var registry = new CatalogRegistry();
        registry.RegisterTableBuffering(function);
        var service = new VgiServiceImpl(registry);
        var executionId = Guid.NewGuid().ToByteArray();

        // What init(phase=TABLE_BUFFERING) persists -- the extension now runs that init itself on
        // empty input, since no Sink thread ever did.
        new FunctionStorage(executionId).WriteSingle(
            "__system__", "bind_context",
            EmbeddedIpc.Encode(new BindRequest { FunctionName = function.Name, FunctionType = FunctionType.TableBuffering }));

        var result = await service.TableBufferingCombineAsync(new TableBufferingCombineRequest
        {
            FunctionName = function.Name,
            ExecutionId = executionId,
            StateIds = [],
        });

        Assert.Equal([0], function.CombinedCounts);
        Assert.Equal(executionId, Assert.Single(result.FinalizeStateIds));
    }
}
