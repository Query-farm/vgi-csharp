// Copyright 2026 Query Farm LLC - https://query.farm
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>A pure optional widening cast with complete, reusable bind state.</summary>
public sealed class ProjectionSequenceFunction : ITableFunction
{
    public string Name => "projection_sequence";
    public Schema ArgumentsSchema { get; } = new([], null);
    public Schema OutputSchema { get; } = new([new Field("n", Int32Type.Default, true)], null);
    public IReadOnlyList<string> ProjectionExpressionProfiles => [ProjectionExpressions.Profile];
    public BindRepeatability Repeatability(TableBindParams parameters) => BindRepeatability.StableWithinQuery;

    public TableFunctionProjectionResponse Projection(TableFunctionProjectionRequest request)
    {
        var original = EmbeddedIpc.Decode<BindResponse>(request.BindResult);
        var source = SchemaIpc.ReadSchemaOnly(original.OutputSchema);
        var result = SchemaIpc.ReadSchemaOnly(request.ResultSchema);
        var indices = new List<long>();
        for (int i = 0; i < request.Expressions.Count; i++)
        {
            var expression = EmbeddedIpc.Decode<SqlExpression>(request.Expressions[i]);
            var context = expression.EvaluationContext;
            if (expression.Sql == "CAST(\"n\" AS BIGINT)" && context.Timezone == "UTC"
                && context.Collation == "binary" && context.DefaultOrder == "asc"
                && context.DefaultNullOrder == "nulls_last" && context.PreserveInsertionOrder
                && result.GetFieldByIndex(i).DataType is Int64Type && result.GetFieldByIndex(i).IsNullable)
            {
                indices.Add(i);
            }
        }
        if (indices.Count == 0)
        {
            return new();
        }
        return new()
        {
            AcceptedIndices = indices,
            BindResult = EmbeddedIpc.Encode(new BindResponse
            {
                OutputSchema = SchemaIpc.WriteSchemaOnly(new Schema(
                    source.FieldsList.Concat(indices.Select(i => result.GetFieldByIndex((int)i))), source.Metadata)),
                OpaqueData = System.Text.Encoding.ASCII.GetBytes(
                    indices.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                ProjectionExpressionProfiles = original.ProjectionExpressionProfiles,
                Repeatability = original.Repeatability,
            }),
        };
    }

    public ITableFunctionProducer CreateProducer(TableInitParams parameters)
    {
        int appended = parameters.BindOpaqueData is { Length: > 0 } state
            ? int.Parse(System.Text.Encoding.ASCII.GetString(state), System.Globalization.CultureInfo.InvariantCulture)
            : 0;
        if (parameters.OutputSchema.FieldsList.Count != 1 + appended)
        {
            throw new InvalidOperationException("Projection state differs from output schema.");
        }
        return new Producer(parameters.OutputSchema);
    }

    private sealed class Producer(Schema schema) : ITableFunctionProducer
    {
        private int _offset;
        public void Produce(OutputCollector output)
        {
            if (_offset == 10)
            {
                output.Finish();
                return;
            }
            var arrays = new List<IArrowArray>();
            foreach (var field in schema.FieldsList)
            {
                if (field.Name == "n")
                {
                    arrays.Add(new Int32Array.Builder().AppendRange(Enumerable.Range(_offset, 5)).Build());
                }
                else
                {
                    arrays.Add(new Int64Array.Builder().AppendRange(Enumerable.Range(_offset, 5).Select(n => (long)n)).Build());
                }
            }
            _offset += 5;
            output.Emit(new RecordBatch(schema, arrays, 5));
        }
    }
}
