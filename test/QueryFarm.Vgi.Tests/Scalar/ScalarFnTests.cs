using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;
using Xunit;

namespace QueryFarm.Vgi.Tests.Scalar;

/// <summary>Exercises <see cref="ScalarFn"/>'s reflection dispatch (<c>ComputePlan</c>, internal
/// to the <c>QueryFarm.Vgi.Scalar</c> assembly) end to end through the public
/// <see cref="IScalarFunction"/> surface: schema derivation (including <c>[ConstParam]</c>'s
/// <c>vgi_const</c> field metadata), const-argument decoding from the same
/// <c>struct(positional_0, ...)</c> wire shape the C++ extension sends, and per-batch dispatch via
/// <c>[Param]</c>/<c>[OutputLength]</c>.</summary>
public class ScalarFnTests
{
    /// <summary>A minimal function exercising every marker attribute at once: one <c>[Param]</c>
    /// column, one <c>[ConstParam]</c>, and <c>[OutputLength]</c> — <c>result[i] = value[i] +
    /// addend</c>, or <c>-1</c> for a null input row.</summary>
    private sealed class AddConstFixture : ScalarFn
    {
        public override string Name => "test_add_const";

        public int LastRows { get; private set; }

        private void Compute([Param] Int64Array value, [ConstParam] long addend, [OutputLength] int rows, Int64Array.Builder result)
        {
            LastRows = rows;
            for (var i = 0; i < value.Length; i++)
            {
                result.Append(value.IsNull(i) ? -1 : value.GetValue(i)!.Value + addend);
            }
        }
    }

    [Fact]
    public void ArgumentsSchema_MarksParamAndConstParamCorrectly()
    {
        var fn = new AddConstFixture();

        Assert.Equal(2, fn.ArgumentsSchema.FieldsList.Count);

        var valueField = fn.ArgumentsSchema.GetFieldByIndex(0);
        Assert.Equal(Int64Type.Default, valueField.DataType);
        Assert.False(valueField.HasMetadata && valueField.Metadata.ContainsKey(VgiWireMetadata.ConstKey));

        var addendField = fn.ArgumentsSchema.GetFieldByIndex(1);
        Assert.Equal(Int64Type.Default, addendField.DataType);
        Assert.True(addendField.HasMetadata);
        Assert.Equal(VgiWireMetadata.ConstTrueValue, addendField.Metadata[VgiWireMetadata.ConstKey]);
    }

    [Fact]
    public void OutputSchema_IsSingleFieldNamedResult_MatchingBuilderType()
    {
        var fn = new AddConstFixture();
        Assert.Single(fn.OutputSchema.FieldsList);
        Assert.Equal("result", fn.OutputSchema.GetFieldByIndex(0).Name);
        Assert.Equal(Int64Type.Default, fn.OutputSchema.GetFieldByIndex(0).DataType);
    }

    [Fact]
    public void Process_DispatchesParamConstParamAndOutputLength_WithNullPropagation()
    {
        var fn = new AddConstFixture();

        var valueBuilder = new Int64Array.Builder();
        valueBuilder.Append(10).AppendNull().Append(30);
        var input = new RecordBatch(fn.ArgumentsSchema, [valueBuilder.Build()], 3);

        var constArgsBytes = EncodeConstArgs(("positional_0", 5L));

        var result = fn.Process(new ScalarProcessParams
        {
            Input = input,
            OutputSchema = fn.OutputSchema,
            Arguments = constArgsBytes,
        });

        var resultArray = (Int64Array)result.Column(0);
        Assert.Equal(3, result.Length);
        Assert.Equal(3, fn.LastRows);
        Assert.Equal(15, resultArray.GetValue(0));
        Assert.Equal(-1, resultArray.GetValue(1));
        Assert.Equal(35, resultArray.GetValue(2));
    }

    [Fact]
    public void ComputePlan_IsCachedPerType_NotRebuiltPerInstance()
    {
        // Two independent instances of the same ScalarFn subclass must derive byte-identical
        // schemas — proves the per-type reflection plan is shared/cached, not redone (and
        // reflected) on every `new`.
        var a = new AddConstFixture();
        var b = new AddConstFixture();

        Assert.True(a.ArgumentsSchema.Equals(b.ArgumentsSchema));
        Assert.True(a.OutputSchema.Equals(b.OutputSchema));
    }

    /// <summary>Builds the exact wire shape <c>BindRequest.Arguments</c>/<c>ScalarProcessParams.Arguments</c>
    /// carries: an IPC stream with schema <c>{args: struct(...)}</c>, one row, matching
    /// <c>vgi_arrow_utils.cpp</c>'s <c>BuildArgumentsFromValues</c> — the field name is literally
    /// what <see cref="ScalarArgCodec.DecodeConstStruct"/> parses back out.</summary>
    private static byte[] EncodeConstArgs(params (string Name, long Value)[] fields)
    {
        var structFields = fields.Select(f => new Field(f.Name, Int64Type.Default, nullable: true)).ToList();
        var structType = new StructType(structFields);
        var children = fields.Select(f =>
        {
            var b = new Int64Array.Builder();
            b.Append(f.Value);
            return (IArrowArray)b.Build();
        }).ToList();
        var validity = new ArrowBuffer.BitmapBuilder();
        validity.Append(true);
        var structArray = new StructArray(structType, 1, children, validity.Build());

        var argsSchema = new Schema([new Field("args", structType, nullable: false)], metadata: null);
        var argsBatch = new RecordBatch(argsSchema, [structArray], 1);

        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, argsSchema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteRecordBatch(argsBatch);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }
}
