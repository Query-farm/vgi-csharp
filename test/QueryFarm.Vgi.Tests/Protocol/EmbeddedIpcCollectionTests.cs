using System.Runtime.CompilerServices;
using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Reflection;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

/// <summary>
/// Regression tests for the intermittent <c>NullReferenceException</c> in
/// <c>ArrowStreamWriter.WriteBufferData</c> that the HTTP fixture worker threw from
/// <c>catalog_schema_contents_functions</c> under load: a batch built only to be written was
/// finalized while its body was being copied (the full mechanism is on
/// <see cref="RecordBatchIpc.Write(Stream, RecordBatch, IReadOnlyDictionary{string, string}?)"/>).
///
/// The bug exists only in optimized code — unoptimized code reports every local live to the end of
/// its method — which is why this project disables quick JIT (see its csproj): under the default
/// tiering these tests would pass until the methods involved happened to be promoted, about ten
/// seconds into a run. They are meaningful in a Release build, which is what CI tests.
/// </summary>
public class EmbeddedIpcCollectionTests
{
    /// <summary>A destination that collects garbage and drains the finalizer queue before every
    /// write, so a batch that is unreachable while its body is being written is finalized in that
    /// window — deterministically, not by chance.</summary>
    private sealed class CollectBeforeEveryWriteStream : MemoryStream
    {
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            Collect();
            base.Write(buffer);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            Collect();
            base.Write(buffer, offset, count);
        }

        private static void Collect()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private static FunctionInfo SampleFunctionInfo() => new()
    {
        Comment = "a comment",
        Tags = new Dictionary<string, string> { ["k"] = "v" },
        Name = "upper_case",
        SchemaPath = ["warehouse", "main"],
        FunctionType = FunctionType.Scalar,
        Arguments = [1, 2],
        OutputSchema = [3, 4],
        ParameterDefaultValues = [5, 6],
        ArgumentMonotonicity = ["STRICTLY_INCREASING", "CONSTANT"],
        Description = "uppercases a string",
        Examples = [new FunctionExample { Sql = "SELECT upper_case('a')", Description = "ex", ExpectedOutput = "A" }],
        Categories = ["string"],
        RequiredSettings = [],
        RequiredSecrets = [new RequiredSecret { SecretType = "s3", Scope = null, SecretName = null }],
    };

    /// <summary>Builds the row the way <see cref="EmbeddedIpc.Encode{T}"/> does, returning it so
    /// that the only reference to it is the argument the caller passes on.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static RecordBatch TemporaryRow(FunctionInfo value)
    {
        var schema = SchemaDerivation.InnerSchemaFor(typeof(FunctionInfo));
        var values = new object?[schema.FieldsList.Count];
        for (var i = 0; i < values.Length; i++)
        {
            var property = typeof(FunctionInfo).GetProperty(ValueCodec.FindClrPropertyName(typeof(FunctionInfo), schema.GetFieldByIndex(i)))!;
            values[i] = property.GetValue(value);
        }

        return ValueCodec.BuildRow(schema, values);
    }

    [Fact]
    public void Write_KeepsATemporaryBatchAlive_UntilItsBodyIsWritten()
    {
        var info = SampleFunctionInfo();
        var expected = EmbeddedIpc.Encode(info);

        using var destination = new CollectBeforeEveryWriteStream();
        RecordBatchIpc.Write(destination, TemporaryRow(info));

        Assert.Equal(expected, destination.ToArray());
    }

    [Fact]
    public void Encode_IsByteStable_WhileTheCollectorRunsConcurrently()
    {
        var info = SampleFunctionInfo();
        var expected = EmbeddedIpc.Encode(info);

        using var stop = new CancellationTokenSource();
        var collector = new Thread(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        })
        { IsBackground = true };
        collector.Start();
        try
        {
            var elapsed = System.Diagnostics.Stopwatch.StartNew();
            for (var iteration = 0; elapsed.Elapsed < TimeSpan.FromSeconds(1.5); iteration++)
            {
                var actual = EmbeddedIpc.Encode(info);
                Assert.True(actual.AsSpan().SequenceEqual(expected), $"iteration {iteration}: encoded bytes differ");
            }
        }
        finally
        {
            stop.Cancel();
            collector.Join();
        }
    }
}
