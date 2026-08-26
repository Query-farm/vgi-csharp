using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>binary_packet(header: BLOB [const], payload: BLOB, config: STRUCT(label VARCHAR, version
/// BIGINT) [const]) -&gt; BLOB</c> — <c>header || payload || utf8(config.label) || (byte)(config.version
/// &amp; 0xFF)</c>. Implements <see cref="IScalarFunction"/> directly: a struct-typed const value
/// isn't a shape <see cref="ScalarFn"/>'s reflection auto-binds (see <c>ComputePlan</c>'s doc
/// comment), so this decodes it by hand via <see cref="ScalarArgCodec"/>. Since <c>header</c> and
/// <c>config</c> are BOTH const, DuckDB erases them from the per-row input batch entirely — the
/// batch's only column is <c>payload</c>.
/// </summary>
public sealed class BinaryPacketFunction : IScalarFunction
{
    private static readonly StructType ConfigType = new(new[]
    {
        new Field("label", StringType.Default, nullable: true),
        new Field("version", Int64Type.Default, nullable: true),
    });

    public string Name => "binary_packet";

    public string Description => "Build binary packets with header, payload, and config";

    public Schema ArgumentsSchema { get; } = new(
        [
            ConstField("header", BinaryType.Default),
            new Field("payload", BinaryType.Default, nullable: true),
            ConstField("config", ConfigType),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", BinaryType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var consts = ScalarArgCodec.DecodeConstStruct(processParams.Arguments);

        var header = consts.TryGetValue(0, out var headerArr) && headerArr is BinaryArray ha && !ha.IsNull(0)
            ? ha.GetBytes(0).ToArray()
            : [];

        byte[] label = [];
        long version = 0;
        if (consts.TryGetValue(1, out var configArr) && configArr is StructArray config && !config.IsNull(0))
        {
            var fields = ((StructType)config.Data.DataType).Fields;
            for (var i = 0; i < fields.Count; i++)
            {
                switch (fields[i].Name)
                {
                    case "label" when config.Fields[i] is StringArray labelArray && !labelArray.IsNull(0):
                        label = Encoding.UTF8.GetBytes(labelArray.GetString(0));
                        break;
                    case "version" when config.Fields[i] is Int64Array versionArray && !versionArray.IsNull(0):
                        version = versionArray.GetValue(0)!.Value;
                        break;
                }
            }
        }

        var payload = processParams.Input.ColumnCount > 0 ? processParams.Input.Column(0) as BinaryArray : null;
        var length = processParams.Input.Length;
        var builder = new BinaryArray.Builder();
        var versionByte = (byte)(version & 0xFF);

        for (var i = 0; i < length; i++)
        {
            var payloadBytes = payload is not null && !payload.IsNull(i) ? payload.GetBytes(i).ToArray() : [];
            var packet = new byte[header.Length + payloadBytes.Length + label.Length + 1];
            var offset = 0;
            header.CopyTo(packet, offset);
            offset += header.Length;
            payloadBytes.CopyTo(packet, offset);
            offset += payloadBytes.Length;
            label.CopyTo(packet, offset);
            offset += label.Length;
            packet[offset] = versionByte;
            builder.Append(packet);
        }

        return new RecordBatch(OutputSchema, [builder.Build()], length);
    }

    private static Field ConstField(string name, IArrowType type) =>
        new(name, type, nullable: true, new Dictionary<string, string> { [VgiWireMetadata.ConstKey] = VgiWireMetadata.ConstTrueValue });
}
