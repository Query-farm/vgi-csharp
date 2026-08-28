using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.AttachOptions;

/// <summary>
/// <c>attach_options.echo_attach_options()</c> — returns the attach-time option values passed at
/// <c>ATTACH</c>, one row, one column per declared option
/// (<see cref="AttachOptionEntries.All"/>). The values come from <c>AttachOpaqueData</c> (see
/// <see cref="AttachOptionsSetup.Handle"/>, which put them there via
/// <see cref="Protocol.AttachContext.ExtraOpaqueData"/>) — decoded fresh on every call, nothing
/// cached on <c>self</c>, so this is safe under pooled-worker reuse and stateless (HTTP) dispatch.
/// </summary>
public sealed class EchoAttachOptionsFunction : ITableFunction
{
    public static readonly Schema FixedSchema = new(
        AttachOptionEntries.All.Select(e => new Field(e.Name, e.Type, nullable: true)),
        metadata: null);

    public string Name => "echo_attach_options";

    public string SchemaName => "main";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema => FixedSchema;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(DecodeExtra(initParams.AttachOpaqueData));

    /// <summary>Strips <c>&lt;identity&gt;\0&lt;16-byte GUID&gt;</c> off the front of
    /// <c>attach_opaque_data</c> — see <c>VgiServiceImpl.EncodeIdentity</c>'s doc comment for the
    /// exact envelope layout every <c>Worker.OnAttach</c>-using fixture's functions decode this
    /// same way.</summary>
    private static byte[] DecodeExtra(byte[] attachOpaqueData)
    {
        var separatorIndex = System.Array.IndexOf(attachOpaqueData, (byte)0);
        if (separatorIndex < 0)
        {
            throw new InvalidOperationException("echo_attach_options requires an attach_opaque_data carrying an options payload");
        }

        var extraStart = separatorIndex + 1 + 16;
        if (extraStart > attachOpaqueData.Length)
        {
            throw new InvalidOperationException("echo_attach_options requires an attach_opaque_data carrying an options payload");
        }

        return attachOpaqueData[extraStart..];
    }

    private sealed class Producer(byte[] serializedEchoBatch) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                output.Emit(RecordBatchIpc.Read(serializedEchoBatch));
            }

            output.Finish();
        }
    }
}
