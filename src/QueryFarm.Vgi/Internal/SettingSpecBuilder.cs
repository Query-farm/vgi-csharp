using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>Builds a <see cref="SettingSpec"/>'s wire-shaped <c>Type</c>/<c>DefaultValue</c> bytes
/// from a plain Arrow type plus an optional single-element default array — see
/// <see cref="Worker.RegisterSetting"/>, the only caller.</summary>
public static class SettingSpecBuilder
{
    public static SettingSpec Build(string name, string description, IArrowType type, IArrowArray? defaultValue)
    {
        var valueSchema = new Schema([new Field("value", type, nullable: true)], metadata: null);
        byte[]? defaultBytes = null;
        if (defaultValue is not null)
        {
            defaultBytes = RecordBatchIpc.Write(new RecordBatch(valueSchema, [defaultValue], defaultValue.Length));
        }

        return new SettingSpec
        {
            Name = name,
            Description = description,
            Type = SchemaIpc.WriteSchemaOnly(valueSchema),
            DefaultValue = defaultBytes,
        };
    }
}
