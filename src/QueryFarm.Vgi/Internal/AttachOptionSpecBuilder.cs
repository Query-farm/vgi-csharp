using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>Builds an <see cref="AttachOptionSpec"/>'s wire-shaped <c>Type</c>/<c>DefaultValue</c>
/// bytes from a plain Arrow type plus an optional single-element default array. Unlike
/// <see cref="SettingSpec"/> (worker-global, registered via <c>Worker.RegisterSetting</c>), an
/// attach option is declared PER CATALOG — a fixture builds a list of these directly and assigns
/// them to <see cref="CatalogInfo.AttachOptionSpecs"/> (via <c>.Select(EmbeddedIpc.Encode).ToList()</c>,
/// same as <c>CatalogAttachResult.Settings</c> does) when constructing the <see cref="CatalogInfo"/>
/// it passes to <c>Worker.RegisterCatalog</c>. Mirrors <see cref="SettingSpecBuilder"/> exactly,
/// plus the <paramref name="required"/> flag <see cref="AttachOptionSpec"/> adds.</summary>
public static class AttachOptionSpecBuilder
{
    public static AttachOptionSpec Build(string name, string description, IArrowType type, IArrowArray? defaultValue, bool required = false)
    {
        var valueSchema = new Schema([new Field("value", type, nullable: true)], metadata: null);
        byte[]? defaultBytes = null;
        if (defaultValue is not null)
        {
            defaultBytes = RecordBatchIpc.Write(new RecordBatch(valueSchema, [defaultValue], defaultValue.Length));
        }

        return new AttachOptionSpec
        {
            Name = name,
            Description = description,
            Type = SchemaIpc.WriteSchemaOnly(valueSchema),
            DefaultValue = defaultBytes,
            Required = required,
        };
    }
}
