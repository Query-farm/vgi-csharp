using Apache.Arrow;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.Internal;

/// <summary>Builds a <see cref="SecretTypeSpec"/>'s wire-shaped <c>ParametersSchema</c> bytes from a
/// plain Arrow schema — see <see cref="Worker.RegisterSecretType"/>, the only caller. Mirrors
/// <see cref="SettingSpecBuilder"/>.</summary>
public static class SecretTypeSpecBuilder
{
    public static SecretTypeSpec Build(string name, string description, Schema parametersSchema) => new()
    {
        Name = name,
        Description = description,
        ParametersSchema = SchemaIpc.WriteSchemaOnly(parametersSchema),
    };
}
