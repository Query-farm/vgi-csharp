using System.Text.Json;
using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>secret_field()</c> — a zero-argument scalar reading two fields of the statically-resolved
/// <c>vgi_example</c> secret: <c>port</c> (numeric) and <c>secret_string</c> (the "name"). The
/// <see cref="SecretAttribute"/> parameter is invisible in the SQL signature — the C++ extension
/// pre-resolves the secret (declared via <c>RequiredSecrets</c>, derived automatically from this
/// attribute) BEFORE the very first bind call. Backs <c>secret/secret_fields.test</c>.
/// </summary>
public sealed class SecretFieldFunction : ScalarFn
{
    public override string Name => "secret_field";

    public override string Description => "Look up secret fields by name";

    private void Compute(
        [Secret(SecretType = "vgi_example")] IReadOnlyDictionary<string, IArrowArray>? secret,
        [OutputLength] int length,
        StringArray.Builder result)
    {
        var port = SecretArgCodec.FieldString(secret, "port") ?? "";
        var name = SecretArgCodec.FieldString(secret, "secret_string") ?? "";
        var value = $"port={port};name={name}";
        for (var i = 0; i < length; i++)
        {
            result.Append(value);
        }
    }
}

/// <summary>
/// <c>return_secret_value()</c> — a zero-argument scalar returning a JSON object of every resolved
/// <c>vgi_example</c> secret field. Backs <c>secret/secret_scalar.test</c>.
/// </summary>
public sealed class ReturnSecretValueFunction : ScalarFn
{
    public override string Name => "return_secret_value";

    public override string Description => "Return a secret's value";

    private void Compute(
        [Secret(SecretType = "vgi_example")] IReadOnlyDictionary<string, IArrowArray>? secret,
        [OutputLength] int length,
        StringArray.Builder result)
    {
        var fields = new Dictionary<string, object?>();
        if (secret is not null)
        {
            foreach (var (key, array) in secret)
            {
                fields[key] = ScalarArgCodec.ReadScalar(array);
            }
        }

        var json = JsonSerializer.Serialize(fields);
        for (var i = 0; i < length; i++)
        {
            result.Append(json);
        }
    }
}
