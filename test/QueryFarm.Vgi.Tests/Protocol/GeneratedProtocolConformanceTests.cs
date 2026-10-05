using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Tests.Generated;
using QueryFarm.VgiRpc.Reflection;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

/// <summary>
/// Checks this port's wire schemas against the protocol's own, field for field.
/// </summary>
/// <remarks>
/// <para>The protocol records in <c>Protocol/Generated/VgiProtocolTypes.g.cs</c> are generated from
/// vgi-python, but this port does not send those declarations — it sends whatever
/// <see cref="SchemaDerivation"/> derives from them, and a CLR type the generator chose wrongly
/// derives a different Arrow type with nothing failing until the C++ client's strict
/// <c>arrow::Schema::Equals</c> rejects a response at runtime. <see cref="VgiProtocolSchemas"/> is
/// generated from the same protocol but as Arrow schemas, so it is the second description these
/// tests compare against.</para>
/// <para>They also cover what is still written by hand: <see cref="IVgiService"/>'s flat method
/// parameters, and which record each of its methods returns.</para>
/// <para>Both generated files are regenerated together from vgi-python:
/// <c>uv run --project ~/Development/vgi-python python scripts/regen_generated.py</c>.</para>
/// </remarks>
public class GeneratedProtocolConformanceTests
{
    private static readonly Assembly s_vgi = typeof(IVgiService).Assembly;

    public static TheoryData<string> RecordNames() => [.. VgiProtocolSchemas.Records.Keys];

    /// <summary>
    /// What a record actually puts on the wire: a default instance encoded through
    /// <see cref="EmbeddedIpc"/> — the path every embedded record takes — with the schema read
    /// back off the bytes, so anything the encoder adjusts (a <c>[WireOptional]</c> column goes out
    /// nullable) is part of what is compared.
    /// </summary>
    [Theory]
    [MemberData(nameof(RecordNames))]
    public void Record_SerializesWithTheProtocolSchema(string name)
    {
        var type = s_vgi.GetType($"{typeof(IVgiService).Namespace}.{name}")
            ?? throw new InvalidOperationException($"no generated type '{name}'; regenerate VgiProtocolTypes.g.cs");

        AssertSchemasEqual(VgiProtocolSchemas.Records[name], WireSchemaOf(type), name);
    }

    public static TheoryData<string> ServiceMethodNames() =>
        [.. ServiceRegistry.GetMethods(typeof(IVgiService)).Keys];

    [Theory]
    [MemberData(nameof(ServiceMethodNames))]
    public void ServiceMethod_TakesTheProtocolParams(string wireName)
    {
        var method = ServiceRegistry.GetMethods(typeof(IVgiService))[wireName];

        Assert.True(
            VgiProtocolSchemas.MethodParams.TryGetValue(wireName, out var expected),
            $"IVgiService declares '{wireName}', which the protocol does not define.");
        AssertSchemasEqual(expected, method.ParamsSchema, $"{wireName} params");
    }

    [Theory]
    [MemberData(nameof(ServiceMethodNames))]
    public void ServiceMethod_ReturnsTheProtocolResult(string wireName)
    {
        var method = ServiceRegistry.GetMethods(typeof(IVgiService))[wireName];
        if (method.Kind != RpcMethodKind.Unary)
        {
            return;
        }

        if (VgiProtocolSchemas.MethodResults.TryGetValue(wireName, out var expected))
        {
            AssertSchemasEqual(expected, WireSchemaOf(method.ResultClrType), $"{wireName} result");
        }
        else
        {
            // Nothing, or raw IPC bytes. A method may return a protocol record there instead —
            // catalog_table_scan_branches_get returns ScanBranchesResult — because an embedded
            // record IS an IPC stream; that record's own schema is checked by the Records test.
            var resultType = method.ResultClrType;
            Assert.True(
                resultType == typeof(void) || resultType == typeof(byte[]) ||
                (resultType.Namespace == typeof(IVgiService).Namespace && VgiProtocolSchemas.Records.ContainsKey(resultType.Name)),
                $"'{wireName}' returns no record in the protocol, but IVgiService returns {resultType}.");
        }
    }

    private static Schema WireSchemaOf(Type recordType)
    {
        var instance = Activator.CreateInstance(recordType)!;
        var encode = typeof(EmbeddedIpc).GetMethod(nameof(EmbeddedIpc.Encode))!.MakeGenericMethod(recordType);
        var bytes = (byte[])encode.Invoke(null, [instance])!;
        using var reader = new ArrowStreamReader(new MemoryStream(bytes));
        return reader.Schema;
    }

    private static void AssertSchemasEqual(Schema expected, Schema actual, string what)
    {
        var want = expected.FieldsList.Select(Describe).ToList();
        var got = actual.FieldsList.Select(Describe).ToList();
        Assert.True(
            want.SequenceEqual(got),
            $"{what}: the derived schema differs from the protocol's.\n" +
            $"  protocol: {string.Join(", ", want)}\n" +
            $"  derived:  {string.Join(", ", got)}\n" +
            "Regenerate from vgi-python (scripts/regen_generated.py); if the generated types are " +
            "current, the generator's CLR mapping disagrees with SchemaDerivation.");
    }

    /// <summary>A field as text: name, type and nullability, recursively — dictionary ids and
    /// metadata deliberately left out, as they are not part of the protocol.</summary>
    private static string Describe(Field field) =>
        $"{field.Name}: {Describe(field.DataType)}{(field.IsNullable ? "" : " not null")}";

    private static string Describe(IArrowType type) => type switch
    {
        ListType list => $"list<{Describe(list.ValueField)}>",
        MapType map => $"map<{Describe(map.KeyField)}, {Describe(map.ValueField)}>",
        StructType st => $"struct<{string.Join(", ", st.Fields.Select(Describe))}>",
        DictionaryType dict => $"dictionary<{Describe(dict.IndexType)}, {Describe(dict.ValueType)}, ordered={dict.Ordered}>",
        TimestampType ts => $"timestamp[{ts.Unit}, {ts.Timezone ?? "naive"}]",
        FixedSizeBinaryType fixedSize => $"fixed_size_binary[{fixedSize.ByteWidth}]",
        _ => type.Name,
    };
}
