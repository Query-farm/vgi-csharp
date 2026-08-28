using Apache.Arrow.Ipc;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Reflection;

namespace QueryFarm.Vgi.BadEnumWorker;

/// <summary>
/// Encodes a <see cref="FunctionInfo"/> exactly like <c>Internal.EmbeddedIpc.Encode&lt;FunctionInfo&gt;</c>
/// does, EXCEPT the <c>null_handling</c> field's value is a member of a wholly unrelated decoy
/// enum instead of the real <see cref="FunctionNullHandling"/> — producing the wire string
/// <c>"WEIRD"</c>, an unrecognized <c>null_handling</c> value the C++ catalog-metadata parser must
/// reject (see <c>test/sql/integration/bad_enum.test</c>).
///
/// Why this works: the generic reflection encoder (<see cref="SchemaDerivation.InnerSchemaFor"/> +
/// <see cref="ValueCodec.BuildRow"/>) derives the wire TYPE for an enum field from the CLR
/// property's DECLARED type (any enum -&gt; the same <c>DictionaryType(int16, string)</c> shape),
/// but the VALUE encoder (<c>ValueCodec</c>'s internal <c>BuildEnumArray</c>) reflects on the
/// boxed VALUE's OWN runtime type to find its member name — not the field's declared type. So
/// substituting a genuine member of a different enum at just this one slot serializes cleanly
/// (no core-library or vgi-rpc-csharp change needed) while every other <see cref="FunctionInfo"/>
/// field encodes normally, straight from the real object's properties.
/// </summary>
internal static class BadEnumFunctionInfoEncoder
{
    private enum BogusNullHandling
    {
        WEIRD,
    }

    public static byte[] Encode(FunctionInfo value)
    {
        var clrType = typeof(FunctionInfo);
        var innerSchema = SchemaDerivation.InnerSchemaFor(clrType);
        var rowValues = new object?[innerSchema.FieldsList.Count];
        for (var i = 0; i < rowValues.Length; i++)
        {
            var field = innerSchema.GetFieldByIndex(i);
            var propertyName = ValueCodec.FindClrPropertyName(clrType, field);
            rowValues[i] = propertyName == nameof(FunctionInfo.NullHandling)
                ? BogusNullHandling.WEIRD
                : clrType.GetProperty(propertyName)!.GetValue(value);
        }

        var row = ValueCodec.BuildRow(innerSchema, rowValues);

        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, innerSchema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteRecordBatch(row);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }
}
