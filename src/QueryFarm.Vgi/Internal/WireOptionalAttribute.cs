namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Marks a property of an embedded-IPC record as a column APPENDED to the wire shape after the
/// original fields, so peers on both sides of the change interoperate:
/// <list type="bullet">
/// <item><b>Encode</b>: the column is emitted as <c>nullable</c> (whatever the CLR type), matching
/// the reference schema, so a peer that types the column as a nullable flag accepts it.</item>
/// <item><b>Decode</b>: the column is looked up by NAME rather than by position. A batch from an
/// older peer that lacks the column, or carries a null in it, leaves the property at its CLR
/// default (<see langword="false"/> for a <see cref="bool"/>).</item>
/// </list>
/// Every such property must come after all the record's positional properties — <see cref="EmbeddedIpc"/>
/// checks this when it first lays the type out.
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
internal sealed class WireOptionalAttribute : Attribute
{
}
