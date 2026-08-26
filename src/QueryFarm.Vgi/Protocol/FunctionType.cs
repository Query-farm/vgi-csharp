namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The kind of function a <see cref="BindRequest"/>/<see cref="FunctionInfo"/> describes.
/// Wire-encoded as <c>dictionary(int16, utf8)</c> by member name (default enum wire naming),
/// producing exactly the strings the C++ extension's <c>ParseVgiFunctionType</c> recognizes:
/// "SCALAR", "TABLE", "AGGREGATE", "TABLE_BUFFERING".
/// </summary>
public enum FunctionType
{
    Scalar,
    Table,
    Aggregate,
    TableBuffering,
}
