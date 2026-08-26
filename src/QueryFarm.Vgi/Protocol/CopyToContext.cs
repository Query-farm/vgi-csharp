namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// A nested struct inside <see cref="BindRequest"/>. Property declaration order matches the C++
/// extension's <c>copy_to_type</c> struct field order exactly: format, file_path.
/// </summary>
public sealed class CopyToContext
{
    public string Format { get; set; } = "";

    public string FilePath { get; set; } = "";
}
