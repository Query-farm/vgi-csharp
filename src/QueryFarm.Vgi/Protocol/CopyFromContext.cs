namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// A nested struct inside <see cref="BindRequest"/> (native Arrow <c>struct</c> — never embedded
/// IPC on its own, since it isn't a top-level RPC parameter). Property declaration order matches
/// the C++ extension's <c>copy_from_type</c> struct field order exactly: format, file_path,
/// expected_schema.
/// </summary>
public sealed class CopyFromContext
{
    public string Format { get; set; } = "";

    public string FilePath { get; set; } = "";

    public byte[] ExpectedSchema { get; set; } = [];
}
