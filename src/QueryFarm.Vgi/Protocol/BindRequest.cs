namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>bind</c> RPC's packed request — also embedded a SECOND level deep inside
/// <see cref="InitRequest.BindCall"/> (see that property's doc comment and
/// <see cref="Internal.EmbeddedIpc"/>).
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING: this type is decoded positionally against the
/// incoming Arrow batch's columns (by index, not by looking up column names) — it must match the
/// C++ extension's <c>BuildBindRequest</c> field order EXACTLY: function_name, arguments,
/// function_type, input_schema, settings, secrets, attach_opaque_data, transaction_opaque_data,
/// resolved_secrets_provided, at_unit, at_value, copy_from, copy_to, schema_name. See
/// <c>vgi_rpc_types.cpp</c>'s own comment on this exact historical bug.
/// </summary>
public sealed class BindRequest
{
    public string FunctionName { get; set; } = "";

    public byte[] Arguments { get; set; } = [];

    public FunctionType FunctionType { get; set; }

    public byte[]? InputSchema { get; set; }

    public byte[]? Settings { get; set; }

    public byte[]? Secrets { get; set; }

    public byte[]? AttachOpaqueData { get; set; }

    public byte[]? TransactionOpaqueData { get; set; }

    public bool ResolvedSecretsProvided { get; set; }

    public string? AtUnit { get; set; }

    public string? AtValue { get; set; }

    public CopyFromContext? CopyFrom { get; set; }

    public CopyToContext? CopyTo { get; set; }

    public string? SchemaName { get; set; }
}
