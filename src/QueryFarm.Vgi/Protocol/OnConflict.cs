namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>ON CONFLICT</c> behavior requested by a <c>CREATE SCHEMA</c>/<c>CREATE VIEW</c> DDL call.
/// Wire-encoded as <c>dictionary(int16, utf8)</c> by member name (default enum wire naming),
/// matching <c>vgi_rpc_types.cpp</c>'s <c>on_conflict_values</c>: "ERROR", "IGNORE", "REPLACE".
/// </summary>
public enum OnConflict
{
    Error,
    Ignore,
    Replace,
}
