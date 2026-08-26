using Apache.Arrow;
using QueryFarm.Vgi.Protocol;

namespace QueryFarm.Vgi.TableInOut;

/// <summary>
/// The raw contract a streaming table-in-out function implements — the VGI analog of
/// <see cref="Table.ITableFunction"/> for a function that takes a TABLE argument and transforms it
/// batch-by-batch (<see cref="ExchangeState"/>-shaped: client sends an input batch, this replies with
/// exactly one output turn per input turn), optionally followed by a per-substream FINALIZE phase
/// (<see cref="ProducerState"/>-shaped) once the input stream ends. Ported from vgi-python's
/// <c>TableInOutGenerator</c>/vgi-java's raw <c>TableInOutExchangeState</c> pattern.
///
/// IMPORTANT — this is the per-SUBSTREAM shape: DuckDB fans a streaming table-in-out call out to one
/// worker PROCESS per substream by default (see <c>vgi_table_in_out_impl.cpp</c>'s "Phase A"
/// comments), so <see cref="ITableInOutProcessor"/> instances never see input from more than one
/// substream, and a FINALIZE phase (if any) runs on that SAME substream's own accumulated state — it
/// is NOT a global cross-substream aggregation. A function that needs a single correct answer over
/// the WHOLE input regardless of how DuckDB partitions it (e.g. a plain "sum every row") belongs to
/// <see cref="Buffering.ITableBufferingFunction"/> instead, which is purpose-built for that (see its
/// own doc comment for why a per-substream finalize is unsound for that case).
/// </summary>
public interface ITableInOutFunction
{
    string Name { get; }

    string SchemaName => "main";

    string Description => "";

    /// <summary>Optional free-text comment surfaced via <c>duckdb_functions().comment</c>
    /// (<see cref="Protocol.FunctionInfo.Comment"/>). <see langword="null"/> (the default) reports
    /// no comment.</summary>
    string? Comment => null;

    /// <summary>Optional key/value tags surfaced via <c>duckdb_functions().tags</c>
    /// (<see cref="Protocol.FunctionInfo.Tags"/>). Empty (the default) reports no tags.</summary>
    IReadOnlyDictionary<string, string> Tags => new Dictionary<string, string>();

    IReadOnlyList<string> Categories => [];

    /// <summary>Describes the function's NON-table positional/named arguments PLUS exactly one
    /// <see cref="Table.TableArgFields.Table"/>-marked field for the TABLE input, in whatever
    /// position matches the desired SQL call shape (e.g. <c>f(data TABLE, threshold INT)</c> or
    /// <c>f(threshold INT, data TABLE)</c>).</summary>
    Schema ArgumentsSchema { get; }

    /// <summary>The function's STATIC/declared output schema — for a dynamic-output function (e.g.
    /// one whose output mirrors the TABLE argument's runtime columns) this is a best-effort/empty
    /// placeholder; the REAL per-call schema comes from <see cref="ResolveOutputSchema"/>.</summary>
    Schema OutputSchema { get; }

    FunctionStability? Stability => null;

    IReadOnlyList<string> RequiredSettings => [];

    /// <summary>Secrets this function needs statically resolved and shipped on every bind — see
    /// <see cref="Table.ITableFunction.RequiredSecrets"/>'s doc comment (same static-vs-dynamic split;
    /// a dynamic scope goes through <see cref="TableInOutBindParams.Secrets"/>'s
    /// <see cref="Internal.SecretsAccessor.Get"/> from <see cref="Bind"/> instead).</summary>
    IReadOnlyList<RequiredSecret> RequiredSecrets => [];

    bool? ProjectionPushdown => null;

    int? MaxWorkers => null;

    /// <summary>Whether this function has a real FINALIZE phase — must agree with whether
    /// <see cref="ITableInOutProcessor.Finalize"/> is overridden to do real work, since the C++
    /// extension decides whether to ever call <c>init(phase=FINALIZE)</c> at all based on this flag
    /// (advertised via <see cref="Protocol.FunctionInfo.HasFinalize"/>). A LATERAL join with
    /// correlated input additionally REJECTS a function that advertises a finalize callback — so a
    /// no-finalize function stays LATERAL-compatible.</summary>
    bool HasFinalize => false;

    /// <summary>"Blended" (a.k.a. vgi-python's <c>RowTransformFunction</c>) opt-in: this function's
    /// <see cref="ArgumentsSchema"/> declares ONLY plain typed positional/named args (no
    /// <see cref="Table.TableArgFields.Table"/>-marked field) — its positional args ARE its per-row
    /// input columns, so it serves both a streaming column-form call (<c>FROM t, f(t.x)</c>) AND a
    /// correlated <c>LATERAL f(t.x)</c> call from one registration. Defaults to
    /// <see langword="false"/> — DuckDB's binder otherwise REJECTS a correlated LATERAL column
    /// argument ("does not support lateral join column parameters") for a table-in-out function
    /// that doesn't advertise this (<see cref="Protocol.FunctionInfo.InputFromArgs"/>). A blended
    /// function must also leave <see cref="HasFinalize"/> false (LATERAL further rejects a
    /// finalize-capable function).</summary>
    bool InputFromArgs => false;

    /// <summary>Called once per bound call (mirrors the <c>bind</c> RPC) — a no-op by default;
    /// override to validate arguments or compute a dynamic output type. Exceptions propagate as a
    /// bind failure back to the client.</summary>
    void Bind(TableInOutBindParams bindParams)
    {
    }

    /// <summary>Resolves the schema to use for THIS specific call. Defaults to the static
    /// <see cref="OutputSchema"/>; override for a dynamic-output function.</summary>
    Schema ResolveOutputSchema(TableInOutBindParams bindParams) => OutputSchema;

    /// <summary>Creates the per-substream processor that handles this substream's INPUT (and, if
    /// <see cref="HasFinalize"/>, FINALIZE) phase — mirrors the <c>init(phase=INPUT)</c> RPC opening
    /// an exchange stream. Exceptions propagate as an init failure.</summary>
    ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams);
}
