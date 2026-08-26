using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// "Blended" (a.k.a. vgi-python's <c>RowTransformFunction</c>) table-in-out functions — see
/// <see cref="ITableInOutFunction.InputFromArgs"/>'s doc comment for the wire-level contract. Backs
/// <c>table_in_out/{blended,lateral_batch,lateral_dedup}.test</c>. Every function here declares
/// ONLY plain typed positional/named args (no <see cref="TableArgFields.Table"/> field) and sets
/// <see cref="ITableInOutFunction.InputFromArgs"/> = <see langword="true"/>; its POSITIONAL
/// (non-named, non-varargs) args become the per-row input batch's columns, read back
/// POSITIONALLY (<c>input.Column(i)</c>) since the wire's actual column names are DuckDB's own
/// synthetic ones, not this function's declared arg names — mirroring
/// <see cref="Scalar.ScalarProcessParams"/>'s convention exactly.
/// </summary>
public static class BlendedFunctions
{
    /// <summary>Formats a coordinate the same way vgi-python's fixture does:
    /// <c>str(round(value, precision))</c> — .NET's default double formatting is already the
    /// shortest round-trippable representation (matching CPython's float repr for the simple
    /// decimal test values these fixtures use), only needing a trailing ".0" appended when the
    /// rounded value happens to be a whole number.</summary>
    internal static string FormatCoordinate(double value, long precision)
    {
        var digits = (int)Math.Clamp(precision, 0, 15);
        var rounded = Math.Round(value, digits, MidpointRounding.ToEven);
        var s = rounded.ToString(CultureInfo.InvariantCulture);
        return s.Contains('.') || s.Contains('E') || s.Contains('e') ? s : s + ".0";
    }
}

/// <summary><c>ex.geo_encode(latitude, longitude [, precision])</c> — blended ("UNNEST-style") geo
/// encoder; one registration serves the literal (<c>geo_encode(52.0, 13.0)</c>), column
/// (<c>FROM t, geo_encode(t.x, t.y)</c>), and correlated LATERAL (<c>LATERAL geo_encode(t.x,
/// t.y)</c>) call shapes. <c>latitude</c>/<c>longitude</c> are the per-row input columns (read
/// POSITIONALLY); <c>precision</c> (default 4) is a named option surfaced via
/// <see cref="TableInOutInitParams.Arguments"/>, never a batch column. Emits one
/// <c>"&lt;lat&gt;:&lt;lon&gt;"</c> geohash string per input row.</summary>
public sealed class GeoEncodeFunction : ITableInOutFunction
{
    public string Name => "geo_encode";

    public string SchemaName => "main";

    public string Description => "Blended per-row geo encoder (lat, lon -> geohash)";

    public IReadOnlyList<string> Categories => ["geo", "blended"];

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("latitude", DoubleType.Default),
            TableArgFields.Positional("longitude", DoubleType.Default),
            TableArgFields.Named("precision", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("geohash", StringType.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var precision = initParams.Arguments.Int64Named("precision", 4);
        return new Processor(precision, initParams.OutputSchema);
    }

    private sealed class Processor(long precision, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var lat = (DoubleArray)input.Column(0);
            var lon = (DoubleArray)input.Column(1);
            var builder = new StringArray.Builder();
            for (var i = 0; i < input.Length; i++)
            {
                if (lat.IsNull(i) || lon.IsNull(i))
                {
                    builder.AppendNull();
                    continue;
                }

                builder.Append(
                    $"{BlendedFunctions.FormatCoordinate(lat.GetValue(i)!.Value, precision)}:{BlendedFunctions.FormatCoordinate(lon.GetValue(i)!.Value, precision)}");
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], input.Length));
        }
    }
}

/// <summary><c>ex.geo_encode(latitude, longitude, altitude [, precision])</c> — arity-overloaded
/// sibling of <see cref="GeoEncodeFunction"/>: SAME <see cref="Name"/> ("geo_encode"), 3 positional
/// input columns. Blended functions use real value types (no TABLE-typed arg), so DuckDB permits
/// multiple same-name overloads disambiguated purely by arity — <c>geo_encode(52,13)</c> resolves
/// to the 2-arg overload, <c>geo_encode(52,13,100)</c> to this one.</summary>
public sealed class GeoEncode3Function : ITableInOutFunction
{
    public string Name => "geo_encode";

    public string SchemaName => "main";

    public string Description => "Blended per-row geo encoder (lat, lon, alt -> geohash)";

    public IReadOnlyList<string> Categories => ["geo", "blended"];

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("latitude", DoubleType.Default),
            TableArgFields.Positional("longitude", DoubleType.Default),
            TableArgFields.Positional("altitude", DoubleType.Default),
            TableArgFields.Named("precision", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("geohash", StringType.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var precision = initParams.Arguments.Int64Named("precision", 4);
        return new Processor(precision, initParams.OutputSchema);
    }

    private sealed class Processor(long precision, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var lat = (DoubleArray)input.Column(0);
            var lon = (DoubleArray)input.Column(1);
            var alt = (DoubleArray)input.Column(2);
            var builder = new StringArray.Builder();
            for (var i = 0; i < input.Length; i++)
            {
                if (lat.IsNull(i) || lon.IsNull(i) || alt.IsNull(i))
                {
                    builder.AppendNull();
                    continue;
                }

                builder.Append(
                    $"{BlendedFunctions.FormatCoordinate(lat.GetValue(i)!.Value, precision)}:" +
                    $"{BlendedFunctions.FormatCoordinate(lon.GetValue(i)!.Value, precision)}:" +
                    $"{BlendedFunctions.FormatCoordinate(alt.GetValue(i)!.Value, precision)}");
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], input.Length));
        }
    }
}

/// <summary><c>ex.row_sum(v1, v2, ... [, absolute])</c> — blended VARARGS row-wise sum, proving the
/// varargs input path: <c>values</c> is a TYPED varargs positional field
/// (<see cref="TableArgFields.TypedVarargs"/>), so the per-row input batch has as many columns as
/// the call site passed (0 for the childless <c>row_sum()</c> call). <c>absolute</c> (default
/// false) is a named option — when true, sums each column's absolute value.</summary>
public sealed class RowSumFunction : ITableInOutFunction
{
    public string Name => "row_sum";

    public string SchemaName => "main";

    public string Description => "Blended per-row varargs sum";

    public IReadOnlyList<string> Categories => ["numeric", "blended"];

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.TypedVarargs("values", DoubleType.Default),
            TableArgFields.Named("absolute", BooleanType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("row_sum", DoubleType.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var absolute = initParams.Arguments.BoolNamed("absolute", false);
        return new Processor(absolute, initParams.OutputSchema);
    }

    private sealed class Processor(bool absolute, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var rows = input.Length;
            var sums = new double[rows];
            var anyNull = new bool[rows];
            for (var c = 0; c < input.ColumnCount; c++)
            {
                var col = (DoubleArray)input.Column(c);
                for (var i = 0; i < rows; i++)
                {
                    if (col.IsNull(i))
                    {
                        anyNull[i] = true;
                        continue;
                    }

                    var v = col.GetValue(i)!.Value;
                    sums[i] += absolute ? Math.Abs(v) : v;
                }
            }

            // A childless call (input.ColumnCount == 0) never marks anyNull, so its single
            // synthesized row (when the transport preserves one — see row_sum()'s own comment
            // about the subprocess-vs-HTTP zero-column row-count quirk) sums to 0.0.
            var builder = new DoubleArray.Builder();
            for (var i = 0; i < rows; i++)
            {
                if (anyNull[i])
                {
                    builder.AppendNull();
                }
                else
                {
                    builder.Append(sums[i]);
                }
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows));
        }
    }
}

/// <summary><c>ex.blended_drop(x)</c> — blended 1-&gt;0 map: emits a single 0-row output batch for
/// its input row, regardless of row count. Exercises the literal scan-mode drain loop's
/// "empty-but-not-EOS" branch (see <c>blended.test</c>'s own comment).</summary>
public sealed class BlendedDropFunction : ITableInOutFunction
{
    public string Name => "blended_drop";

    public string SchemaName => "main";

    public string Description => "Blended 1->0 map emitting a single 0-row batch (literal scan-mode)";

    public IReadOnlyList<string> Categories => ["blended", "test"];

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("x", DoubleType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("v", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(initParams.OutputSchema);

    private sealed class Processor(Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output) =>
            output.Emit(new RecordBatch(outputSchema, [new Int64Array.Builder().Build()], 0));
    }
}

/// <summary><c>ex.blended_explode(n)</c> — blended 1-&gt;N fan-out map carrying per-output-row
/// provenance: for input row value <c>n</c>, emits <c>n</c> output rows <c>i = 0..n-1</c>
/// (<c>n=0</c> emits none — a filter). Because output row count can differ from input row count,
/// attaches <c>vgi_rpc.parent_row#b64</c> (base64 little-endian int32[], one entry per OUTPUT row
/// = its 0-based parent INPUT row index) so the batched correlated-LATERAL operator can stamp each
/// output row's outer/correlated columns from the right input row. Only attached when the counts
/// actually differ — the identity 1-&gt;1 map is assumed when absent.</summary>
public sealed class BlendedExplodeFunction : ITableInOutFunction
{
    public string Name => "blended_explode";

    public string SchemaName => "main";

    public string Description => "Blended 1->N fan-out (emit 0..n-1 per input row) with row provenance";

    public IReadOnlyList<string> Categories => ["blended", "test"];

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("n", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("i", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(initParams.OutputSchema);

    private sealed class Processor(Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var n = (Int64Array)input.Column(0);
            var builder = new Int64Array.Builder();
            var parentRows = new List<int>();
            for (var r = 0; r < n.Length; r++)
            {
                if (n.IsNull(r))
                {
                    continue;
                }

                var fan = n.GetValue(r)!.Value;
                for (var i = 0L; i < fan; i++)
                {
                    builder.Append(i);
                    parentRows.Add(r);
                }
            }

            // Identity requires each OUTPUT row's parent to equal ITS OWN row index — matching
            // total counts is NOT sufficient (e.g. a deduped input batch of 3 rows with fans
            // [0, 1, 2] also totals 3 output rows, but output row 0's parent is input row 1, not
            // 0 — a genuine fan-out, not an identity map; conflating the two silently drops the
            // provenance a correlated LATERAL group-expansion needs).
            var isIdentity = parentRows.Count == n.Length;
            for (var i = 0; isIdentity && i < parentRows.Count; i++)
            {
                isIdentity = parentRows[i] == i;
            }

            IReadOnlyDictionary<string, string>? metadata = null;
            if (!isIdentity)
            {
                var bytes = new byte[parentRows.Count * sizeof(int)];
                for (var i = 0; i < parentRows.Count; i++)
                {
                    BitConverter.TryWriteBytes(bytes.AsSpan(i * sizeof(int), sizeof(int)), parentRows[i]);
                }

                metadata = new Dictionary<string, string> { ["vgi_rpc.parent_row#b64"] = Convert.ToBase64String(bytes) };
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], parentRows.Count), metadata);
        }
    }
}

/// <summary><c>ex.projectable_blended(x)</c> — blended 1-&gt;1 map advertising
/// <see cref="ITableInOutFunction.ProjectionPushdown"/>, with TWO output columns
/// (<c>a = x*10</c>, <c>b = x*100</c>) — regression fixture for the batched correlated-LATERAL
/// operator vs projection pushdown (see <c>lateral_batch.test</c>'s own comment). The batched
/// operator VALIDATES the wire schema strictly against whatever subset it negotiated (unlike a
/// plain table function's producer, which may harmlessly over-fetch), so this narrows to
/// <see cref="TableInOutInitParams.ProjectedSchema"/> — computing both columns internally then
/// emitting only the ones actually requested, in the requested order.</summary>
public sealed class ProjectableBlendedFunction : ITableInOutFunction
{
    public string Name => "projectable_blended";

    public string SchemaName => "main";

    public string Description => "Blended 1->1 map with projection_pushdown + two output columns";

    public IReadOnlyList<string> Categories => ["blended", "test"];

    public bool InputFromArgs => true;

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("x", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("a", Int64Type.Default, nullable: true),
            new Field("b", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
        new Processor(initParams.ProjectedSchema, initParams.ProjectionIds);

    private sealed class Processor(Schema projectedSchema, IReadOnlyList<long>? projectionIds) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var x = (Int64Array)input.Column(0);
            var aBuilder = new Int64Array.Builder();
            var bBuilder = new Int64Array.Builder();
            for (var i = 0; i < x.Length; i++)
            {
                if (x.IsNull(i))
                {
                    aBuilder.AppendNull();
                    bBuilder.AppendNull();
                    continue;
                }

                var v = x.GetValue(i)!.Value;
                aBuilder.Append(v * 10);
                bBuilder.Append(v * 100);
            }

            IArrowArray a = aBuilder.Build();
            IArrowArray b = bBuilder.Build();
            var full = new IArrowArray[] { a, b };
            var indices = projectionIds ?? [0, 1];
            var columns = indices.Select(idx => full[idx]).ToList();

            // 1->1 identity map: no provenance needed (the operator assumes identity).
            output.Emit(new RecordBatch(projectedSchema, columns, x.Length));
        }
    }
}

/// <summary><c>ex.hostile_provenance(x [, mode])</c> — adversarial blended fixture emitting a
/// MALFORMED <c>vgi_rpc.parent_row#b64</c> payload, simulating a buggy/hostile worker the batched
/// correlated-LATERAL operator must reject rather than use as an unchecked array index. Emits one
/// output row per input row (row counts match — only the metadata is poisoned) with <c>hv</c> =
/// <c>x</c>. <c>mode</c> selects the poison: <c>range</c> (default) — every parent index ==
/// row count (one past the last valid index); <c>length</c> — a well-formed but one-element-too-long
/// payload; <c>base64</c> — not valid base64 at all.</summary>
public sealed class HostileProvenanceFunction : ITableInOutFunction
{
    public string Name => "hostile_provenance";

    public string SchemaName => "main";

    public string Description => "Adversarial blended fixture emitting malformed vgi_rpc.parent_row";

    public IReadOnlyList<string> Categories => ["blended", "test", "adversarial"];

    public bool InputFromArgs => true;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("x", Int64Type.Default),
            TableArgFields.Named("mode", StringType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("hv", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams)
    {
        var mode = initParams.Arguments.StringNamed("mode", "range");
        return new Processor(mode, initParams.OutputSchema);
    }

    private sealed class Processor(string mode, Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var x = (Int64Array)input.Column(0);
            var n = x.Length;
            var hvBuilder = new Int64Array.Builder();
            for (var i = 0; i < n; i++)
            {
                if (x.IsNull(i))
                {
                    hvBuilder.AppendNull();
                }
                else
                {
                    hvBuilder.Append(x.GetValue(i)!.Value);
                }
            }

            string payload;
            switch (mode)
            {
                case "base64":
                    payload = "@@@ this is not base64 @@@";
                    break;
                case "length":
                    {
                        // One int32 too many for the emitted row count.
                        var bytes = new byte[(n + 1) * sizeof(int)];
                        payload = Convert.ToBase64String(bytes);
                        break;
                    }

                default:
                    {
                        // "range" — every parent index == n (one past the last valid index n-1).
                        var bytes = new byte[n * sizeof(int)];
                        for (var i = 0; i < n; i++)
                        {
                            BitConverter.TryWriteBytes(bytes.AsSpan(i * sizeof(int), sizeof(int)), n);
                        }

                        payload = Convert.ToBase64String(bytes);
                        break;
                    }
            }

            var metadata = new Dictionary<string, string> { ["vgi_rpc.parent_row#b64"] = payload };
            output.Emit(new RecordBatch(outputSchema, [hvBuilder.Build()], n), metadata);
        }
    }
}
