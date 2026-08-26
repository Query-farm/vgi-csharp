using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Picks the one candidate, among several functions registered under the same
/// <c>(identity, schemaName, name)</c> key (see <see cref="CatalogRegistry"/>'s overload support),
/// whose declared <c>ArgumentsSchema</c> matches the concrete per-call argument types DuckDB's own
/// binder already resolved for this call site.
///
/// DuckDB's binder (see <c>vgi_scalar_function_impl.cpp</c>/<c>vgi_table_function_set.cpp</c> on
/// the C++ side) already picked exactly one overload before ever sending a <c>bind</c>/<c>init</c>
/// RPC — the wire gives the worker no overload id, only <c>function_name</c> (identical across every
/// overload sharing that name) plus whatever argument-shape information that request kind carries.
/// This type redundantly, but necessarily, re-derives which registered candidate DuckDB meant.
///
/// Two DISTINCT argument-shape sources exist, because <c>BindRequest.InputSchema</c> only reflects
/// the per-row PARAM columns of the exchange's streamed input batch — a bind-time CONST argument
/// (<see cref="Attributes.ConstParamAttribute"/>) never appears there at all (it travels separately,
/// in <c>BindRequest.Arguments</c>'s const-value struct — see <see cref="ScalarArgCodec"/>) — and a
/// plain TABLE function has no streamed exchange input concept whatsoever (every one of its
/// arguments is a bind-time constant, decoded via <see cref="TableArgCodec"/> instead):
/// <list type="bullet">
/// <item><see cref="SelectScalar{T}"/> — walks a candidate's <c>ArgumentsSchema</c> field by field,
/// consulting the const-value struct's types for a <c>vgi_const=true</c> field and
/// <c>InputSchema</c>'s types (in order) for every other field.</item>
/// <item><see cref="SelectTable{T}"/> — every argument is bind-time-constant, so a candidate's
/// declared field types are compared directly against the ACTUAL decoded argument values' Arrow
/// types (<see cref="TableArguments"/>).</item>
/// </list>
///
/// A field carrying <c>vgi_type=any</c> metadata (<see cref="VgiWireMetadata.TypeAnyValue"/>) always
/// matches any incoming field type (used by e.g. <c>any_mixed</c>'s first parameter). A trailing
/// field carrying <c>vgi_varargs=true</c> metadata matches every remaining input field from its own
/// position onward, each individually checked against that same varargs field's own declared type
/// (or ANY).
/// </summary>
public static class OverloadResolver
{
    /// <summary>Resolves one candidate SCALAR function out of several sharing a name — see this
    /// type's doc comment for why const-parameter and per-row-parameter types come from two
    /// different wire sources for a scalar call.</summary>
    public static T SelectScalar<T>(IReadOnlyList<T> candidates, Func<T, Schema> argumentsSchema, byte[] constArguments, Schema? paramSchema, string name)
    {
        if (candidates.Count == 1)
        {
            return candidates[0];
        }

        var constTypes = ScalarArgCodec.DecodeConstStruct(constArguments)
            .OrderBy(kv => kv.Key)
            .Select(kv => kv.Value.Data.DataType)
            .ToList();
        var paramTypes = paramSchema?.FieldsList.Select(f => f.DataType).ToList() ?? [];

        var matches = candidates.Where(c => MatchesSplit(argumentsSchema(c), constTypes, paramTypes)).ToList();
        return Resolve(matches, name);
    }

    /// <summary>Resolves one candidate TABLE function out of several sharing a name — every
    /// argument is a bind-time constant, so candidate types are compared directly against the
    /// ACTUAL decoded argument values' Arrow types.</summary>
    public static T SelectTable<T>(IReadOnlyList<T> candidates, Func<T, Schema> argumentsSchema, TableArguments arguments, string name)
    {
        if (candidates.Count == 1)
        {
            return candidates[0];
        }

        var argTypes = Enumerable.Range(0, arguments.PositionalCount)
            .Select(i => arguments.PositionalArray(i)?.Data.DataType)
            .ToList();

        var matches = candidates.Where(c => MatchesPositional(argumentsSchema(c), argTypes)).ToList();
        return Resolve(matches, name);
    }

    /// <summary>Resolves one candidate table-in-out function out of several sharing a name — the
    /// ONLY shape this currently needs to handle is a <c>InputFromArgs</c> ("blended") function's
    /// arity overload (e.g. <c>geo_encode(lat, lon)</c> vs <c>geo_encode(lat, lon, alt)</c>): its
    /// declared POSITIONAL (non-named, non-table) fields ARE its per-row input columns, and the
    /// C++ side reports the call site's ACTUALLY-RESOLVED input column types via
    /// <paramref name="inputSchema"/> (<c>BindRequest.InputSchema</c>/<c>InitRequest</c>'s embedded
    /// bind call) — see <c>vgi_table_in_out_impl.cpp</c>'s blended bind: it builds
    /// <c>bind_data-&gt;input_schema</c> from whichever registered overload's declared positional
    /// arg types DuckDB's own binder already matched, so its FIELD COUNT is exactly this call's
    /// resolved arity. A classic (non-blended, TABLE-arg) candidate is never expected to have
    /// siblings sharing its name today, but is handled the same way for generality: its
    /// <see cref="Table.TableArgFields.Table"/> field (if any) is excluded from the comparison
    /// (that field's own type never round-trips — see its doc comment — and
    /// <paramref name="inputSchema"/> reflects its TABLE columns, not a positional arg list, for a
    /// non-blended call).</summary>
    public static T SelectTableInOut<T>(IReadOnlyList<T> candidates, Func<T, Schema> argumentsSchema, Schema? inputSchema, string name)
    {
        if (candidates.Count == 1)
        {
            return candidates[0];
        }

        var inputTypes = inputSchema?.FieldsList.Select(f => (IArrowType?)f.DataType).ToList() ?? [];

        var matches = candidates.Where(c => MatchesPositional(NonNamedNonTableFields(argumentsSchema(c)), inputTypes)).ToList();
        return Resolve(matches, name);
    }

    private static Schema NonNamedNonTableFields(Schema schema) =>
        new(schema.FieldsList.Where(f => !IsNamed(f) && !IsTable(f)), metadata: null);

    private static T Resolve<T>(List<T> matches, string name) => matches.Count switch
    {
        0 => throw new InvalidOperationException($"'{name}': no registered overload matches the call-site argument types."),
        1 => matches[0],
        _ => throw new InvalidOperationException($"'{name}': {matches.Count} registered overloads ambiguously match the call-site argument types."),
    };

    /// <summary>Matches a scalar candidate field-by-field, pulling each field's expected type from
    /// EITHER <paramref name="constTypes"/> or <paramref name="paramTypes"/> depending on whether
    /// that field itself carries <c>vgi_const=true</c> metadata — and requires BOTH lists fully
    /// consumed (exact arity on each dimension) for a non-varargs candidate.</summary>
    private static bool MatchesSplit(Schema candidate, IReadOnlyList<IArrowType> constTypes, IReadOnlyList<IArrowType> paramTypes)
    {
        var fields = candidate.FieldsList;
        int constI = 0, paramI = 0;

        foreach (var field in fields)
        {
            if (IsVarargs(field))
            {
                // A varargs field (never const in any fixture here) consumes every remaining
                // param, each individually checked against its own declared type (or ANY).
                for (; paramI < paramTypes.Count; paramI++)
                {
                    if (!FieldTypeMatches(field, paramTypes[paramI]))
                    {
                        return false;
                    }
                }

                return constI == constTypes.Count;
            }

            if (IsConst(field))
            {
                if (constI >= constTypes.Count || !FieldTypeMatches(field, constTypes[constI]))
                {
                    return false;
                }

                constI++;
            }
            else
            {
                if (paramI >= paramTypes.Count || !FieldTypeMatches(field, paramTypes[paramI]))
                {
                    return false;
                }

                paramI++;
            }
        }

        return constI == constTypes.Count && paramI == paramTypes.Count;
    }

    /// <summary>Matches a table-function candidate field-by-field against the actual decoded
    /// argument types (all positional — table functions have no per-row concept), handling a
    /// trailing varargs field the same way <see cref="MatchesSplit"/> does.</summary>
    private static bool MatchesPositional(Schema candidate, IReadOnlyList<IArrowType?> argTypes)
    {
        var fields = candidate.FieldsList;
        var varargsIndex = fields.Select((f, i) => (f, i)).Where(t => IsVarargs(t.f)).Select(t => t.i).Cast<int?>().FirstOrDefault();

        if (varargsIndex is null)
        {
            if (fields.Count != argTypes.Count)
            {
                return false;
            }

            for (var i = 0; i < fields.Count; i++)
            {
                if (argTypes[i] is not { } t || !FieldTypeMatches(fields[i], t))
                {
                    return false;
                }
            }

            return true;
        }

        var prefixCount = varargsIndex.Value;
        if (argTypes.Count < prefixCount)
        {
            return false;
        }

        for (var i = 0; i < prefixCount; i++)
        {
            if (argTypes[i] is not { } t || !FieldTypeMatches(fields[i], t))
            {
                return false;
            }
        }

        var varargsField = fields[prefixCount];
        for (var i = prefixCount; i < argTypes.Count; i++)
        {
            if (argTypes[i] is not { } t || !FieldTypeMatches(varargsField, t))
            {
                return false;
            }
        }

        return true;
    }

    private static bool FieldTypeMatches(Field candidate, IArrowType actual) => IsAny(candidate) || candidate.DataType.TypeId == actual.TypeId;

    private static bool IsAny(Field field) =>
        field.Metadata is not null &&
        field.Metadata.TryGetValue(VgiWireMetadata.TypeKey, out var v) &&
        v == VgiWireMetadata.TypeAnyValue;

    private static bool IsVarargs(Field field) =>
        field.Metadata is not null &&
        field.Metadata.TryGetValue(VgiWireMetadata.VarargsKey, out var v) &&
        v == VgiWireMetadata.VarargsTrueValue;

    private static bool IsConst(Field field) =>
        field.Metadata is not null &&
        field.Metadata.TryGetValue(VgiWireMetadata.ConstKey, out var v) &&
        v == VgiWireMetadata.ConstTrueValue;

    private static bool IsNamed(Field field) =>
        field.Metadata is not null &&
        field.Metadata.TryGetValue(VgiWireMetadata.ArgKey, out var v) &&
        v == VgiWireMetadata.ArgNamedValue;

    private static bool IsTable(Field field) =>
        field.Metadata is not null &&
        field.Metadata.TryGetValue(VgiWireMetadata.TypeKey, out var v) &&
        v == VgiWireMetadata.TypeTableValue;
}
