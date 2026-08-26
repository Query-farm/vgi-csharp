using System.Collections.Concurrent;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Scalar;

/// <summary>
/// Reflects a <see cref="ScalarFn"/> subclass's <c>Compute</c> method exactly once (cached per
/// CLR type) and dispatches every subsequent <see cref="ScalarFn.Process"/> call against that
/// reflected plan — ported from vgi-java's <c>ScalarFn.ComputePlan</c>, adapted for C#'s
/// immutable-array-plus-builder model (a <c>Compute</c> method here looks like Java's: <c>void</c>-
/// returning, writing into a builder parameter — not Python's return-an-array style).
///
/// Supported per-parameter markers: <see cref="ParamAttribute"/> (a per-row input column, fixed
/// concrete Arrow array type), <see cref="ConstParamAttribute"/> (a bind-time constant, resolved
/// from <see cref="ScalarProcessParams.Arguments"/> by a separate const-only positional index),
/// <see cref="SettingAttribute"/> (a resolved DuckDB setting value), and
/// <see cref="OutputLengthAttribute"/> (the batch row count, as <see cref="int"/>). The one
/// parameter with NO marker attribute must be last — an Arrow array Builder instance
/// (e.g. <c>StringArray.Builder</c>) that becomes the sole output column.
///
/// Deliberately NOT supported here (see each fixture's own doc comment for why): <c>[Param(Any =
/// true)]</c>/dynamic ("ANY"-typed) output — those fixtures (<c>add_values</c>, <c>double</c>,
/// <c>sum_values</c>) implement <see cref="IScalarFunction"/> directly instead, since their output
/// type is derived per-call via <see cref="Types.TypeRules"/> rather than fixed at reflection time;
/// and <c>[Param(Varargs = true)]</c> for a non-primitive element type (struct/list/fixed-size-list
/// varargs — the <c>geo_centroid_*</c> fixtures) similarly implement <see cref="IScalarFunction"/>
/// directly, since their nested Arrow types can't be inferred from a CLR array-class lookup table.
/// </summary>
internal sealed class ComputePlan
{
    private static readonly ConcurrentDictionary<Type, ComputePlan> Cache = new();

    private static readonly IReadOnlyDictionary<Type, IArrowType> ArrayClrToArrow = new Dictionary<Type, IArrowType>
    {
        [typeof(Int8Array)] = Int8Type.Default,
        [typeof(Int16Array)] = Int16Type.Default,
        [typeof(Int32Array)] = Int32Type.Default,
        [typeof(Int64Array)] = Int64Type.Default,
        [typeof(UInt8Array)] = UInt8Type.Default,
        [typeof(UInt16Array)] = UInt16Type.Default,
        [typeof(UInt32Array)] = UInt32Type.Default,
        [typeof(UInt64Array)] = UInt64Type.Default,
        [typeof(FloatArray)] = FloatType.Default,
        [typeof(DoubleArray)] = DoubleType.Default,
        [typeof(StringArray)] = StringType.Default,
        [typeof(BooleanArray)] = BooleanType.Default,
        [typeof(BinaryArray)] = BinaryType.Default,
    };

    private static readonly IReadOnlyDictionary<Type, IArrowType> ConstClrToArrow = new Dictionary<Type, IArrowType>
    {
        [typeof(sbyte)] = Int8Type.Default,
        [typeof(short)] = Int16Type.Default,
        [typeof(int)] = Int32Type.Default,
        [typeof(long)] = Int64Type.Default,
        [typeof(byte)] = UInt8Type.Default,
        [typeof(ushort)] = UInt16Type.Default,
        [typeof(uint)] = UInt32Type.Default,
        [typeof(ulong)] = UInt64Type.Default,
        [typeof(float)] = FloatType.Default,
        [typeof(double)] = DoubleType.Default,
        [typeof(string)] = StringType.Default,
        [typeof(bool)] = BooleanType.Default,
        [typeof(byte[])] = BinaryType.Default,
    };

    private static readonly IReadOnlyDictionary<int, IArrowArray> ImmutableEmptyConstValues = new Dictionary<int, IArrowArray>();
    private static readonly IReadOnlyDictionary<string, IArrowArray> ImmutableEmptySettingValues = new Dictionary<string, IArrowArray>();
    private static readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> ImmutableEmptySecretValues = new Dictionary<string, IReadOnlyDictionary<string, IArrowArray>>();

    private enum Kind { Param, ConstParam, Setting, Secret, OutputLength, Output }

    private sealed record Slot(Kind Kind, Type ClrType, string? Name, int ConstIndex);

    private readonly MethodInfo _method;
    private readonly IReadOnlyList<Slot> _slots;
    private readonly Type _outputBuilderType;
    private readonly MethodInfo _outputBuildMethod;
    private readonly bool _hasConstParams;
    private readonly bool _hasSettings;
    private readonly bool _hasSecrets;

    public Schema ArgumentsSchema { get; }

    public Schema OutputSchema { get; }

    public IReadOnlyList<string> RequiredSettingNames { get; }

    public IReadOnlyList<Protocol.RequiredSecret> RequiredSecretsList { get; }

    public static ComputePlan ForType(Type type) => Cache.GetOrAdd(type, Build);

    private ComputePlan(
        MethodInfo method,
        IReadOnlyList<Slot> slots,
        Type outputBuilderType,
        MethodInfo outputBuildMethod,
        Schema argumentsSchema,
        Schema outputSchema,
        IReadOnlyList<string> requiredSettingNames,
        IReadOnlyList<Protocol.RequiredSecret> requiredSecretsList)
    {
        _method = method;
        _slots = slots;
        _outputBuilderType = outputBuilderType;
        _outputBuildMethod = outputBuildMethod;
        _hasConstParams = slots.Any(s => s.Kind == Kind.ConstParam);
        _hasSettings = slots.Any(s => s.Kind == Kind.Setting);
        _hasSecrets = slots.Any(s => s.Kind == Kind.Secret);
        ArgumentsSchema = argumentsSchema;
        OutputSchema = outputSchema;
        RequiredSettingNames = requiredSettingNames;
        RequiredSecretsList = requiredSecretsList;
    }

    private static ComputePlan Build(Type type)
    {
        var method = type.GetMethod(
            "Compute",
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            ?? throw new InvalidOperationException($"'{type}' derives from ScalarFn but declares no 'Compute' method.");

        if (method.ReturnType != typeof(void))
        {
            throw new InvalidOperationException($"'{type}.Compute' must return void (write into its output builder parameter).");
        }

        var parameters = method.GetParameters();
        if (parameters.Length == 0)
        {
            throw new InvalidOperationException($"'{type}.Compute' must declare at least an output builder parameter.");
        }

        var slots = new List<Slot>(parameters.Length);
        var argFields = new List<Field>();
        var settingNames = new List<string>();
        var requiredSecrets = new List<Protocol.RequiredSecret>();
        var constIndex = 0;
        Type? outputBuilderType = null;

        for (var i = 0; i < parameters.Length; i++)
        {
            var p = parameters[i];
            var param = p.GetCustomAttribute<ParamAttribute>();
            var constParam = p.GetCustomAttribute<ConstParamAttribute>();
            var setting = p.GetCustomAttribute<SettingAttribute>();
            var secret = p.GetCustomAttribute<SecretAttribute>();
            var outputLength = p.GetCustomAttribute<OutputLengthAttribute>();

            var markerCount = (param is not null ? 1 : 0) + (constParam is not null ? 1 : 0) +
                               (setting is not null ? 1 : 0) + (secret is not null ? 1 : 0) +
                               (outputLength is not null ? 1 : 0);
            if (markerCount > 1)
            {
                throw new InvalidOperationException(
                    $"'{type}.Compute' parameter '{p.Name}' carries more than one of [Param]/[ConstParam]/[Setting]/[Secret]/[OutputLength].");
            }

            if (param is not null)
            {
                if (param.Any || param.Varargs)
                {
                    throw new NotSupportedException(
                        $"'{type}.Compute' parameter '{p.Name}': ScalarFn's reflection dispatch does not support " +
                        "[Param(Any = true)] or [Param(Varargs = true)] — implement IScalarFunction directly for " +
                        "ANY-typed/dynamic-output or non-primitive-element varargs functions.");
                }

                if (!ArrayClrToArrow.TryGetValue(p.ParameterType, out var arrowType))
                {
                    throw new InvalidOperationException(
                        $"'{type}.Compute' parameter '{p.Name}': unsupported [Param] array type '{p.ParameterType}'.");
                }

                var wireName = param.Name.Length > 0 ? param.Name : ToSnakeCase(p.Name ?? $"arg{i}");
                var paramMetadata = param.Doc.Length > 0
                    ? new Dictionary<string, string> { [VgiWireMetadata.DocKey] = param.Doc }
                    : null;
                argFields.Add(new Field(wireName, arrowType, nullable: true, paramMetadata));
                slots.Add(new Slot(Kind.Param, p.ParameterType, null, -1));
                continue;
            }

            if (constParam is not null)
            {
                if (!ConstClrToArrow.TryGetValue(p.ParameterType, out var arrowType))
                {
                    throw new InvalidOperationException(
                        $"'{type}.Compute' parameter '{p.Name}': unsupported [ConstParam] CLR type '{p.ParameterType}'.");
                }

                var wireName = constParam.Name.Length > 0 ? constParam.Name : ToSnakeCase(p.Name ?? $"arg{i}");
                var metadata = new Dictionary<string, string> { [VgiWireMetadata.ConstKey] = VgiWireMetadata.ConstTrueValue };
                if (constParam.Doc.Length > 0)
                {
                    metadata[VgiWireMetadata.DocKey] = constParam.Doc;
                }

                if (VgiWireMetadata.BuildRange(constParam.Ge, constParam.Gt, constParam.Le, constParam.Lt) is { } range)
                {
                    metadata[VgiWireMetadata.RangeKey] = range;
                }

                argFields.Add(new Field(wireName, arrowType, nullable: true, metadata));
                slots.Add(new Slot(Kind.ConstParam, p.ParameterType, null, constIndex));
                constIndex++;
                continue;
            }

            if (setting is not null)
            {
                var key = setting.Key.Length > 0 ? setting.Key : ToSnakeCase(p.Name ?? $"setting{i}");
                settingNames.Add(key);
                slots.Add(new Slot(Kind.Setting, p.ParameterType, key, -1));
                continue;
            }

            if (secret is not null)
            {
                if (p.ParameterType != typeof(IReadOnlyDictionary<string, IArrowArray>))
                {
                    throw new InvalidOperationException(
                        $"'{type}.Compute' parameter '{p.Name}': [Secret] must be declared as 'IReadOnlyDictionary<string, IArrowArray>?'.");
                }

                requiredSecrets.Add(new Protocol.RequiredSecret { SecretType = secret.SecretType, Scope = secret.Scope, SecretName = secret.Name });
                slots.Add(new Slot(Kind.Secret, p.ParameterType, secret.SecretType, -1));
                continue;
            }

            if (outputLength is not null)
            {
                if (p.ParameterType != typeof(int))
                {
                    throw new InvalidOperationException($"'{type}.Compute' parameter '{p.Name}': [OutputLength] must be declared as 'int'.");
                }

                slots.Add(new Slot(Kind.OutputLength, p.ParameterType, null, -1));
                continue;
            }

            // Unmarked — must be the output builder, and must be the last parameter.
            if (i != parameters.Length - 1)
            {
                throw new InvalidOperationException(
                    $"'{type}.Compute' parameter '{p.Name}' carries no [Param]/[ConstParam]/[Setting]/[Secret]/[OutputLength] " +
                    "marker but isn't the last parameter — the unmarked output-builder parameter must be last.");
            }

            outputBuilderType = p.ParameterType;
            slots.Add(new Slot(Kind.Output, p.ParameterType, null, -1));
        }

        if (outputBuilderType is null)
        {
            throw new InvalidOperationException(
                $"'{type}.Compute' declares no unmarked (output builder) parameter — every parameter carries a " +
                "[Param]/[ConstParam]/[Setting]/[Secret]/[OutputLength] marker.");
        }

        if (outputBuilderType.DeclaringType is null || !ArrayClrToArrow.TryGetValue(outputBuilderType.DeclaringType, out var outputArrowType))
        {
            throw new InvalidOperationException($"'{type}.Compute': unsupported output builder type '{outputBuilderType}'.");
        }

        var buildMethod = outputBuilderType.GetMethod("Build", BindingFlags.Instance | BindingFlags.Public, Type.EmptyTypes)
            ?? outputBuilderType.GetMethods(BindingFlags.Instance | BindingFlags.Public)
                .FirstOrDefault(m => m.Name == "Build" && m.GetParameters().All(mp => mp.IsOptional))
            ?? throw new InvalidOperationException($"'{outputBuilderType}' has no accessible parameterless-callable 'Build' method.");

        var argumentsSchema = new Schema(argFields, metadata: null);
        var outputSchema = new Schema([new Field("result", outputArrowType, nullable: true)], metadata: null);

        return new ComputePlan(method, slots, outputBuilderType, buildMethod, argumentsSchema, outputSchema, settingNames, requiredSecrets);
    }

    public RecordBatch Invoke(IScalarFunction instance, ScalarProcessParams processParams)
    {
        var input = processParams.Input;
        var rows = input.Length;

        // Decoding is skipped entirely when this plan has no [ConstParam]/[Setting]/[Secret] slots
        // at all — not just an optimization: reading a zero-field `struct<>` "args" IPC batch (what
        // the C++ side sends when a function has no const parameters at all) has been observed to
        // intermittently corrupt/crash the vendored Arrow IPC reader. Since an empty decode
        // dictionary is exactly what a function with no such slots needs anyway, the safest fix is
        // to never touch that reader for a plan that will never consult the result.
        var constValues = _hasConstParams
            ? ScalarArgCodec.DecodeConstStruct(processParams.Arguments)
            : ImmutableEmptyConstValues;
        var settingValues = _hasSettings
            ? ScalarArgCodec.DecodeSettings(processParams.Settings)
            : ImmutableEmptySettingValues;
        var secretValues = _hasSecrets
            ? SecretArgCodec.Decode(processParams.Secrets)
            : ImmutableEmptySecretValues;

        var args = new object?[_slots.Count];
        object? outputBuilder = null;
        var vectorIndex = 0;

        for (var i = 0; i < _slots.Count; i++)
        {
            var slot = _slots[i];
            args[i] = slot.Kind switch
            {
                Kind.Param => input.Column(vectorIndex++),
                Kind.ConstParam => ScalarArgCodec.ConvertTo(
                    ScalarArgCodec.ReadScalar(constValues.GetValueOrDefault(slot.ConstIndex)), slot.ClrType),
                Kind.Setting => ScalarArgCodec.ConvertTo(
                    ScalarArgCodec.ReadScalar(settingValues.GetValueOrDefault(slot.Name!)), slot.ClrType),
                Kind.Secret => SecretArgCodec.FindByType(secretValues, slot.Name!),
                Kind.OutputLength => rows,
                Kind.Output => outputBuilder = Activator.CreateInstance(_outputBuilderType),
                _ => throw new InvalidOperationException("Unreachable."),
            };
        }

        _method.Invoke(instance, args);

        var builtArray = (IArrowArray)_outputBuildMethod.Invoke(outputBuilder, BuildInvokeArgs(_outputBuildMethod))!;
        return new RecordBatch(processParams.OutputSchema, [builtArray], rows);
    }

    private static object?[] BuildInvokeArgs(MethodInfo buildMethod)
    {
        var parameters = buildMethod.GetParameters();
        if (parameters.Length == 0)
        {
            return [];
        }

        // Build(MemoryAllocator? allocator = null) — pass the default for whatever that parameter is.
        return [parameters[0].HasDefaultValue ? parameters[0].DefaultValue : null];
    }

    private static string ToSnakeCase(string pascalOrCamel)
    {
        if (pascalOrCamel.Length == 0)
        {
            return pascalOrCamel;
        }

        Span<char> buffer = stackalloc char[pascalOrCamel.Length * 2];
        var pos = 0;
        for (var i = 0; i < pascalOrCamel.Length; i++)
        {
            var c = pascalOrCamel[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                {
                    buffer[pos++] = '_';
                }

                buffer[pos++] = char.ToLowerInvariant(c);
            }
            else
            {
                buffer[pos++] = c;
            }
        }

        return new string(buffer[..pos]);
    }
}
