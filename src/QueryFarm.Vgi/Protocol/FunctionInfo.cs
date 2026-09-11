namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One item of a <c>catalog_schema_contents_functions</c> <see cref="ItemsResponse"/> (also
/// reusable for <see cref="CatalogAttachResult.GlobalFunctions"/>). The C++ extension validates
/// each item's embedded-IPC schema with STRICT <c>arrow::Schema::Equals</c> against its generated
/// <c>FunctionInfoSchema()</c> — property declaration order is LOAD-BEARING and must match that
/// 41-field schema exactly, field-for-field, even though individual value reads on the C++ side
/// are by name.
/// </summary>
public sealed class FunctionInfo
{
    public string? Comment { get; set; }

    public Dictionary<string, string> Tags { get; set; } = [];

    public string Name { get; set; } = "";

    public List<string> SchemaPath { get; set; } = ["main"];

    public FunctionType FunctionType { get; set; }

    /// <summary>Serialized (schema-only) Arrow IPC bytes describing the function's arguments.
    /// Fixed argument fields retain their declared parameter names; <c>vgi_arg=named</c> marks
    /// table-function-style named-only options.</summary>
    public byte[] Arguments { get; set; } = [];

    /// <summary>Serialized (schema-only) Arrow IPC bytes describing the return value: exactly one
    /// field.</summary>
    public byte[] OutputSchema { get; set; } = [];

    /// <summary>Serialized authoritative typed defaults: a one-row record batch containing only
    /// defaulted parameters in signature order. A present null cell is an explicit NULL default.
    /// <c>vgi_default</c> argument metadata remains discovery-only.</summary>
    public byte[]? ParameterDefaultValues { get; set; }

    public FunctionStability? Stability { get; set; }

    public FunctionNullHandling? NullHandling { get; set; }

    /// <summary>Scalar-only uppercase claims aligned with argument declaration slots.
    /// <see langword="null"/> makes no claims; a vararg declaration occupies one slot.</summary>
    public List<string>? ArgumentMonotonicity { get; set; }

    public string Description { get; set; } = "";

    public List<FunctionExample> Examples { get; set; } = [];

    public List<string> Categories { get; set; } = [];

    public bool? ProjectionPushdown { get; set; }

    public bool? FilterPushdown { get; set; }

    public bool? SamplingPushdown { get; set; }

    public bool? LateMaterialization { get; set; }

    public List<string> FilterSemanticProfiles { get; set; } = [];

    public List<FilterFunctionCapability> AdditionalFilterFunctions { get; set; } = [];

    public List<RuntimeFilterAlgorithmCapability> RuntimeFilterAlgorithms { get; set; } = [];

    public List<EvaluationContextCapability> FilterEvaluationContexts { get; set; } = [];

    public VgiOrderPreservation? OrderPreservation { get; set; }

    public int? MaxWorkers { get; set; }

    public bool SupportsBatchIndex { get; set; }

    public bool SupportsSplits { get; set; }

    public bool FiltersExactlyApplied { get; set; }

    public bool SupportsPositions { get; set; }

    public long? SplitTokenTtlSeconds { get; set; }

    public VgiPartitionKind PartitionKind { get; set; } = VgiPartitionKind.NotPartitioned;

    public AggregateOrderDependent OrderDependent { get; set; } = AggregateOrderDependent.NotOrderDependent;

    public AggregateDistinctDependent DistinctDependent { get; set; } = AggregateDistinctDependent.NotDistinctDependent;

    public bool SupportsWindow { get; set; }

    public bool StreamingPartitioned { get; set; }

    public bool HasFinalize { get; set; }

    public bool SourceOrderDependent { get; set; }

    public bool SinkOrderDependent { get; set; }

    public bool RequiresInputBatchIndex { get; set; }

    public bool InputFromArgs { get; set; }

    public List<string> RequiredSettings { get; set; } = [];

    public List<RequiredSecret> RequiredSecrets { get; set; } = [];
}

public sealed class FilterFunctionCapability
{
    public string Namespace { get; set; } = "";

    public string Name { get; set; } = "";

    public ulong Version { get; set; }
}

public sealed class RuntimeFilterAlgorithmCapability
{
    public string Namespace { get; set; } = "";

    public string Name { get; set; } = "";

    public ulong Version { get; set; }
}

public sealed class EvaluationContextCapability
{
    public string Profile { get; set; } = "";

    public string? ProviderFingerprint { get; set; }
}

/// <summary>Nested struct inside <see cref="FunctionInfo.Examples"/>. Property order matches the
/// C++ side's struct field order: sql, description, expected_output.</summary>
public sealed class FunctionExample
{
    public string Sql { get; set; } = "";

    public string Description { get; set; } = "";

    public string? ExpectedOutput { get; set; }
}

/// <summary>Nested struct inside <see cref="FunctionInfo.RequiredSecrets"/>. Property order
/// matches the C++ side's struct field order: secret_type, scope, secret_name.</summary>
public sealed class RequiredSecret
{
    public string SecretType { get; set; } = "";

    public string? Scope { get; set; }

    public string? SecretName { get; set; }
}
