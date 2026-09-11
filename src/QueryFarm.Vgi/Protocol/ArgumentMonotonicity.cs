namespace QueryFarm.Vgi.Protocol;

/// <summary>Monotonicity of a scalar function in one argument with all others fixed.</summary>
public enum ArgumentMonotonicity
{
    Unknown,
    Constant,
    NonDecreasing,
    StrictlyIncreasing,
    NonIncreasing,
    StrictlyDecreasing,
}
