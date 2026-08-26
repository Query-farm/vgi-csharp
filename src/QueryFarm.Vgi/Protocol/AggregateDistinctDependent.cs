namespace QueryFarm.Vgi.Protocol;

/// <summary>Wire values: NOT_DISTINCT_DEPENDENT, DISTINCT_DEPENDENT. <see cref="NotDistinctDependent"/>
/// is the default (first, value 0) member — matches this non-nullable field's C++-side default.</summary>
public enum AggregateDistinctDependent
{
    NotDistinctDependent,
    DistinctDependent,
}
