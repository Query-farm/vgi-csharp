namespace QueryFarm.Vgi.Protocol;

/// <summary>Wire values: NOT_ORDER_DEPENDENT, ORDER_DEPENDENT. <see cref="NotOrderDependent"/> is
/// the default (first, value 0) member — matches this non-nullable field's C++-side default.</summary>
public enum AggregateOrderDependent
{
    NotOrderDependent,
    OrderDependent,
}
