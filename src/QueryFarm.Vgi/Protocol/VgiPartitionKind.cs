namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// Wire values: NOT_PARTITIONED, SINGLE_VALUE_PARTITIONS, OVERLAPPING_PARTITIONS,
/// DISJOINT_PARTITIONS (matches C++'s ParseVgiPartitionKind). <see cref="NotPartitioned"/> is
/// deliberately the first (default) member — <see cref="FunctionInfo.PartitionKind"/> is a
/// non-nullable wire field, so an unset property must already resolve to the C++ side's own
/// documented default.
/// </summary>
public enum VgiPartitionKind
{
    NotPartitioned,
    SingleValuePartitions,
    OverlappingPartitions,
    DisjointPartitions,
}
