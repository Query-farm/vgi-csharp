namespace QueryFarm.Vgi.Protocol;

/// <summary>Wire values: PRESERVES_ORDER, NO_ORDER_GUARANTEE, FIXED_ORDER (matches C++'s ParseVgiOrderPreservation).</summary>
public enum VgiOrderPreservation
{
    PreservesOrder,
    NoOrderGuarantee,
    FixedOrder,
}
