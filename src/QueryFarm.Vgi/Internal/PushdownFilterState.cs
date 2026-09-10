namespace QueryFarm.Vgi.Internal;

/// <summary>Revisioned scan-local state for applying atomic v2 dynamic-filter deltas.</summary>
public sealed class PushdownFilterState
{
    private const int MaxPredicateIds = 4_096;

    private Dictionary<string, LivePredicate> _predicates;
    private Dictionary<string, ulong> _revisions;
    private readonly HashSet<string> _requiredIds;

    public PushdownFilterState(DecodedFilters snapshot)
    {
        if (snapshot.Kind != "snapshot")
        {
            throw new ArgumentException("Initial filter state must be a snapshot.", nameof(snapshot));
        }

        _predicates = snapshot.Predicates.ToDictionary(p => p.Id, p => new LivePredicate(snapshot, p), StringComparer.Ordinal);
        _revisions = snapshot.Predicates.ToDictionary(p => p.Id, p => p.Revision, StringComparer.Ordinal);
        _requiredIds = snapshot.Predicates.Where(p => p.Mode == "required").Select(p => p.Id).ToHashSet(StringComparer.Ordinal);
    }

    public void ApplyDelta(DecodedFilters delta)
    {
        if (delta.Kind != "delta")
        {
            throw new ArgumentException("Dynamic filter update must be a delta.", nameof(delta));
        }

        var predicates = new Dictionary<string, LivePredicate>(_predicates, StringComparer.Ordinal);
        var revisions = new Dictionary<string, ulong>(_revisions, StringComparer.Ordinal);
        foreach (var update in delta.Updates)
        {
            if (_requiredIds.Contains(update.Id))
            {
                throw new InvalidDataException($"Delta cannot target required predicate '{update.Id}'.");
            }

            if (revisions.TryGetValue(update.Id, out var prior) && update.Revision <= prior)
            {
                continue;
            }

            revisions[update.Id] = update.Revision;
            if (update.Operation == "remove")
            {
                predicates.Remove(update.Id);
            }
            else
            {
                predicates[update.Id] = new LivePredicate(delta, update.Predicate!);
            }
        }

        if (revisions.Count > MaxPredicateIds)
        {
            throw new InvalidDataException("Dynamic filter state exceeds the 4096 predicate-ID limit.");
        }

        _predicates = predicates;
        _revisions = revisions;
    }

    public bool Matches(IReadOnlyDictionary<string, object?> row) =>
        _predicates.Values.All(item => PushdownFilterEvaluator.MatchesPredicate(item.Payload, item.Predicate, row));

    private sealed record LivePredicate(DecodedFilters Payload, DecodedPredicate Predicate);
}
