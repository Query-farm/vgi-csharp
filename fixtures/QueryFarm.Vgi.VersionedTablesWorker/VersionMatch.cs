namespace QueryFarm.Vgi.VersionedTablesWorker;

/// <summary>
/// The small npm-style version-spec matcher <c>attach/versioning.test</c> needs — NOT a general
/// semver library, scoped to exactly the forms that file exercises against a small hardcoded
/// candidate list: an exact 3-part version, <c>^X.Y.Z</c> (highest candidate with the same major,
/// &gt;= the target), <c>~X.Y.Z</c> (highest candidate with the same major.minor, &gt;= the
/// target), a bare major (<c>"1"</c>, highest candidate in that major), or a bare major.minor
/// (<c>"1.0"</c>, highest candidate in that major.minor). A missing/null spec resolves to the
/// newest candidate (the default-version behavior the test's "omitted" case expects).
///
/// Duplicated verbatim from <c>QueryFarm.Vgi.VersionedWorker</c>'s own copy — each dedicated
/// worker is deliberately small and self-contained; see <c>versioning.test</c>/
/// <c>versioned_tables*.test</c> and this project's <c>Program.cs</c> for why they're two
/// separate processes rather than one shared catalog set (an unfiltered <c>vgi_catalogs()</c>
/// discovery query expecting exactly one row).
/// </summary>
internal static class VersionMatch
{
    /// <param name="spec">The caller-supplied spec, or <see langword="null"/>/empty for "omitted —
    /// use the default".</param>
    /// <param name="candidatesNewestFirst">Every version this catalog supports, sorted descending —
    /// the first entry is the default when <paramref name="spec"/> is omitted, and "highest
    /// matching" scans this list in order.</param>
    /// <returns>The resolved candidate string, or <see langword="null"/> if nothing satisfies
    /// <paramref name="spec"/> (the caller turns that into the "Unsupported ..." ATTACH error).</returns>
    public static string? Resolve(string? spec, IReadOnlyList<string> candidatesNewestFirst)
    {
        if (string.IsNullOrEmpty(spec))
        {
            return candidatesNewestFirst.Count > 0 ? candidatesNewestFirst[0] : null;
        }

        if (spec[0] is '^' or '~')
        {
            if (!TryParse(spec[1..], out var target))
            {
                return null;
            }

            var sameMinorToo = spec[0] == '~';
            foreach (var candidate in candidatesNewestFirst)
            {
                if (!TryParse(candidate, out var v))
                {
                    continue;
                }

                var sameScope = v.Major == target.Major && (!sameMinorToo || v.Minor == target.Minor);
                if (sameScope && CompareTo(v, target) >= 0)
                {
                    return candidate;
                }
            }

            return null;
        }

        var parts = spec.Split('.');
        return parts.Length switch
        {
            // Exact X.Y.Z — a literal match, never "closest".
            3 => candidatesNewestFirst.Contains(spec) ? spec : null,
            // Bare major "X" — highest candidate whose major matches.
            1 => FirstWhere(candidatesNewestFirst, v => v.Major.ToString() == parts[0]),
            // Bare major.minor "X.Y" — highest candidate whose major.minor matches.
            2 => FirstWhere(candidatesNewestFirst, v => $"{v.Major}.{v.Minor}" == spec),
            _ => null,
        };
    }

    private static string? FirstWhere(IReadOnlyList<string> candidatesNewestFirst, Func<(int Major, int Minor, int Patch), bool> predicate)
    {
        foreach (var candidate in candidatesNewestFirst)
        {
            if (TryParse(candidate, out var v) && predicate(v))
            {
                return candidate;
            }
        }

        return null;
    }

    private static bool TryParse(string s, out (int Major, int Minor, int Patch) version)
    {
        version = default;
        var parts = s.Split('.');
        if (parts.Length != 3
            || !int.TryParse(parts[0], out var major)
            || !int.TryParse(parts[1], out var minor)
            || !int.TryParse(parts[2], out var patch))
        {
            return false;
        }

        version = (major, minor, patch);
        return true;
    }

    private static int CompareTo((int Major, int Minor, int Patch) a, (int Major, int Minor, int Patch) b) =>
        (a.Major, a.Minor, a.Patch).CompareTo((b.Major, b.Minor, b.Patch));
}
