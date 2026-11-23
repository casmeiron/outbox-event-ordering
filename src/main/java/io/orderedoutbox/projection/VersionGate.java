package io.orderedoutbox.projection;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

@ApplicationScoped

/**
 * Pure version check. The database projection is the applied version; this class
 * does not touch Kafka or Postgres so the rules can be tested on their own.
 *
 * <p>Version {@code 0} means nothing has been applied. The first event of an
 * aggregate must be version {@code 1}. A gap is parked rather than applied, and
 * a duplicate or older version is ignored.
 */
public final class VersionGate {

    public ApplyDecision decide(long appliedVersion, long incomingVersion) {
        if (appliedVersion < 0) {
            throw new IllegalArgumentException("applied version must be >= 0");
        }
        if (incomingVersion <= 0) {
            throw new IllegalArgumentException("incoming version must be positive");
        }
        long expected = appliedVersion + 1;
        if (incomingVersion == expected) {
            return new ApplyDecision.Accept();
        }
        if (incomingVersion < expected) {
            return new ApplyDecision.Ignore("duplicate-or-stale");
        }
        return new ApplyDecision.Park(expected);
    }

    /**
     * Parked versions that form a contiguous run starting at {@code appliedVersion + 1}.
     * A hole stops the drain: version 6 stays parked when 5 is missing.
     */
    public List<Long> contiguousFrom(long appliedVersion, SortedSet<Long> parkedVersions) {
        List<Long> ready = new ArrayList<>();
        long next = appliedVersion + 1;
        while (parkedVersions.contains(next)) {
            ready.add(next);
            next++;
        }
        return ready;
    }
}
