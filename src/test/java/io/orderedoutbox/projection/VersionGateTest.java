package io.orderedoutbox.projection;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VersionGateTest {

    private final VersionGate gate = new VersionGate();

    @Test
    void acceptsTheNextVersion() {
        assertInstanceOf(ApplyDecision.Accept.class, gate.decide(0, 1));
        assertInstanceOf(ApplyDecision.Accept.class, gate.decide(2, 3));
    }

    @Test
    void ignoresDuplicatesAndOlderVersions() {
        ApplyDecision duplicate = gate.decide(3, 3);
        ApplyDecision stale = gate.decide(3, 1);
        assertEquals("duplicate-or-stale", assertInstanceOf(ApplyDecision.Ignore.class, duplicate).reason());
        assertEquals("duplicate-or-stale", assertInstanceOf(ApplyDecision.Ignore.class, stale).reason());
    }

    @Test
    void parksAGapAndNamesTheVersionItStillNeeds() {
        ApplyDecision parked = gate.decide(1, 3);
        assertEquals(2L, assertInstanceOf(ApplyDecision.Park.class, parked).expectedVersion());
        ApplyDecision firstMissing = gate.decide(0, 2);
        assertEquals(1L, assertInstanceOf(ApplyDecision.Park.class, firstMissing).expectedVersion());
    }

    @Test
    void drainsOnlyTheContiguousParkedPrefix() {
        SortedSet<Long> parked = new TreeSet<>(List.of(3L, 4L, 6L));
        assertEquals(List.of(), gate.contiguousFrom(0, parked));
        assertEquals(List.of(3L, 4L), gate.contiguousFrom(2, parked));
        assertEquals(List.of(6L), gate.contiguousFrom(5, parked));
    }

    @Test
    void rejectsNonPositiveVersions() {
        assertThrows(IllegalArgumentException.class, () -> gate.decide(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> gate.decide(0, 0));
    }
}
