package io.orderedoutbox.outbox;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VersionDelayTest {

    @Test
    void lowerVersionsWaitLongerThanHigherOnes() {
        assertTrue(VersionDelay.steps(1, 3) > VersionDelay.steps(2, 3));
        assertTrue(VersionDelay.steps(2, 3) > VersionDelay.steps(3, 3));
        assertEquals(3L, VersionDelay.steps(1, 3));
        assertEquals(1L, VersionDelay.steps(3, 3));
        assertEquals(0L, VersionDelay.steps(4, 3));
    }

    @Test
    void rejectsANonPositivePipeline() {
        assertThrows(IllegalArgumentException.class, () -> VersionDelay.steps(1, 0));
        assertThrows(IllegalArgumentException.class, () -> VersionDelay.steps(0, 3));
    }
}
