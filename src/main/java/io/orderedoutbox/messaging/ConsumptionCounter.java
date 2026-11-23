package io.orderedoutbox.messaging;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.atomic.AtomicLong;

/** Test and demo hook: how many records the projection consumer has finished handling. */
@ApplicationScoped
public class ConsumptionCounter {

    private final AtomicLong count = new AtomicLong();

    public void increment() {
        count.incrementAndGet();
    }

    public long get() {
        return count.get();
    }
}
