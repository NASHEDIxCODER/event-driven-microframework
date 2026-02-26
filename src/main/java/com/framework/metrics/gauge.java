package com.framework.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class Counter {

    private final AtomicLong value = new AtomicLong(0);

    public void increment() {
        value.incrementAndGet();
    }

    public void increment(long amount) {
        value.addAndGet(amount);
    }

    public long get() {
        return value.get();
    }
}