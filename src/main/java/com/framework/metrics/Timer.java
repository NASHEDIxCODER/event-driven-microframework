package com.framework.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class Timer {

    private final AtomicLong totalTime = new AtomicLong(0);
    private final AtomicLong count = new AtomicLong(0);

    public void record(long durationMillis) {
        totalTime.addAndGet(durationMillis);
        count.incrementAndGet();
    }

    public long getCount() {
        return count.get();
    }

    public double getAverage() {
        long c = count.get();
        return c == 0 ? 0 : (double) totalTime.get() / c;
    }
}