package com.framework.resilience;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class CircuitBreaker {

    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private final int failureThreshold;
    private final long openTimeoutMillis;

    private final AtomicInteger failureCount = new AtomicInteger(0);
    private volatile State state = State.CLOSED;
    private volatile long lastFailureTime = 0;

    public CircuitBreaker(int failureThreshold, long openTimeoutMillis) {
        this.failureThreshold = failureThreshold;
        this.openTimeoutMillis = openTimeoutMillis;
    }

    public synchronized boolean allowRequest() {

        if (state == State.OPEN) {
            long now = Instant.now().toEpochMilli();

            if (now - lastFailureTime > openTimeoutMillis) {
                state = State.HALF_OPEN;
                return true;
            }
            return false;
        }

        return true;
    }

    public synchronized void recordSuccess() {
        failureCount.set(0);
        state = State.CLOSED;
    }

    public synchronized void recordFailure() {
        int failures = failureCount.incrementAndGet();

        if (failures >= failureThreshold) {
            state = State.OPEN;
            lastFailureTime = Instant.now().toEpochMilli();
        }
    }

    public State getState() {
        return state;
    }
}