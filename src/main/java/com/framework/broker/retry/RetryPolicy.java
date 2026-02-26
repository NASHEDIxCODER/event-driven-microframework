package com.framework.broker.retry;

public class RetryPolicy {

    private final int maxAttempts;
    private final long initialDelay;
    private final double multiplier;

    public RetryPolicy(int maxAttempts, long initialDelay, double multiplier) {
        this.maxAttempts = maxAttempts;
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long computeDelay(int attempt) {
        return (long) (initialDelay * Math.pow(multiplier, attempt - 1));
    }
}