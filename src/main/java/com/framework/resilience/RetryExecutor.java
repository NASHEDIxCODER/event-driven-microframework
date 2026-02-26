package com.framework.resilience;

import java.util.concurrent.Callable;

public class RetryExecutor {

    private final int maxAttempts;
    private final long delayMillis;

    public RetryExecutor(int maxAttempts, long delayMillis) {
        this.maxAttempts = maxAttempts;
        this.delayMillis = delayMillis;
    }

    public <T> T execute(Callable<T> callable) throws Exception {

        int attempt = 0;
        Exception lastException = null;

        while (attempt < maxAttempts) {
            try {
                return callable.call();
            } catch (Exception e) {
                lastException = e;
                attempt++;
                Thread.sleep(delayMillis);
            }
        }

        throw lastException;
    }
}