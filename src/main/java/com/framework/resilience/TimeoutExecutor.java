package com.framework.resilience;

import java.util.concurrent.*;

public class TimeoutExecutor {

    private final ExecutorService executor =
            Executors.newCachedThreadPool();

    public <T> T execute(Callable<T> task, long timeoutMillis)
            throws Exception {

        Future<T> future = executor.submit(task);

        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("Execution timed out");
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}