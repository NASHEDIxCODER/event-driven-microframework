package com.framework.broker.dispatcher;

public class DispatcherConfig {

    private final int workerThreads;
    private final long shutdownTimeoutMillis;

    public DispatcherConfig(int workerThreads, long shutdownTimeoutMillis) {
        this.workerThreads = workerThreads;
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public long getShutdownTimeoutMillis() {
        return shutdownTimeoutMillis;
    }
}