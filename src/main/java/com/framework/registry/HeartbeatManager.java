package com.framework.registry;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;

public class HeartbeatManager {

    private final ServiceRegistry registry;
    private final long ttlMillis;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    public HeartbeatManager(ServiceRegistry registry,
                            long ttlMillis,
                            long cleanupIntervalMillis) {

        this.registry = registry;
        this.ttlMillis = ttlMillis;

        scheduler.scheduleAtFixedRate(
                this::cleanup,
                cleanupIntervalMillis,
                cleanupIntervalMillis,
                TimeUnit.MILLISECONDS
        );
    }

    private void cleanup() {
        long now = Instant.now().toEpochMilli();

        for (List<ServiceInstance> instances : registry.getAll().values()) {
            instances.removeIf(instance ->
                    now - instance.getLastHeartbeat() > ttlMillis);
        }
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}