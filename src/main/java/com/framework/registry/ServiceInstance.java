package com.framework.registry;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceInstance {

    private final String serviceName;
    private final String instanceId;
    private final String host;
    private final int port;
    private final Map<String, String> metadata = new ConcurrentHashMap<>();

    private volatile long lastHeartbeat;

    public ServiceInstance(String serviceName, String host, int port) {
        this.serviceName = serviceName;
        this.host = host;
        this.port = port;
        this.instanceId = UUID.randomUUID().toString();
        this.lastHeartbeat = Instant.now().toEpochMilli();
    }

    public String getServiceName() { return serviceName; }
    public String getInstanceId() { return instanceId; }
    public String getHost() { return host; }
    public int getPort() { return port; }

    public long getLastHeartbeat() { return lastHeartbeat; }

    public void heartbeat() {
        this.lastHeartbeat = Instant.now().toEpochMilli();
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}