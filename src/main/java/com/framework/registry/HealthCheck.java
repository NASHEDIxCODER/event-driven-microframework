package com.framework.registry;

public interface HealthCheck {
    boolean isHealthy(ServiceInstance instance);
}