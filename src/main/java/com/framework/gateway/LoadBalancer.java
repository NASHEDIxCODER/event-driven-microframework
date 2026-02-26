package com.framework.gateway;

import com.framework.registry.ServiceInstance;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadBalancer {

    private final AtomicInteger counter = new AtomicInteger(0);

    public ServiceInstance choose(List<ServiceInstance> instances) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }

        int index = Math.abs(counter.getAndIncrement() % instances.size());
        return instances.get(index);
    }
}