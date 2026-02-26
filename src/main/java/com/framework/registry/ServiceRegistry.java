package com.framework.registry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ServiceRegistry {

    private final Map<String, List<ServiceInstance>> registry =
            new ConcurrentHashMap<>();

    public void register(ServiceInstance instance) {
        registry
                .computeIfAbsent(instance.getServiceName(),
                        k -> new CopyOnWriteArrayList<>())
                .add(instance);
    }

    public void deregister(String serviceName, String instanceId) {
        List<ServiceInstance> instances = registry.get(serviceName);
        if (instances != null) {
            instances.removeIf(i -> i.getInstanceId().equals(instanceId));
        }
    }

    public List<ServiceInstance> getInstances(String serviceName) {
        return registry.getOrDefault(serviceName, Collections.emptyList());
    }

    public Optional<ServiceInstance> getRandomInstance(String serviceName) {
        List<ServiceInstance> instances = getInstances(serviceName);
        if (instances.isEmpty()) return Optional.empty();

        int index = new Random().nextInt(instances.size());
        return Optional.of(instances.get(index));
    }

    public Map<String, List<ServiceInstance>> getAll() {
        return registry;
    }
}