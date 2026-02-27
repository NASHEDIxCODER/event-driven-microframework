package com.framework.gateway;

import com.framework.registry.ServiceInstance;
import com.framework.registry.ServiceRegistry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ApiGateway {

    private final ServiceRegistry registry;
    private final LoadBalancer loadBalancer;
    private final RateLimiter rateLimiter;

    private final Map<String, RouteDefinition> routes =
            new ConcurrentHashMap<>();

    public ApiGateway(ServiceRegistry registry,
                      LoadBalancer loadBalancer,
                      RateLimiter rateLimiter) {

        this.registry = registry;
        this.loadBalancer = loadBalancer;
        this.rateLimiter = rateLimiter;
    }

    public void addRoute(RouteDefinition route) {
        routes.put(route.getPath(), route);
    }

    public Optional<ServiceInstance> route(String path,
                                           String clientId) {

        // 1️⃣ Rate limit
        if (!rateLimiter.allow(clientId)) {
            throw new RuntimeException("Rate limit exceeded");
        }

        // 2️⃣ Find route
        RouteDefinition route = routes.get(path);
        if (route == null) {
            throw new RuntimeException("Route not found");
        }

        // 3️⃣ Lookup service instances
        List<ServiceInstance> instances =
                registry.getInstances(route.getServiceName());

        if (instances.isEmpty()) {
            throw new RuntimeException("No service instances available");
        }

        // 4️⃣ Load balancing
        ServiceInstance chosen =
                loadBalancer.choose(instances);

        return Optional.ofNullable(chosen);
    }
}