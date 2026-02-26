package com.framework.gateway;

public class RouteDefinition {

    private final String path;
    private final String serviceName;

    public RouteDefinition(String path, String serviceName) {
        this.path = path;
        this.serviceName = serviceName;
    }

    public String getPath() {
        return path;
    }

    public String getServiceName() {
        return serviceName;
    }
}