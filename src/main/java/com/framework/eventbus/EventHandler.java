package com.framework.eventbus;

public interface EventHandler {
    void handle(Object payload) throws Exception;
}