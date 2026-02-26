package com.framework.broker.queue;

public enum QueuePolicy {
    BLOCK,          // block when full
    REJECT,         // throw exception
    DROP            // silently drop
}