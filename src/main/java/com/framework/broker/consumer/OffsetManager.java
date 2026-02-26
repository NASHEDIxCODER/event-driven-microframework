package com.framework.broker.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class OffsetManager {

    private final Map<String, AtomicLong> offsets = new ConcurrentHashMap<>();

    public void initializeConsumer(String consumerId) {
        offsets.putIfAbsent(consumerId, new AtomicLong(0));
    }

    public long getOffset(String consumerId) {
        AtomicLong offset = offsets.get(consumerId);
        if (offset == null) {
            throw new IllegalArgumentException("Consumer not registered: " + consumerId);
        }
        return offset.get();
    }

    public void incrementOffset(String consumerId) {
        AtomicLong offset = offsets.get(consumerId);
        if (offset == null) {
            throw new IllegalArgumentException("Consumer not registered: " + consumerId);
        }
        offset.incrementAndGet();
    }
}