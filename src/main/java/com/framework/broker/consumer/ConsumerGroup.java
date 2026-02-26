package com.framework.broker.consumer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConsumerGroup {

    private final String topic;
    private final List<EventConsumer> consumers = new CopyOnWriteArrayList<>();

    public ConsumerGroup(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void addConsumer(EventConsumer consumer) {
        consumers.add(consumer);
    }

    public List<EventConsumer> getConsumers() {
        return consumers;
    }
}