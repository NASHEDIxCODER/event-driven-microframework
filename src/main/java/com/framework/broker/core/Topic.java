package com.framework.broker.core;

import com.framework.broker.queue.BoundedEventQueue;
import com.framework.broker.queue.QueuePolicy;

public class Topic {

    private final String name;
    private final BoundedEventQueue queue;

    public Topic(String name, int capacity) {
        this.name = name;
        this.queue = new BoundedEventQueue(capacity, QueuePolicy.BLOCK);
    }

    public String getName() {
        return name;
    }

    public void publish(Event event) throws InterruptedException {
        queue.publish(event);
    }

    public Event consume() throws InterruptedException {
        return queue.consume();
    }

    public int size() {
        return queue.size();
    }
}