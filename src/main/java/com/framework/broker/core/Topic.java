package com.framework.broker.core;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Topic {

    private final String name;
    private final BlockingQueue<Event> queue;
    private final int capacity;

    public Topic(String name, int capacity) {
        this.name = Objects.requireNonNull(name, "Topic name must not be null");
        this.capacity = capacity;
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    public String getName() {
        return name;
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * Publish event to topic.
     * Blocks if queue is full (backpressure).
     */
    public void publish(Event event) throws InterruptedException {
        queue.put(event); // blocks if full
    }

    /**
     * Publish with timeout to avoid infinite blocking.
     */
    public boolean publish(Event event, long timeout, TimeUnit unit) throws InterruptedException {
        return queue.offer(event, timeout, unit);
    }

    /**
     * Consume event from topic.
     * Blocks if queue is empty.
     */
    public Event consume() throws InterruptedException {
        return queue.take();
    }

    /**
     * Non-blocking poll.
     */
    public Event poll() {
        return queue.poll();
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public String toString() {
        return "Topic{" +
                "name='" + name + '\'' +
                ", capacity=" + capacity +
                ", currentSize=" + queue.size() +
                '}';
    }
}