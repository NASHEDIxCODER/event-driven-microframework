package com.framework.broker.queue;

import com.framework.broker.core.Event;
import com.framework.broker.exception.BackpressureException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BoundedEventQueue {

    private final BlockingQueue<Event> queue;
    private final QueuePolicy policy;

    public BoundedEventQueue(int capacity, QueuePolicy policy) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.policy = policy;
    }

    public void publish(Event event) throws InterruptedException {
        switch (policy) {
            case BLOCK:
                queue.put(event);
                break;

            case REJECT:
                if (!queue.offer(event)) {
                    throw new BackpressureException("Queue is full");
                }
                break;

            case DROP:
                queue.offer(event);
                break;
        }
    }

    public Event consume() throws InterruptedException {
        return queue.take();
    }

    public int size() {
        return queue.size();
    }
}