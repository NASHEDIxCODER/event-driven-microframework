package com.framework.broker.producer;

import com.framework.broker.core.Event;
import com.framework.broker.core.MessageBroker;

public class EventProducer {

    private final MessageBroker broker;

    public EventProducer(MessageBroker broker) {
        this.broker = broker;
    }

    public void send(Event event) throws InterruptedException {
        broker.publish(event);
    }
}