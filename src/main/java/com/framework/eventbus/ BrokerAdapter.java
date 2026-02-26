package com.framework.eventbus;

import com.framework.broker.core.Event;
import com.framework.broker.core.MessageBroker;
import com.framework.broker.consumer.EventConsumer;
import com.framework.broker.dispatcher.EventDispatcher;
import com.framework.broker.core.Topic;

public class BrokerAdapter {

    private final MessageBroker broker;
    private final EventDispatcher dispatcher;

    public BrokerAdapter(MessageBroker broker, EventDispatcher dispatcher) {
        this.broker = broker;
        this.dispatcher = dispatcher;
    }

    public void createTopic(String topicName, int capacity) {
        Topic topic = broker.createTopic(topicName);
        dispatcher.start(topic);
    }

    public void publish(Event event) throws InterruptedException {
        broker.publish(event);
    }

    public void subscribe(String topic, EventConsumer consumer) {
        dispatcher.registerConsumer(topic, consumer);
    }
}