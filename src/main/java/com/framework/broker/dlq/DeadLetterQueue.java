package com.framework.broker.dlq;

import com.framework.broker.core.Event;
import com.framework.broker.core.Topic;

public class DeadLetterQueue {

    private final Topic dlqTopic;

    public DeadLetterQueue(String originalTopicName, int capacity) {
        this.dlqTopic = new Topic(originalTopicName + ".DLQ", capacity);
    }

    public void publish(Event event) throws InterruptedException {
        dlqTopic.publish(event);
    }

    public Topic getDlqTopic() {
        return dlqTopic;
    }
}