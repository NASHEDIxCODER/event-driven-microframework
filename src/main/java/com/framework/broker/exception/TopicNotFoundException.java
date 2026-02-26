package com.framework.broker.exception;

public class TopicNotFoundException extends BrokerException {
    public TopicNotFoundException(String topic) {
        super("Topic not found: " + topic);
    }
}