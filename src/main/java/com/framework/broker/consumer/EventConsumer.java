package com.framework.broker.consumer;

import com.framework.broker.core.Event;

public interface EventConsumer {

    String getConsumerId();

    void onEvent(Event event) throws Exception;
}