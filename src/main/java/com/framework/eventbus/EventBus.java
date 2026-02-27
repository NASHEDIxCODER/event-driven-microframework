package com.framework.eventbus;

import com.framework.broker.core.Event;

public class EventBus {

    private final BrokerAdapter adapter;
    private final EventSerializer serializer;

    public EventBus(BrokerAdapter adapter,
                    EventSerializer serializer) {
        this.adapter = adapter;
        this.serializer = serializer;
    }

    public void createTopic(String topic, int capacity) {
        adapter.createTopic(topic, capacity);
    }

    public void publish(String topic, Object payload)
            throws InterruptedException {

        byte[] serialized = serializer.serialize(payload);

        Event event = Event.builder()
                .topic(topic)
                .payload(serialized)
                .build();

        adapter.publish(event);
    }

    public void subscribe(String topic,
                          String consumerId,
                          EventHandler handler) {

        adapter.subscribe(topic, new com.framework.broker.consumer.EventConsumer() {

            @Override
            public String getConsumerId() {
                return consumerId;
            }

            @Override
            public void onEvent(com.framework.broker.core.Event event) throws Exception {
                Object deserialized =
                        serializer.deserialize(event.getPayload());
                handler.handle(deserialized);
            }
        });
    }
}