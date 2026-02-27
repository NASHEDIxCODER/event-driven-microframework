package com.framework;

import com.framework.bootstrap.FrameworkBootstrap;
import com.framework.eventbus.EventBus;
import com.framework.tracing.EventTracer;

public class Main {

    public static void main(String[] args) throws Exception {

        FrameworkBootstrap framework =
                new FrameworkBootstrap();

        EventBus eventBus = framework.getEventBus();

        // 1️⃣ Create Topic
        eventBus.createTopic("user-events", 100);

        // 2️⃣ Register Consumer
        eventBus.subscribe("user-events", "consumer-1", payload -> {

            String message = (String) payload;

            System.out.println("Processing: " + message);

            if ("fail-event".equals(message)) {
                throw new RuntimeException("Simulated failure");
            }
        });

        // 3️⃣ Publish Events
        eventBus.publish("user-events", "Hello World");
        eventBus.publish("user-events", "fail-event");
        eventBus.publish("user-events", "Another Event");

        // Let async system process
        Thread.sleep(5000);

        // 4️⃣ Shutdown cleanly
        framework.shutdown();
    }
}