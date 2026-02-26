package com.framework.broker.dispatcher;

import com.framework.broker.consumer.*;
import com.framework.broker.core.*;
import com.framework.broker.dlq.DeadLetterQueue;
import com.framework.broker.retry.RetryPolicy;

import java.util.Map;
import java.util.concurrent.*;

public class EventDispatcher {

    private final ExecutorService workerPool;
    private final ScheduledExecutorService retryScheduler =
            Executors.newScheduledThreadPool(2);

    private final Map<String, ConsumerGroup> consumerGroups =
            new ConcurrentHashMap<>();

    private final Map<String, DeadLetterQueue> dlqRegistry =
            new ConcurrentHashMap<>();

    private final OffsetManager offsetManager = new OffsetManager();
    private final RetryPolicy retryPolicy =
            new RetryPolicy(3, 1000, 2.0);

    private volatile boolean running = false;

    public EventDispatcher(DispatcherConfig config) {
        this.workerPool =
                Executors.newFixedThreadPool(config.getWorkerThreads());
    }

    public void registerConsumer(String topic, EventConsumer consumer) {
        consumerGroups
                .computeIfAbsent(topic, ConsumerGroup::new)
                .addConsumer(consumer);

        offsetManager.initializeConsumer(consumer.getConsumerId());
    }

    public void start(Topic topic) {
        running = true;
        Thread t = new Thread(() -> dispatchLoop(topic));
        t.start();
    }

    private void dispatchLoop(Topic topic) {
        while (running) {
            try {
                Event event = topic.consume();
                ConsumerGroup group =
                        consumerGroups.get(topic.getName());

                if (group == null) continue;

                for (EventConsumer consumer : group.getConsumers()) {
                    workerPool.submit(
                            new DispatchWorker(
                                    topic.getName(),
                                    consumer,
                                    event,
                                    offsetManager,
                                    retryPolicy,
                                    retryScheduler,
                                    workerPool,
                                    dlqRegistry
                            )
                    );
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void processWithRetry(String topicName,
                                  EventConsumer consumer,
                                  Event event) {

        int attempt = Integer.parseInt(
                event.getHeaders().getOrDefault("retryCount", "0")
        ) + 1;

        try {
            consumer.onEvent(event);
            offsetManager.incrementOffset(consumer.getConsumerId());

        } catch (Exception ex) {

            if (attempt <= retryPolicy.getMaxAttempts()) {

                long delay = retryPolicy.computeDelay(attempt);

                Event retryEvent = Event.builder()
                        .id(event.getId())
                        .topic(event.getTopic())
                        .payload(event.getPayload())
                        .headers(event.getHeaders())
                        .header("retryCount", String.valueOf(attempt))
                        .build();

                retryScheduler.schedule(() ->
                                workerPool.submit(() ->
                                        processWithRetry(topicName, consumer, retryEvent)),
                        delay,
                        TimeUnit.MILLISECONDS
                );

            } else {
                sendToDlq(topicName, event);
            }
        }
    }

    private void sendToDlq(String topicName, Event event) {
        try {
            DeadLetterQueue dlq =
                    dlqRegistry.computeIfAbsent(
                            topicName,
                            t -> new DeadLetterQueue(t, 1000)
                    );

            dlq.publish(event);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        running = false;
        workerPool.shutdown();
        retryScheduler.shutdown();
    }
}