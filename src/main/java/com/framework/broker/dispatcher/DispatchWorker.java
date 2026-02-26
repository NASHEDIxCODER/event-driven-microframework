package com.framework.broker.dispatcher;

import com.framework.broker.consumer.EventConsumer;
import com.framework.broker.consumer.OffsetManager;
import com.framework.broker.core.Event;
import com.framework.broker.dlq.DeadLetterQueue;
import com.framework.broker.retry.RetryPolicy;

import java.util.Map;
import java.util.concurrent.*;

public class DispatchWorker implements Runnable {

    private final String topicName;
    private final EventConsumer consumer;
    private final Event event;

    private final Counter processedCounter;
    private final Counter retryCounter;
    private final Counter dlqCounter;
    private final Timer processingTimer;


    private final OffsetManager offsetManager;
    private final RetryPolicy retryPolicy;
    private final ScheduledExecutorService retryScheduler;
    private final ExecutorService workerPool;
    private final Map<String, DeadLetterQueue> dlqRegistry;

    public DispatchWorker(String topicName,
                          EventConsumer consumer,
                          Event event,
                          OffsetManager offsetManager,
                          RetryPolicy retryPolicy,
                          ScheduledExecutorService retryScheduler,
                          ExecutorService workerPool,
                          Map<String, DeadLetterQueue> dlqRegistry) {

        this.topicName = topicName;
        this.consumer = consumer;
        this.event = event;
        this.offsetManager = offsetManager;
        this.retryPolicy = retryPolicy;
        this.retryScheduler = retryScheduler;
        this.workerPool = workerPool;
        this.dlqRegistry = dlqRegistry;
    }

    @Override
    public void run() {
        process(event);
    }

    private void process(Event event) {

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
                                workerPool.submit(
                                        new DispatchWorker(
                                                topicName,
                                                consumer,
                                                retryEvent,
                                                offsetManager,
                                                retryPolicy,
                                                retryScheduler,
                                                workerPool,
                                                dlqRegistry
                                        )
                                ),
                        delay,
                        TimeUnit.MILLISECONDS
                );

            } else {
                sendToDlq(event);
            }
        }
    }

    private void sendToDlq(Event event) {
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
}