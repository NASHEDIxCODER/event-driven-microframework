package com.framework.broker.dispatcher;

import com.framework.broker.consumer.EventConsumer;
import com.framework.broker.consumer.OffsetManager;
import com.framework.broker.core.Event;
import com.framework.broker.dlq.DeadLetterQueue;
import com.framework.broker.retry.RetryHandler;
import com.framework.metrics.Counter;
import com.framework.metrics.Timer;

import java.util.Map;
import java.util.concurrent.*;

public class DispatchWorker implements Runnable {

    private final String topicName;
    private final EventConsumer consumer;
    private final Event event;

    private final OffsetManager offsetManager;
    private final RetryHandler retryHandler;
    private final ScheduledExecutorService retryScheduler;
    private final ExecutorService workerPool;
    private final Map<String, DeadLetterQueue> dlqRegistry;

    private final Counter processedCounter;
    private final Counter retryCounter;
    private final Counter dlqCounter;
    private final Timer processingTimer;

    public DispatchWorker(String topicName,
                          EventConsumer consumer,
                          Event event,
                          OffsetManager offsetManager,
                          RetryHandler retryHandler,
                          ScheduledExecutorService retryScheduler,
                          ExecutorService workerPool,
                          Map<String, DeadLetterQueue> dlqRegistry,
                          Counter processedCounter,
                          Counter retryCounter,
                          Counter dlqCounter,
                          Timer processingTimer) {

        this.topicName = topicName;
        this.consumer = consumer;
        this.event = event;
        this.offsetManager = offsetManager;
        this.retryHandler = retryHandler;   // ✅ fixed
        this.retryScheduler = retryScheduler;
        this.workerPool = workerPool;
        this.dlqRegistry = dlqRegistry;

        this.processedCounter = processedCounter;
        this.retryCounter = retryCounter;
        this.dlqCounter = dlqCounter;
        this.processingTimer = processingTimer;
    }

    @Override
    public void run() {
        process(event);
    }

    private void process(Event event) {

        int attempt = Integer.parseInt(
                event.getHeaders().getOrDefault("retryCount", "0")
        ) + 1;
        System.out.println("Attempt=" + attempt +
                " eventId=" + event.getId());

        try {

            long start = System.currentTimeMillis();

            consumer.onEvent(event);

            long duration = System.currentTimeMillis() - start;

            processingTimer.record(duration);
            processedCounter.increment();

            offsetManager.incrementOffset(consumer.getConsumerId());

        } catch (Exception ex) {

            retryCounter.increment();

            if (retryHandler.shouldRetry(attempt)) {

                long delay = retryHandler.nextDelay(attempt);

                Event retryEvent =
                        retryHandler.createRetryEvent(event, attempt);

                retryScheduler.schedule(() ->
                                workerPool.submit(
                                        new DispatchWorker(
                                                topicName,
                                                consumer,
                                                retryEvent,
                                                offsetManager,
                                                retryHandler,
                                                retryScheduler,
                                                workerPool,
                                                dlqRegistry,
                                                processedCounter,
                                                retryCounter,
                                                dlqCounter,
                                                processingTimer
                                        )
                                ),
                        delay,
                        TimeUnit.MILLISECONDS
                );

            } else {
                dlqCounter.increment();
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