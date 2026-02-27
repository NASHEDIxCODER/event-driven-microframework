package com.framework.broker.dispatcher;

import com.framework.broker.consumer.EventConsumer;
import com.framework.broker.consumer.OffsetManager;
import com.framework.broker.core.Event;
import com.framework.broker.core.Topic;
import com.framework.broker.dlq.DeadLetterQueue;
import com.framework.broker.retry.RetryHandler;
import com.framework.broker.retry.RetryPolicy;
import com.framework.metrics.Counter;
import com.framework.metrics.MetricRegistry;
import com.framework.metrics.Timer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class EventDispatcher {

    private final DispatcherConfig config;

    private final ExecutorService workerPool;
    private final ScheduledExecutorService retryScheduler;

    private final OffsetManager offsetManager = new OffsetManager();

    private final RetryHandler retryHandler;

    private final Map<String, List<EventConsumer>> consumers =
            new ConcurrentHashMap<>();

    private final Map<String, DeadLetterQueue> dlqRegistry =
            new ConcurrentHashMap<>();

    // Metrics
    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final Counter processedCounter;
    private final Counter retryCounter;
    private final Counter dlqCounter;
    private final Timer processingTimer;

    public EventDispatcher(DispatcherConfig config) {

        this.config = config;

        this.workerPool =
                Executors.newFixedThreadPool(config.getWorkerThreads());

        this.retryScheduler =
                Executors.newScheduledThreadPool(2);

        this.retryHandler =
                new RetryHandler(
                        new RetryPolicy(3, 1000, 2.0)
                );

        // Initialize metrics once
        this.processedCounter =
                metricRegistry.counter("events.processed");

        this.retryCounter =
                metricRegistry.counter("events.retry");

        this.dlqCounter =
                metricRegistry.counter("events.dlq");

        this.processingTimer =
                metricRegistry.timer("events.processing.time");
    }

    // ============================================
    // Register Consumer
    // ============================================

    public void registerConsumer(String topicName,
                                 EventConsumer consumer) {

        consumers
                .computeIfAbsent(topicName,
                        t -> new CopyOnWriteArrayList<>())
                .add(consumer);
        offsetManager.initializeConsumer(consumer.getConsumerId());
    }

    // ============================================
    // Start Dispatching For Topic
    // ============================================

    public void start(Topic topic) {

        new Thread(() -> {

            while (!Thread.currentThread().isInterrupted()) {

                try {

                    Event event = topic.consume();
                    String topicName = topic.getName();

                    List<EventConsumer> topicConsumers =
                            consumers.get(topicName);

                    if (topicConsumers == null || topicConsumers.isEmpty()) {
                        continue;
                    }

                    for (EventConsumer consumer : topicConsumers) {

                        workerPool.submit(
                                new DispatchWorker(
                                        topicName,
                                        consumer,
                                        event,
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
                        );
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            }

        }).start();
    }

    // ============================================
    // Shutdown
    // ============================================

    public void shutdown() {
        workerPool.shutdown();
        retryScheduler.shutdown();
    }
}