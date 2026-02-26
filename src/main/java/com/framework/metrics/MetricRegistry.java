package com.framework.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricRegistry {

    private final Map<String, Counter> counters =
            new ConcurrentHashMap<>();

    private final Map<String, Gauge> gauges =
            new ConcurrentHashMap<>();

    private final Map<String, Timer> timers =
            new ConcurrentHashMap<>();

    public Counter counter(String name) {
        return counters.computeIfAbsent(name, k -> new Counter());
    }

    public void registerGauge(String name, Gauge gauge) {
        gauges.put(name, gauge);
    }

    public Timer timer(String name) {
        return timers.computeIfAbsent(name, k -> new Timer());
    }

    public void printAll() {

        System.out.println("==== Metrics Snapshot ====");

        counters.forEach((k, v) ->
                System.out.println("Counter: " + k + " = " + v.get()));

        gauges.forEach((k, v) ->
                System.out.println("Gauge: " + k + " = " + v.get()));

        timers.forEach((k, v) ->
                System.out.println("Timer: " + k +
                        " avg=" + v.getAverage() +
                        "ms count=" + v.getCount()));
    }
}