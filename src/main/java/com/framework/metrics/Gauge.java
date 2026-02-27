package com.framework.metrics;

import java.util.function.Supplier;

public class Gauge {

    private final Supplier<Long> supplier;

    public Gauge(Supplier<Long> supplier) {
        this.supplier = supplier;
    }

    public long get() {
        return supplier.get();
    }
}