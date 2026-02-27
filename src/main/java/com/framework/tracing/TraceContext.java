package com.framework.tracing;

public class TraceContext {

    private static final ThreadLocal<String> correlationId =
            new ThreadLocal<>();

    public static void set(String id) {
        correlationId.set(id);
    }

    public static String get() {
        return correlationId.get();
    }

    public static void clear() {
        correlationId.remove();
    }
}