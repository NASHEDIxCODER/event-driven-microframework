package com.framework.broker.retry;

import com.framework.broker.core.Event;

public class RetryHandler {

    private final RetryPolicy policy;

    public RetryHandler(RetryPolicy policy) {
        this.policy = policy;
    }

    public boolean shouldRetry(int attempt) {
        return attempt <= policy.getMaxAttempts();
    }

    public long nextDelay(int attempt) {
        return policy.computeDelay(attempt);
    }

    public Event createRetryEvent(Event event, int attempt) {

        // Clone headers safely
        java.util.Map<String, String> newHeaders =
                new java.util.HashMap<>(event.getHeaders());

        newHeaders.put("retryCount", String.valueOf(attempt));

        return Event.builder()
                .id(event.getId())
                .topic(event.getTopic())
                .payload(event.getPayload())
                .headers(newHeaders)
                .build();
    }
}