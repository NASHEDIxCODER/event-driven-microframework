package com.framework.gateway;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RateLimiter {

    private final int maxRequests;
    private final long windowMillis;

    private final Map<String, RequestWindow> clientWindows =
            new ConcurrentHashMap<>();

    public RateLimiter(int maxRequests, long windowMillis) {
        this.maxRequests = maxRequests;
        this.windowMillis = windowMillis;
    }

    public boolean allow(String clientId) {

        long now = Instant.now().toEpochMilli();

        clientWindows.computeIfAbsent(clientId,
                k -> new RequestWindow(now));

        RequestWindow window = clientWindows.get(clientId);

        synchronized (window) {

            if (now - window.startTime > windowMillis) {
                window.startTime = now;
                window.count = 0;
            }

            if (window.count < maxRequests) {
                window.count++;
                return true;
            }

            return false;
        }
    }

    private static class RequestWindow {
        long startTime;
        int count;

        RequestWindow(long startTime) {
            this.startTime = startTime;
            this.count = 0;
        }
    }
}