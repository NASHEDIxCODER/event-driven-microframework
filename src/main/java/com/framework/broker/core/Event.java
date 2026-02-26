package com.framework.broker.core;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class Event {

    private final String id;
    private final String topic;
    private final byte[] payload;
    private final long timestamp;
    private final Map<String, String> headers;

    private Event(Builder builder) {
        this.id = builder.id != null ? builder.id : UUID.randomUUID().toString();
        this.topic = Objects.requireNonNull(builder.topic, "Topic must not be null");
        this.payload = Objects.requireNonNull(builder.payload, "Payload must not be null");
        this.timestamp = builder.timestamp != 0 ? builder.timestamp : Instant.now().toEpochMilli();
        this.headers = Collections.unmodifiableMap(new HashMap<>(builder.headers));
    }

    public String getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String topic;
        private byte[] payload;
        private long timestamp;
        private Map<String, String> headers = new HashMap<>();

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder header(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers.putAll(headers);
            return this;
        }

        public Event build() {
            return new Event(this);
        }
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", topic='" + topic + '\'' +
                ", timestamp=" + timestamp +
                ", headers=" + headers +
                '}';
    }
}