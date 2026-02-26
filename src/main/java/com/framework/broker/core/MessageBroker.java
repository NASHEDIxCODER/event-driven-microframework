package com.framework.broker.core;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MessageBroker {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final int defaultTopicCapacity;

    public MessageBroker(int defaultTopicCapacity) {
        this.defaultTopicCapacity = defaultTopicCapacity;
    }

    /**
     * Create topic if it doesn't exist.
     */
    public Topic createTopic(String topicName) {
        Objects.requireNonNull(topicName, "Topic name cannot be null");

        return topics.computeIfAbsent(
                topicName,
                name -> new Topic(name, defaultTopicCapacity)
        );
    }

    /**
     * Get existing topic.
     */
    public Topic getTopic(String topicName) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        return topic;
    }

    /**
     * Publish event to a topic.
     */
    public void publish(Event event) throws InterruptedException {
        Objects.requireNonNull(event, "Event cannot be null");

        Topic topic = topics.get(event.getTopic());
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + event.getTopic());
        }

        topic.publish(event);
    }

    /**
     * List all topics.
     */
    public Set<String> listTopics() {
        return topics.keySet();
    }

    /**
     * Check if topic exists.
     */
    public boolean topicExists(String topicName) {
        return topics.containsKey(topicName);
    }
}