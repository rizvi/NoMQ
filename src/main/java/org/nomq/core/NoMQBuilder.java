
/*
 * Copyright 2014 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.nomq.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.nomq.core.impl.AsyncEventPublisher;
import org.nomq.core.impl.EventPlayer;
import org.nomq.core.impl.EventRecorder;
import org.nomq.core.impl.NoMQImpl;
import org.nomq.core.support.InMemoryEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Setup of the NoMQ-system is done via this class. The builder design pattern is used and all the relevant settings can be
 * changed/overridden.
 *
 * <p>The following components can be specified:
 *
 * <strong>Record event store</strong>: This component defines where inbound events should be stored. The default recording
 * store is a simple in-memory version and does not support durable events.
 *
 * <strong>Playback event store</strong>: Contains the events that have been dispatched to this application. The default
 * playback store is a simple in-memory version and does not support durable events.
 *
 * <strong>Hazelcast</strong>: Various ways of configuring the Hazelcast cluster either via configuration or via a {@link
 * HazelcastInstance}
 *
 * <strong>Topic</strong>: The name of the internal Hazelcast-queue to use. This is only required if multiple NoMQ-instances
 * share the same Hazelcast-instance.
 *
 * <strong>Executor service</strong>: If a custom thread pool is to be used (e.g. if you have a shared thread pool or similar).
 * If no thread pool is defined a default fixed-size thread pool is created. </p>
 *
 * To initialize NoMQ with the default values and then publish a message the following code can be used:
 * <pre>
 *     // Initialize and start NoMQ
 *     NoMQ noMQ = NoMQBuilder.builder()
 *         .build()
 *         .start();
 *
 *     // Publish a message, all messages are of type byte[] but conversion
 *     // from arbitrary objects can be used by providing a "converter"
 *     noMQ.publish("a message".getBytes());
 * </pre>
 *
 * The following demonstrates how to register a simple subscriber that will be notified of all events in the cluster in the
 * correct order:
 * <pre>
 *     // Initialize NoMQ with an event subscriber
 *     NoMQ noMQ = NoMQBuilder.builder()
 *         .subscribe(e -> System.out.println(e.id()))
 *         .build()
 *         .start();
 * </pre>
 *
 * @author Tommy Wassgren
 */
public final class NoMQBuilder {
    public static final String DEFAULT_TOPIC = "NoMQ";
    public static final long DEFAULT_SYNC_TIMEOUT = 5000;
    public static final int DEFAULT_SYNC_ATTEMPTS = 3;
    private final Collection<Consumer<Event>> eventSubscribers = new ArrayList<>();
    private ScheduledExecutorService executorService;
    private HazelcastInstance hz;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private int maxSyncAttempts;
    private EventStore playbackEventStore;
    private BlockingQueue<Event> playbackQueue;
    private EventStore recordEventStore;
    private long syncTimeout;
    private String topic;

    /**
     * Creates this builder.
     *
     * @return The builder.
     */
    public static NoMQBuilder builder() {
        return new NoMQBuilder();
    }

    private NoMQBuilder() {
        // empty
    }

    /**
     * Build the NoMQ-instance based on the settings you provided in the earlier steps.
     *
     * @return A NoMQ-instance that has not yet been started.
     * @see #playback(org.nomq.core.EventStore)
     * @see #record(org.nomq.core.EventStore)
     * @see #hazelcast(com.hazelcast.config.Config)
     * @see #hazelcast(HazelcastInstance)
     * @see #playbackQueue(java.util.concurrent.BlockingQueue)
     * @see #executorService(java.util.concurrent.ScheduledExecutorService)
     */
    public NoMQ build() {
        final BlockingQueue<Event> playbackQueue = playbackQueue();
        final EventStore playbackEventStore = playback();
        final EventStore recordEventStore = record();
        final HazelcastInstance hz = hazelcast();
        final String topic = topic();
        final ScheduledExecutorService executorService = executorService();
        final Collection<Consumer<Event>> eventSubscribers = eventSubscribers();
        final long syncTimeout = syncTimeout();
        final int maxSyncAttempts = syncAttempts();
        final AsyncEventPublisher eventPublisher = new AsyncEventPublisher(topic, hz, executorService);

        return new NoMQImpl(
                hz,
                playbackEventStore,
                recordEventStore,
                new EventRecorder(eventPublisher, playbackQueue, topic, hz, recordEventStore, syncTimeout, maxSyncAttempts),
                new EventPlayer(playbackQueue, playbackEventStore, recordEventStore, executorService, eventSubscribers),
                eventPublisher
        );
    }

    /**
     * Provide your own thread pool instead of the default.
     *
     * @param executorService The thread pool.
     * @return The builder to allow further chaining.
     */
    public NoMQBuilder executorService(final ScheduledExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Set the Hazelcast-instance to use for NoMQBuilder. If no Hazelcast-instance has been provided a default version will be
     * created. Note that it is also possible to simply pass a Hazelcast {@link com.hazelcast.config.Config}-object to configure
     * the Hazelcast cluster.
     *
     * @param hz The hazelcast instance to use.
     * @return The builder to allow further chaining.
     * @see #hazelcast(com.hazelcast.config.Config)
     */
    public NoMQBuilder hazelcast(final HazelcastInstance hz) {
        hz.getConfig().addTopicConfig(topicConfig());
        this.hz = hz;
        return this;
    }

    /**
     * Configures the Hazelcast instance with the provided configuration object. Config-instances can be created programatically
     * or via configuration files. See <a href="http://hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#configuration">the
     * Hazelcast documentation</a> for more info.
     *
     * @param config The hazelcast configuration to use.
     * @return The builder to allow further chaining.
     * @see #hazelcast(HazelcastInstance)
     */
    public NoMQBuilder hazelcast(final Config config) {
        config.addTopicConfig(topicConfig());
        this.hz = Hazelcast.newHazelcastInstance(config);
        return this;
    }

    /**
     * Create the NoMQBuilder-instance using a custom event store for playback.
     *
     * @param playbackEventStore The playback event store.
     * @return The builder to allow further chaining.
     */
    public NoMQBuilder playback(final EventStore playbackEventStore) {
        this.playbackEventStore = playbackEventStore;
        return this;
    }

    /**
     * Sets the internal in-memory playback to use for the NoMQBuilder-instance. This is mainly used for internal usage and
     * should rarely be set but it could be useful if you wish to add statistics or similar to the in-memory queue.
     *
     * @param playbackQueue The playback queue.
     * @return The builder to allow further chaining.
     */
    public NoMQBuilder playbackQueue(final BlockingQueue<Event> playbackQueue) {
        this.playbackQueue = playbackQueue;
        return this;
    }


    /**
     * Create the NoMQBuilder-instance using a custom event store for recording.
     *
     * @param recordEventStore The record event store.
     * @return The builder to allow further chaining.
     */
    public NoMQBuilder record(final EventStore recordEventStore) {
        this.recordEventStore = recordEventStore;
        return this;
    }

    /**
     * This is where you add event subscribers that will receive events.
     *
     * @param eventSubscriber The event subscribers.
     * @return The builder to allow further chaining.
     */
    public NoMQBuilder subscribe(final Consumer<Event> eventSubscriber) {
        eventSubscribers.add(eventSubscriber);
        return this;
    }

    /**
     * Subscribe to the payload of the events - the payload is converted via the provided converter.
     *
     * @param type              The event type (this must be the same type used when publishing the event).
     * @param payloadSubscriber The payload subscriber.
     * @param converter         The payload converter.
     * @return The builder to allow for further chaining.
     */
    public <T> NoMQBuilder subscribe(
            final String type, final Consumer<T> payloadSubscriber, final Function<byte[], T> converter) {
        subscribe(event -> {
            if (type.equals(event.type())) {
                payloadSubscriber.accept(converter.apply(event.payload()));
            }
        });
        return this;
    }

    /**
     * Sets the max number of attempts that NoMQ will attempt to sync the data during startup.
     *
     * @param maxSyncAttempts Max number of sync attempts.
     * @return The builder to allow further chaining.
     * @see #syncTimeout(long)
     */
    public NoMQBuilder syncAttempts(final int maxSyncAttempts) {
        this.maxSyncAttempts = maxSyncAttempts;
        return this;
    }

    /**
     * Sets the timeout (in millis) for how long the sync operation should wait before it performs a new attempt.
     *
     * @param syncTimeout The timeout in millis.
     * @return The builder to allow further chaining.
     * @see #syncAttempts(int)
     */
    public NoMQBuilder syncTimeout(final long syncTimeout) {
        this.syncTimeout = syncTimeout;
        return this;
    }

    /**
     * Set the name of the Hazelcast-topic to use. This is only required if multiple instances of NoMQBuilder shares the same
     * Hazelcast instance. The default name is {@link NoMQBuilder#DEFAULT_TOPIC}.
     *
     * @param name The name of the topic to use.
     * @return The builder to allow further chaining.
     */
    public NoMQBuilder topic(final String name) {
        this.topic = name;
        return this;
    }

    private EventStore createDefaultStore() {
        return new InMemoryEventStore();
    }

    private Collection<Consumer<Event>> eventSubscribers() {
        return eventSubscribers;
    }

    private ScheduledExecutorService executorService() {
        if (executorService == null) {
            executorService = Executors.newScheduledThreadPool(5);
        }
        return executorService;
    }

    private HazelcastInstance hazelcast() {
        if (hz == null) {
            hazelcast(new Config());
        }

        return hz;
    }

    private EventStore playback() {
        if (playbackEventStore == null) {
            logger.warn("The default EventStore for playbacks is being used. " +
                    "This is a non-durable EventStore so events may disappear when the instance is rebooted. " +
                    "It is strongly recommended that you use a durable event store.");
            playbackEventStore = createDefaultStore();
        }

        return playbackEventStore;
    }

    private BlockingQueue<Event> playbackQueue() {
        if (playbackQueue == null) {
            playbackQueue = new LinkedBlockingQueue<>();
        }
        return playbackQueue;
    }


    private EventStore record() {
        if (recordEventStore == null) {
            logger.warn("The default EventStore for recording is being used. " +
                    "This is a non-durable EventStore so events may disappear when the instance is rebooted. " +
                    "It is strongly recommended that you use a durable event store.");

            recordEventStore = createDefaultStore();
        }
        return recordEventStore;
    }

    private int syncAttempts() {
        if (maxSyncAttempts <= 0) {
            maxSyncAttempts = DEFAULT_SYNC_ATTEMPTS;
        }
        return maxSyncAttempts;
    }

    private long syncTimeout() {
        if (syncTimeout <= 0) {
            syncTimeout = DEFAULT_SYNC_TIMEOUT;
        }
        return syncTimeout;
    }

    private String topic() {
        if (topic == null || topic.length() == 0) {
            topic = DEFAULT_TOPIC;
        }
        return topic;
    }

    /**
     * Creates the topic configuration that enables global ordering.
     *
     * @return A topic config with global ordering enabled for the topic defined by {@link #topic()}.
     */
    private TopicConfig topicConfig() {
        final TopicConfig topicConfig = new TopicConfig();
        topicConfig.setGlobalOrderingEnabled(true);
        topicConfig.setName(topic());
        return topicConfig;
    }
}
