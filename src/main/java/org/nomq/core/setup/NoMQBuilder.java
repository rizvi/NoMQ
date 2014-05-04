
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

package org.nomq.core.setup;

import com.hazelcast.config.Config;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.nomq.core.Event;
import org.nomq.core.EventPublisher;
import org.nomq.core.EventPublisherCallback;
import org.nomq.core.EventStore;
import org.nomq.core.EventSubscriber;
import org.nomq.core.ExceptionCallback;
import org.nomq.core.NoMQ;
import org.nomq.core.lifecycle.Startable;
import org.nomq.core.lifecycle.Stoppable;
import org.nomq.core.process.AsyncEventPublisher;
import org.nomq.core.process.EventPlayer;
import org.nomq.core.process.EventPublisherSupport;
import org.nomq.core.process.EventRecorder;
import org.nomq.core.process.JournalEventStore;
import org.nomq.core.process.OrderedEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static org.nomq.core.setup.NoMQBuilder.PublishStrategy.ORDER_DOES_NOT_MATTER;

/**
 * Setup of the NoMQ-system is done via this class. The builder design pattern is used and all the relevant settings can be
 * changed/overridden.
 *
 * <p>The following components can be specified:
 *
 * <strong>Record event store</strong>: This component defines where inbound events should be stored.
 *
 * <strong>Playback event store</strong>: Contains the events that have been dispatched to this application.
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
 *     NoMQ noMQ = NoMQBuilder.builder().build().start();
 *
 *     // Publish a message
 *     noMQ.publish("a message".getBytes());
 * </pre>
 *
 * The following demonstrates how to register a simple subscriber that will be notified of all events in the cluster in the
 * correct order:
 * <pre>
 *     // Initialize NoMQ with an event subscriber
 *     NoMQ noMQ = NoMQBuilder.builder()
 *         .subscribe(e -> System.out.println(e.id()))
 *         .build().start();
 * </pre>
 *
 * @author Tommy Wassgren
 */
public final class NoMQBuilder {
    public static final String DEFAULT_RECORD_FOLDER = System.getProperty("user.home") + "/.nomq/record";
    public static final String DEFAULT_PLAYBACK_FOLDER = System.getProperty("user.home") + "/.nomq/playback";
    public static final String DEFAULT_TOPIC = "NoMQ";

    public static final long DEFAULT_SYNC_TIMEOUT = 5000;
    public static final int DEFAULT_SYNC_ATTEMPTS = 3;

    public enum PublishStrategy {
        ORDER_DOES_NOT_MATTER, ORDER_MATTERS
    }

    /**
     * The internal implementation of the NoMQ-system.
     */
    private static class NoMQImpl implements NoMQ {
        private final HazelcastInstance hz;
        private final Logger log = LoggerFactory.getLogger(getClass());
        private final EventStore playbackEventStore;
        private final EventPlayer player;
        private final EventPublisher publisher;
        private final EventStore recordEventStore;
        private final EventRecorder recorder;

        private NoMQImpl(
                final HazelcastInstance hz,
                final EventStore playbackEventStore,
                final EventStore recordEventStore,
                final EventRecorder recorder,
                final EventPlayer player,
                final EventPublisher publisher) {

            this.hz = hz;
            this.playbackEventStore = playbackEventStore;
            this.recordEventStore = recordEventStore;
            this.recorder = recorder;
            this.player = player;
            this.publisher = publisher;
        }

        @Override
        public Event publish(final byte[] payload) {
            return publisher.publish(payload);
        }

        @Override
        public void publish(
                final byte[] payload,
                final EventPublisherCallback publisherCallback,
                final ExceptionCallback... exceptionCallbacks) {

            publisher.publish(payload, publisherCallback, exceptionCallbacks);
        }

        @Override
        public NoMQ start() {
            start(playbackEventStore);
            start(recordEventStore);
            start(player);
            start(recorder);
            start(publisher);

            log.debug("NoMQ started [nodeId={}]", hz.getLocalEndpoint().getUuid());
            return this;
        }

        @Override
        public void stop() {
            stop(publisher);
            stop(recorder);
            stop(player);
            stop(recordEventStore);
            stop(playbackEventStore);
            hz.getLifecycleService().shutdown();
        }

        private void start(final Object startable) {
            if (startable instanceof Startable) {
                ((Startable) startable).start();
            }
        }

        private void stop(final Object stoppable) {
            if (stoppable instanceof Stoppable) {
                try {
                    ((Stoppable) stoppable).stop();
                } catch (final Throwable throwable) {
                    log.error("Error while invoking stop", throwable);
                }
            }
        }
    }

    private EventSubscriber[] eventSubscribers;
    private ScheduledExecutorService executorService;
    private HazelcastInstance hz;
    private int maxSyncAttempts;
    private EventStore playbackEventStore;
    private BlockingQueue<Event> playbackQueue;
    private PublishStrategy publishStrategy;
    private EventStore recordEventStore;
    private long syncTimeout;
    private String topic;

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
     * @see #playback(String)
     * @see #playback(org.nomq.core.EventStore)
     * @see #record(String)
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
        final EventSubscriber[] eventSubscribers = eventSubscribers();
        final long syncTimeout = syncTimeout();
        final int maxSyncAttempts = syncAttempts();
        final EventPublisherSupport eventPublisher = publisher();

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
     * @see #hazelcast(HazelcastInstance)
     */
    public NoMQBuilder hazelcast(final Config config) {
        config.addTopicConfig(topicConfig());
        this.hz = Hazelcast.newHazelcastInstance(config);
        return this;
    }

    /**
     * This sets the folder to use for the playback event store. If this value is set the folder must be writeable and the
     * standard event store is used ({@link org.nomq.core.process.JournalEventStore}.
     *
     * If you wish to provide your own playback event store simply build the NoMQBuilder-instance using the {@link
     * #playback(org.nomq.core.EventStore)}-method.
     *
     * @see #playback(org.nomq.core.EventStore)
     */
    public NoMQBuilder playback(final String folder) {
        assertThatFolderIsWriteable(folder);
        this.playbackEventStore = new JournalEventStore(folder);
        return this;
    }

    /**
     * Create the NoMQBuilder-instance using a custom event store for playback. To use the default playback event store invoke
     * the {@link #playback(String)}-method with a valid folder or simply don't invoke any of the playback-methods.
     *
     * @see #playback(String)
     */
    public NoMQBuilder playback(final EventStore playbackEventStore) {
        this.playbackEventStore = playbackEventStore;
        return this;
    }

    /**
     * Sets the internal in-memory playback to use for the NoMQBuilder-instance. This is mainly used for internal usage and
     * should rarely be set but it could be useful if you wish to add statistics or similar to the in-memory queue.
     */
    public NoMQBuilder playbackQueue(final BlockingQueue<Event> playbackQueue) {
        this.playbackQueue = playbackQueue;
        return this;
    }

    /**
     * Sets the publishing strategy to use - there are two available alternatives: The default option {@link
     * PublishStrategy#ORDER_DOES_NOT_MATTER} attempts to publish the event ASAP on any available thread. The other option
     * {@link PublishStrategy#ORDER_MATTERS} uses an event queue and a single thread to publish messages so that they are
     * guaranteed to be in the order that they are enqueued.
     *
     * @param publishStrategy The publishing strategy to use.
     * @return The builder to allow for further chaining.
     */
    public NoMQBuilder publishStrategy(final PublishStrategy publishStrategy) {
        this.publishStrategy = publishStrategy;
        return this;
    }

    /**
     * This sets the folder to use for the record event store. If this value is set the folder must be writeable and the
     * standard event store is used ({@link org.nomq.core.process.JournalEventStore}.
     *
     * If you wish to provide your own record event store simply build the NoMQBuilder-instance using the {@link
     * #record(org.nomq.core.EventStore)}-method.
     *
     * @see #record(org.nomq.core.EventStore)
     */
    public NoMQBuilder record(final String folder) {
        assertThatFolderIsWriteable(folder);
        this.recordEventStore = new JournalEventStore(folder);
        return this;
    }

    /**
     * Create the NoMQBuilder-instance using a custom event store for recording. To use the default recording event store invoke
     * the {@link #record(String)}-method with a valid folder or simply don't invoke any of the record-methods.
     *
     * @see #record(String)
     */
    public NoMQBuilder record(final EventStore recordEventStore) {
        this.recordEventStore = recordEventStore;
        return this;
    }

    /**
     * This is where you add event subscribers that will receive events.
     */
    public NoMQBuilder subscribe(final EventSubscriber... eventSubscribers) {
        if (this.eventSubscribers == null) {
            this.eventSubscribers = eventSubscribers;
        } else {
            this.eventSubscribers = merge(this.eventSubscribers, eventSubscribers);
        }
        return this;
    }

    /**
     * Sets the max number of attempts that NoMQ will attempt to sync the data during startup.
     *
     * @see #syncTimeout(long)
     */
    public NoMQBuilder syncAttempts(final int maxSyncAttempts) {
        this.maxSyncAttempts = maxSyncAttempts;
        return this;
    }

    /**
     * Sets the timeout (in millis) for how long the sync operation should wait before it performs a new attempt.
     *
     * @see #syncAttempts(int)
     */
    public NoMQBuilder syncTimeout(final long syncTimeout) {
        this.syncTimeout = syncTimeout;
        return this;
    }

    /**
     * Set the name of the Hazelcast-topic to use. This is only required if multiple instances of NoMQBuilder shares the same
     * Hazelcast instance. The default name is {@link NoMQBuilder#DEFAULT_TOPIC}.
     */
    public NoMQBuilder topic(final String name) {
        this.topic = name;
        return this;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void assertThatFolderIsWriteable(final String folder) {
        final File f = new File(folder);
        f.mkdirs();
        final Path path = f.toPath();

        if (!Files.isDirectory(path) || !Files.isWritable(path)) {
            throw new IllegalStateException(String.format("%s is an invalid path, make sure it is writeable", folder));
        }
    }

    private EventSubscriber[] eventSubscribers() {
        if (eventSubscribers == null) {
            eventSubscribers = new EventSubscriber[0];
        }
        return eventSubscribers;
    }

    private ScheduledExecutorService executorService() {
        if (executorService == null) {
            executorService = Executors.newScheduledThreadPool(3);
        }
        return executorService;
    }

    private HazelcastInstance hazelcast() {
        if (hz == null) {
            hazelcast(new Config());
        }

        return hz;
    }

    private EventSubscriber[] merge(final EventSubscriber[] first, final EventSubscriber[] second) {
        final int firstLen = first.length;
        final int secondLen = second.length;
        final EventSubscriber[] merged = new EventSubscriber[firstLen + secondLen];
        System.arraycopy(first, 0, merged, 0, firstLen);
        System.arraycopy(second, 0, merged, firstLen, secondLen);
        return merged;
    }

    private EventStore playback() {
        if (playbackEventStore == null) {
            assertThatFolderIsWriteable(DEFAULT_PLAYBACK_FOLDER);
            playbackEventStore = new JournalEventStore(DEFAULT_PLAYBACK_FOLDER);
        }

        return playbackEventStore;
    }

    private BlockingQueue<Event> playbackQueue() {
        if (playbackQueue == null) {
            playbackQueue = new LinkedBlockingQueue<>();
        }
        return playbackQueue;
    }

    private PublishStrategy publishStrategy() {
        return publishStrategy == null ? ORDER_DOES_NOT_MATTER : publishStrategy;
    }

    private EventPublisherSupport publisher() {
        switch (publishStrategy()) {
            case ORDER_MATTERS:
                return new OrderedEventPublisher(topic(), hazelcast(), executorService());
        }
        return new AsyncEventPublisher(topic(), hazelcast(), executorService());
    }

    private EventStore record() {
        if (recordEventStore == null) {
            assertThatFolderIsWriteable(DEFAULT_RECORD_FOLDER);
            recordEventStore = new JournalEventStore(DEFAULT_RECORD_FOLDER);
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
     */
    private TopicConfig topicConfig() {
        final TopicConfig topicConfig = new TopicConfig();
        topicConfig.setGlobalOrderingEnabled(true);
        topicConfig.setName(topic());
        return topicConfig;
    }
}
