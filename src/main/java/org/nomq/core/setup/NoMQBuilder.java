
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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.nomq.core.Event;
import org.nomq.core.EventPublisher;
import org.nomq.core.EventStore;
import org.nomq.core.EventSubscriber;
import org.nomq.core.NoMQ;
import org.nomq.core.lifecycle.Startable;
import org.nomq.core.lifecycle.Stoppable;
import org.nomq.core.process.EventPlayer;
import org.nomq.core.process.EventRecorder;
import org.nomq.core.process.JournalEventStore;
import org.nomq.core.process.NoMQEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Setup of the NoMQBuilder-system is done via this class. The builder design pattern is used and all the relevant settings can
 * be changed/overridden.
 *
 * <p>The following components can be specified:
 *
 * <strong>Record event store</strong>: This component defines where inbound events should be stored.
 *
 * <strong>Playback event store</strong>: Contains the events that have been dispatched to this application.
 *
 * <strong>Hazelcast</strong>: Various ways of configuring the Hazelcast cluster either via configuration or via a {@link
 * com.hazelcast.core.HazelcastInstance}
 *
 * <strong>Topic</strong>: The name of the internal Hazelcast-queue to use. This is only required if multiple
 * NoMQBuilder-instances share the same Hazelcast-instance.
 *
 * <strong>Executor service</strong>: If a custom thread pool is to be used (e.g. if you have a shared thread pool or similar).
 * If no thread pool is defined a default fixed-size thread pool is created. </p>
 *
 *
 * To initialize NoMQBuilder with the default values and then publish a message the following code can be used:
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
 *     // Initialize NoMQBuilder with an event subscriber
 *     NoMQBuilder noMQ = NoMQBuilder.builder()
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

    /**
     * The internal implementation of the NoMQ-system.
     */
    private static class NoMQSystem implements NoMQ {
        private final HazelcastInstance hz;
        private final Logger log = LoggerFactory.getLogger(getClass());
        private final EventStore playbackEventStore;
        private final EventPlayer player;
        private final EventPublisher publisher;
        private final EventStore recordEventStore;
        private final EventRecorder recorder;

        private NoMQSystem(
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
        public String publish(final byte[] payload) {
            return publisher.publish(payload);
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
                ((Stoppable) stoppable).stop();
            }
        }
    }

    private EventSubscriber[] eventSubscribers;
    private ExecutorService executorService;
    private HazelcastInstance hz;
    private int maxSyncAttempts;
    private EventStore playbackEventStore;
    private BlockingQueue<Event> playbackQueue;
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
     * Build the NoMQBuilder-instance based on the settings you provided in the earlier steps.
     *
     * @return A NoMQBuilder-instance that has not yet been started.
     * @see #playback(String)
     * @see #playback(org.nomq.core.EventStore)
     * @see #record(String)
     * @see #record(org.nomq.core.EventStore)
     * @see #hazelcast(com.hazelcast.config.Config)
     * @see #hazelcast(com.hazelcast.core.HazelcastInstance)
     * @see #playbackQueue(java.util.concurrent.BlockingQueue)
     * @see #executorService(java.util.concurrent.ExecutorService)
     */
    public NoMQ build() {
        final BlockingQueue<Event> playbackQueue = playbackQueue();
        final EventStore playbackEventStore = playback();
        final EventStore recordEventStore = record();
        final HazelcastInstance hz = hazelcast();
        final String topic = topic();
        final ExecutorService executorService = executorService();
        final EventSubscriber[] eventSubscribers = eventSubscribers();
        final long syncTimeout = syncTimeout();
        final int maxSyncAttempts = maxSyncAttempts();

        return new NoMQSystem(
                hz,
                playbackEventStore,
                recordEventStore,
                new EventRecorder(playbackQueue, topic, hz, recordEventStore, syncTimeout, maxSyncAttempts),
                new EventPlayer(playbackQueue, playbackEventStore, recordEventStore, executorService, eventSubscribers),
                new NoMQEventPublisher(topic, hz)
        );
    }

    /**
     * Provide your own thread pool instead of the default.
     */
    public NoMQBuilder executorService(final ExecutorService executorService) {
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
        this.hz = hz;
        return this;
    }

    /**
     * Configures the Hazelcast instance with the provided configuration object. Config-instances can be created programatically
     * or via configuration files. See <a href="http://hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#configuration">the
     * Hazelcast documentation</a> for more info.
     *
     * @see #hazelcast(com.hazelcast.core.HazelcastInstance)
     */
    public NoMQBuilder hazelcast(final Config config) {
        this.hz = Hazelcast.newHazelcastInstance(config);
        return this;
    }

    public NoMQBuilder maxSyncAttempts(final int maxSyncAttempts) {
        this.maxSyncAttempts = maxSyncAttempts;
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
        assertWriteableFolder(folder);
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
     * This sets the folder to use for the record event store. If this value is set the folder must be writeable and the
     * standard event store is used ({@link org.nomq.core.process.JournalEventStore}.
     *
     * If you wish to provide your own record event store simply build the NoMQBuilder-instance using the {@link
     * #record(org.nomq.core.EventStore)}-method.
     *
     * @see #record(org.nomq.core.EventStore)
     */
    public NoMQBuilder record(final String folder) {
        assertWriteableFolder(folder);
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
    private void assertWriteableFolder(final String folder) {
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

    private ExecutorService executorService() {
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(2);
        }
        return executorService;
    }

    private HazelcastInstance hazelcast() {
        if (hz == null) {
            hazelcast(new Config());
        }

        return hz;
    }

    private int maxSyncAttempts() {
        if (maxSyncAttempts <= 0) {
            maxSyncAttempts = 3;
        }
        return maxSyncAttempts;
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
            assertWriteableFolder(DEFAULT_PLAYBACK_FOLDER);
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

    private EventStore record() {
        if (recordEventStore == null) {
            assertWriteableFolder(DEFAULT_RECORD_FOLDER);
            recordEventStore = new JournalEventStore(DEFAULT_RECORD_FOLDER);
        }
        return recordEventStore;
    }

    private long syncTimeout() {
        if (syncTimeout <= 0) {
            syncTimeout = 5000;
        }
        return syncTimeout;
    }

    private String topic() {
        if (topic == null) {
            topic = DEFAULT_TOPIC;
        }
        return topic;
    }
}
