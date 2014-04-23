package org.nomq.core.setup;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.nomq.core.Event;
import org.nomq.core.EventPublisher;
import org.nomq.core.EventSubscriber;
import org.nomq.core.lifecycle.Startable;
import org.nomq.core.lifecycle.Stoppable;
import org.nomq.core.process.EventPlayer;
import org.nomq.core.process.EventRecorder;
import org.nomq.core.process.EventStore;
import org.nomq.core.process.HazelcastEventPublisher;
import org.nomq.core.process.JournalEventStore;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class NoMQ implements Startable, Stoppable {
    public static class Builder {
        public static final String DEFAULT_RECORD_FOLDER = System.getProperty("user.home") + "/.nomq/record";
        public static final String DEFAULT_PLAY_FOLDER = System.getProperty("user.home") + "/.nomq/play";
        private static final String DEFAULT_TOPIC = "NoMQ";
        private EventSubscriber[] eventSubscribers;
        private ExecutorService executorService;
        private HazelcastInstance hz;
        private EventStore playEventStore;
        private BlockingQueue<Event> playQueue;
        private EventStore recordEventStore;
        private String topic;

        public Builder() {
            // Empty
        }

        public NoMQ build() {
            final BlockingQueue<Event> playQueue = playQueue();
            final EventStore playEventStore = play();
            final EventStore recordEventStore = record();
            final HazelcastInstance hz = hazelcast();
            final String topic = topic();
            final ExecutorService executorService = executorService();
            final EventSubscriber[] eventSubscribers = eventSubscribers();

            return new NoMQ(
                    hz,
                    playEventStore,
                    recordEventStore,
                    new EventRecorder(playQueue, topic, hz, recordEventStore),
                    new EventPlayer(playQueue, playEventStore, recordEventStore, executorService, eventSubscribers),
                    new HazelcastEventPublisher(topic, hz)
            );
        }

        public Builder eventSubscribers(final EventSubscriber... eventSubscribers) {
            this.eventSubscribers = eventSubscribers;
            return this;
        }

        public Builder executorService(final ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public Builder hazelcast(final HazelcastInstance hz) {
            this.hz = hz;
            return this;
        }

        public HazelcastInstance hazelcast() {
            if (hz == null) {
                final Config cfg = new Config();
                hz = Hazelcast.newHazelcastInstance(cfg);
            }

            return hz;
        }

        public Builder play(final String folder) {
            verifyFolder(folder);
            this.playEventStore = new JournalEventStore(folder);
            return this;
        }

        public Builder play(final EventStore playEventStore) {
            this.playEventStore = playEventStore;
            return this;
        }

        public Builder playQueue(final BlockingQueue<Event> playQueue) {
            this.playQueue = playQueue;
            return this;
        }

        public Builder record(final String folder) {
            verifyFolder(folder);
            this.recordEventStore = new JournalEventStore(folder);
            return this;
        }

        public Builder record(final EventStore recordEventStore) {
            this.recordEventStore = recordEventStore;
            return this;
        }

        public Builder topic(final String name) {
            this.topic = name;
            return this;
        }

        protected EventSubscriber[] eventSubscribers() {
            if (eventSubscribers == null) {
                eventSubscribers = new EventSubscriber[0];
            }
            return eventSubscribers;
        }

        protected ExecutorService executorService() {
            if (executorService == null) {
                executorService = Executors.newFixedThreadPool(2);
            }
            return executorService;
        }

        protected EventStore play() {
            if (playEventStore == null) {
                verifyFolder(DEFAULT_PLAY_FOLDER);
                playEventStore = new JournalEventStore(DEFAULT_PLAY_FOLDER);
            }

            return playEventStore;
        }

        protected BlockingQueue<Event> playQueue() {
            if (playQueue == null) {
                playQueue = new LinkedBlockingQueue<>();
            }
            return playQueue;
        }

        protected EventStore record() {
            if (recordEventStore == null) {
                verifyFolder(DEFAULT_RECORD_FOLDER);
                recordEventStore = new JournalEventStore(DEFAULT_RECORD_FOLDER);
            }
            return recordEventStore;
        }

        protected String topic() {
            if (topic == null) {
                topic = DEFAULT_TOPIC;
            }
            return topic;
        }

        private void verifyFolder(final String folder) {
            final File f = new File(folder);
            f.mkdirs();
            final Path path = f.toPath();

            if (!Files.isDirectory(path) || !Files.isWritable(path)) {
                throw new IllegalStateException(String.format("%s is an invalid path, make sure it is writeable", folder));
            }
        }
    }

    private final HazelcastInstance hz;
    private final EventStore playEventStore;
    private final EventPlayer player;
    private final EventPublisher publisher;
    private final EventStore recordEventStore;
    private final EventRecorder recorder;

    private NoMQ(
            final HazelcastInstance hz,
            final EventStore playEventStore,
            final EventStore recordEventStore,
            final EventRecorder recorder,
            final EventPlayer player,
            final EventPublisher publisher) {

        this.hz = hz;
        this.playEventStore = playEventStore;
        this.recordEventStore = recordEventStore;
        this.recorder = recorder;
        this.player = player;
        this.publisher = publisher;
    }

    public EventPublisher publisher() {
        return publisher;
    }

    @Override
    public void start() {
        start(playEventStore);
        start(recordEventStore);
        start(player);
        start(recorder);
        start(publisher);
    }

    @Override
    public void stop() {
        stop(publisher);
        stop(recorder);
        stop(player);
        stop(recordEventStore);
        stop(playEventStore);
        hz.shutdown();
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
