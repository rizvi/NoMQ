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

package org.nomq.core.impl;

import com.hazelcast.core.HazelcastInstance;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.nomq.core.Startable;

import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * The event recorder stores all incoming events in the event store and notifies the shared in-memory queue. This is the "store"
 * step of the store-and-forward pattern used when receiving events.
 *
 * @author Tommy Wassgren
 */
public class EventRecorder implements Startable<EventRecorder>, AutoCloseable {
    private final EventSynchronizer eventSynchronizer;
    private final HazelcastInstance hz;
    private String listenerId;
    private NoMQMessageListener messageListener;
    private final BlockingQueue<Event> playbackQueue;
    private final EventStore recordEventStore;
    private final String topic;

    public EventRecorder(
            final AsyncEventPublisher eventPublisher,
            final BlockingQueue<Event> playbackQueue,
            final String topic,
            final HazelcastInstance hz,
            final EventStore recordEventStore,
            final long syncTimeout,
            final int maxSyncAttempts) {

        this.playbackQueue = playbackQueue;
        this.topic = topic;
        this.hz = hz;
        this.recordEventStore = recordEventStore;
        this.eventSynchronizer = new EventSynchronizer(
                eventPublisher, playbackQueue, topic, hz, recordEventStore, syncTimeout, maxSyncAttempts);
    }

    @Override
    public void close() {
        if (messageListener != null) {
            messageListener.close();
        }
        NoMQHelper.sharedTopic(hz, topic).removeMessageListener(listenerId);
        listenerId = null;
    }

    @Override
    public EventRecorder start() {
        sync();
        return this;
    }

    /**
     * Catches up from the remote collection. Retrieves all elements from the remote collection (based on the "latest" entry in
     * the recorded event store) and adds them to the event store (and local playback queue).
     */
    private void sync() {
        // Start listening to messages and store them in the playback queue. Before the queue has caught up the events are
        // stored in a temp playback queue.
        messageListener = new NoMQMessageListener(recordEventStore, playbackQueue);
        listenerId = NoMQHelper.sharedTopic(hz, topic).addMessageListener(messageListener);

        // Do the actual sync with the cluster. All processed ids are returned so that they can be removed from the temp queue
        // in the next step.
        final Set<String> processedIds = eventSynchronizer.sync();

        // Then sync the item listener as well (if we caught some events between start of sync until now)
        messageListener.sync(processedIds);
    }
}
