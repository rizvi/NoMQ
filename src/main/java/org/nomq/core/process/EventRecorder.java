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

package org.nomq.core.process;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.nomq.core.lifecycle.Startable;
import org.nomq.core.lifecycle.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * The event recorder stores all incoming events in the event store and notifies the shared in-memory queue. This is the "store"
 * step of the store-and-forward pattern used when receiving events.
 *
 * @author Tommy Wassgren
 */
public class EventRecorder implements Startable<EventRecorder>, Stoppable {
    private final HazelcastInstance hazelcastInstance;
    private String listenerId;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BlockingQueue<Event> playbackQueue;
    private final EventStore recordEventStore;
    private final String topic;

    public EventRecorder(
            final BlockingQueue<Event> playbackQueue,
            final String topic,
            final HazelcastInstance hazelcastInstance,
            final EventStore recordEventStore) {

        this.playbackQueue = playbackQueue;
        this.topic = topic;
        this.hazelcastInstance = hazelcastInstance;
        this.recordEventStore = recordEventStore;
    }

    @Override
    public EventRecorder start() {
        catchup();
        return this;
    }

    @Override
    public void stop() {
        hazelcastInstance.getList(topic).removeItemListener(listenerId);
        listenerId = null;
    }

    /**
     * Catches up from the remote collection. Retrieves all elements from the remote collection (based on the "latest" entry in
     * the recorded event store) and adds them to the event store (and local playback queue).
     */
    private void catchup() {
        final IList<Event> coll = hazelcastInstance.getList(topic);

        // Start listening to messages and store them in the playback queue. Before the queue has caught up the events are stored in
        // a temp playback queue.
        final PlaybackQueueItemListener itemListener = new PlaybackQueueItemListener(recordEventStore, playbackQueue);
        listenerId = coll.addItemListener(itemListener, true);

        // Do the actual catchup from the remote queue
        final Set<String> processedIds = catchupFromQueue(coll);

        // And finally catchup the item listener as well (if we caught some events between start of catchup until now)
        itemListener.catchup(processedIds);
    }

    private Set<String> catchupFromQueue(final Collection<Event> coll) {
        final RecordEventProcessingStatus status = new RecordEventProcessingStatus(recordEventStore.latest());

        // Then, process all the messages and store a key-ref for later use
        final Set<String> processedKeys = new HashSet<>();
        coll.forEach(event -> {
            if (status.shouldProcess(event.id())) {
                log.debug("Recording event [id={}] - catchup", event.id());
                processedKeys.add(event.id());
                recordEventStore.append(event);
                playbackQueue.add(event);
            }
        });

        return processedKeys;
    }
}
