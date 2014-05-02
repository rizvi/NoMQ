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

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.nomq.core.lifecycle.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import static org.nomq.core.process.NoMQHelper.isSyncRequest;

/**
 * This item listener feeds the playback queue with all the new entries that are added to the list. During initialization, all
 * events are stored in a temp queue so that the sync-phase can be executed first.
 *
 * @author Tommy Wassgren
 */
class PlaybackQueueItemListener implements ItemListener<Event>, Stoppable {
    private final LockTemplate lockTemplate;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BlockingQueue<Event> playbackQueue;
    private final EventStore recordEventStore;
    private boolean synced = false;
    private final BlockingQueue<Event> tempPlaybackQueue;

    PlaybackQueueItemListener(final EventStore recordEventStore, final BlockingQueue<Event> playbackQueue) {
        this.recordEventStore = recordEventStore;
        this.playbackQueue = playbackQueue;
        tempPlaybackQueue = new LinkedBlockingQueue<>();
        lockTemplate = new LockTemplate(new ReentrantLock(), 0);
    }

    @Override
    public void itemAdded(final ItemEvent<Event> event) {
        lockTemplate.lock(() -> {
            if (synced) {
                if (event.getItem() != null) {
                    log.debug("Recording event [id={}]", event.getItem().id());

                    recordEventStore.append(event.getItem());

                    if (!isSyncRequest(event.getItem())) {
                        playbackQueue.add(event.getItem());
                    }
                }
            } else {
                if (event.getItem() != null) {
                    if (!isSyncRequest(event.getItem())) {
                        tempPlaybackQueue.add(event.getItem());
                    }
                }
            }
        });
    }

    @Override
    public void itemRemoved(final ItemEvent<Event> item) {
        // Do nothing
        log.info("Event [type={}]", item.getEventType());
    }

    @Override
    public void stop() {
        synced = false;
    }

    public void sync(final Set<String> processedIds) {
        lockTemplate.lock(() -> {
            removeAlreadyProcessedIds(processedIds, tempPlaybackQueue);
            tempPlaybackQueue.drainTo(playbackQueue);
            synced = true;
        });
    }

    private void removeAlreadyProcessedIds(final Set<String> processedIds, final BlockingQueue<Event> q) {
        // Fast forward local queue to avoid duplicate entries (should rarely happen but there is a small window of opportunity.
        while (true) {
            final Event event = q.peek();
            if (event != null) {
                if (!processedIds.contains(event.id())) {
                    break;
                } else {
                    // Fast forward - the element has already been processed.
                    q.remove();
                }
            } else {
                break;
            }
        }
    }
}
