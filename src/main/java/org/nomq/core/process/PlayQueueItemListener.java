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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This item listener feeds the play queue with all the new entries that are added to the list. During initialization, all
 * events are stored in a temp queue so that the catchup-phase can be executed first.
 *
 * @author Tommy Wassgren
 */
class PlayQueueItemListener implements ItemListener<Event> {
    private final Lock lock;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final BlockingQueue<Event> playQueue;
    private final EventStore recordEventStore;
    private boolean started = false;
    private final BlockingQueue<Event> tempPlayQueue;

    PlayQueueItemListener(final EventStore recordEventStore, final BlockingQueue<Event> playQueue) {
        this.recordEventStore = recordEventStore;
        this.playQueue = playQueue;
        tempPlayQueue = new LinkedBlockingQueue<>();
        lock = new ReentrantLock();
    }

    public void catchup(final Set<String> processedIds) {
        lock.lock();
        try {
            removeAlreadyProcessedIds(processedIds, tempPlayQueue);
            tempPlayQueue.drainTo(playQueue);
            started = true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void itemAdded(final ItemEvent<Event> event) {
        lock.lock();
        try {
            if (started) {
                log.debug("Recording event [id={}]", event.getItem().id());
                recordEventStore.append(event.getItem());
                playQueue.add(event.getItem());
            } else {
                tempPlayQueue.add(event.getItem());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void itemRemoved(final ItemEvent<Event> item) {
        // Do nothing
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
