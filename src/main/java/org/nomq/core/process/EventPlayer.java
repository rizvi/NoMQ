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

import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.nomq.core.EventSubscriber;
import org.nomq.core.lifecycle.Startable;
import org.nomq.core.lifecycle.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * The event player sends events to the application - this is the "forward" step of the store-and-forward pattern used when
 * receiving events.
 *
 * @author Tommy Wassgren
 */
public class EventPlayer implements Startable<EventPlayer>, Stoppable {
    private final EventSubscriber[] eventSubscribers;
    private final ExecutorService executorService;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final EventStore playbackEventStore;
    private final BlockingQueue<Event> playbackQueue;
    private final EventStore recordEventStore;
    private boolean stopped = false;

    public EventPlayer(
            final BlockingQueue<Event> playbackQueue,
            final EventStore playbackEventStore,
            final EventStore recordEventStore,
            final ExecutorService executorService,
            final EventSubscriber... eventSubscribers) {
        this.playbackQueue = playbackQueue;
        this.playbackEventStore = playbackEventStore;
        this.recordEventStore = recordEventStore;
        this.executorService = executorService;
        this.eventSubscribers = eventSubscribers == null ? new EventSubscriber[0] : eventSubscribers;
    }

    @Override
    public EventPlayer start() {
        sync();
        stopped = false;
        startPlaying();
        return this;
    }

    @Override
    public void stop() {
        stopped = true;
    }

    private void startPlaying() {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                while (!stopped) {
                    try {
                        final Event event = playbackQueue.poll(500, TimeUnit.MILLISECONDS);
                        if (event != null) {
                            notifySubscribers(event);
                        }
                    } catch (final InterruptedException e) {
                        log.error("Interrupted processing", e);
                    }
                }
            }

            private void notifySubscribers(final Event event) {
                log.debug("Play event [id={}]", event.id());

                // TODO: Error handling
                for (final EventSubscriber eventSubscriber : eventSubscribers) {
                    eventSubscriber.onEvent(event);
                }
                playbackEventStore.append(event);
            }
        });
    }

    /**
     * Syncs the event store i.e. the "difference" between the playback event store and the record event store.
     */
    private void sync() {
        log.debug("Syncing event player");
        final Optional<Event> latestPlayedEvent = playbackEventStore.latest();

        final Stream<Event> stream;
        if (latestPlayedEvent.isPresent()) {
            final String idOfLatestPlayedEvent = latestPlayedEvent.get().id();
            stream = recordEventStore.replay(idOfLatestPlayedEvent);
        } else {
            stream = recordEventStore.replayAll();
        }

        stream.forEach(playbackQueue::add);
        log.debug("Event player sync completed, ready to execute [items={}]", playbackQueue.size());
    }
}
