package org.nomq.core.process;

import org.nomq.core.Event;
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
 */
public class EventPlayer implements Startable, Stoppable {
    private final EventSubscriber[] eventSubscribers;
    private final ExecutorService executorService;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final EventStore playEventStore;
    private final BlockingQueue<Event> playQueue;
    private final EventStore recordEventStore;
    private boolean stopped = false;

    public EventPlayer(
            final BlockingQueue<Event> playQueue,
            final EventStore playEventStore,
            final EventStore recordEventStore,
            final ExecutorService executorService,
            final EventSubscriber... eventSubscribers) {
        this.playQueue = playQueue;
        this.playEventStore = playEventStore;
        this.recordEventStore = recordEventStore;
        this.executorService = executorService;
        this.eventSubscribers = eventSubscribers == null ? new EventSubscriber[0] : eventSubscribers;
    }

    @Override
    public void start() {
        catchup();
        stopped = false;
        startPlaying();
    }

    @Override
    public void stop() {
        stopped = true;
    }

    /**
     * Catches up the event store i.e. the "difference" between the play event store and the record event store.
     */
    private void catchup() {
        log.debug("Catching up event player");
        final Optional<Event> latestPlayedEvent = playEventStore.latest();

        final Stream<Event> stream;
        if (latestPlayedEvent.isPresent()) {
            final String idOfLatestPlayedEvent = latestPlayedEvent.get().id();
            stream = recordEventStore.replay(idOfLatestPlayedEvent);
        } else {
            stream = recordEventStore.replayAll();
        }

        stream.forEach(e -> playQueue.add(e));
        log.debug("Event player catchup completed, ready to execute [items={}]", playQueue.size());
    }

    private void startPlaying() {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                while (!stopped) {
                    try {
                        final Event event = playQueue.poll(500, TimeUnit.MILLISECONDS);
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
                playEventStore.append(event);
            }
        });
    }
}
