package org.nomq.core.transport;

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import org.nomq.core.Event;
import org.nomq.core.persistence.EventStore;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This item listener feeds the play queue with all the new entries that are added to the list. During initialization, all
 * events are stored in a temp queue so that the catchup-phase can be executed first.
 */
class PlayQueueItemListener implements ItemListener<Event> {
    private final Lock lock;
    private final BlockingQueue<Event> playQueue;
    private final EventStore recordEventStore;
    private boolean started = false;
    private final BlockingQueue<Event> tempPlayQueue;

    public PlayQueueItemListener(final EventStore recordEventStore, final BlockingQueue<Event> playQueue) {
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
