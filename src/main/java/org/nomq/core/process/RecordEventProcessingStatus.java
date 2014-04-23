package org.nomq.core.process;

import org.nomq.core.Event;

import java.util.Optional;

/**
 * Keeps the "should process" state for events during catchup. When the event recorder is initialized it typically starts with a
 * "latest processed" event. All events up to that point (including the latest processed event) should be ignored but all events
 * after that point should be handled.
 */
class RecordEventProcessingStatus {
    private final String latestProcessedId;
    private boolean startEventProcessed = false;

    public RecordEventProcessingStatus(final Optional<Event> event) {
        this.latestProcessedId = event.isPresent() ? event.get().id() : null;
    }

    public boolean shouldProcess(final String id) {
        if (latestProcessedId == null) {
            return true;
        } else if (startEventProcessed) {
            return true;
        } else if (id.equals(latestProcessedId)) {
            startEventProcessed = true;
            return true;
        }
        return false;
    }
}
