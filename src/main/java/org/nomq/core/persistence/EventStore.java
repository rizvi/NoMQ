package org.nomq.core.persistence;

import org.nomq.core.Event;

import java.util.Optional;
import java.util.stream.Stream;

public interface EventStore {
    void append(Event event);

    Optional<Event> latest();

    Stream<Event> replay(String latestProcessedId);

    Stream<Event> replayAll();
}
