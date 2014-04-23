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

package org.nomq.core;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * An event store is responsible for storing events in the order they are appended and later be able to replay the events that
 * occurred. The event store is similar to an append-only journal.
 */
public interface EventStore {
    /**
     * Append a new event to the end of event store.
     */
    void append(Event event);

    /**
     * Find the latest event that was added to the event store.
     */
    Optional<Event> latest();

    /**
     * Replay all events starting with the event after the provided event id. Example: If the event store contains the events
     * {"1", "2", "3", "4"} and a replay is requested using "2" as the latest processed id the replay stream will contain {"3",
     * "4"}.
     */
    Stream<Event> replay(String latestProcessedId);

    /**
     * Replays all events that are stored in this event store.
     */
    Stream<Event> replayAll();
}
