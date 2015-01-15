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

package org.nomq.core.support;

import org.nomq.core.Event;
import org.nomq.core.EventStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The default non-durable EventStore, this simple implementation uses an in-memory List that contains the events. Since it is
 * only in-memory events will not be able to survive reboots. This EventStore is mainly used for testing purposes and it is
 * strongly recommended to use a durable store for production scenarios.
 *
 * @author Tommy Wassgren
 */
public class InMemoryEventStore implements EventStore {
    private final List<Event> repository;

    public InMemoryEventStore() {
        repository = new ArrayList<>();
    }

    @Override
    public void append(final Event event) {
        repository.add(event);
    }

    @Override
    public Optional<Event> latest() {
        if (!repository.isEmpty()) {
            return Optional.of(repository.get(repository.size() - 1));
        }
        return Optional.empty();
    }

    @Override
    public Stream<Event> replay(final String latestProcessedId) {
        for (int i = 0; i < repository.size() - 1; i++) {
            if (repository.get(i).id().equals(latestProcessedId)) {
                return repository.stream().skip(i + 1);
            }
        }
        return Stream.of();
    }

    @Override
    public Stream<Event> replayAll() {
        return repository.stream();
    }
}
