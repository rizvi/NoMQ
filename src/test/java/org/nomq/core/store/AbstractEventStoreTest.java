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

package org.nomq.core.store;

import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.nomq.core.support.DefaultEvent;
import org.nomq.core.support.StreamUtil;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test case for event stores, this can be used as base for all types of event stores.
 *
 * @author Tommy Wassgren
 */
public abstract class AbstractEventStoreTest {
    @Test
    public void emptyStoreShouldReturnEmptyLatest() throws Exception {
        // Given
        final EventStore eventStore = createEventStore();

        // When
        final Optional<Event> latest = eventStore.latest();

        // Then
        assertFalse(latest.isPresent());

        // Cleanup
        close(eventStore);
    }

    @Test
    public void emptyStoreShouldReturnEmptyReplay() throws Exception {
        // Given
        final EventStore eventStore = createEventStore();

        // When
        final Stream<Event> replay = eventStore.replay("non-existing-id");

        // Then
        assertEquals(0L, replay.count());

        // Cleanup
        close(eventStore);
    }

    @Test
    public void latest() throws Exception {
        // Given
        final EventStore eventStore = createEventStore();

        // When
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));

        // Then
        final Optional<Event> latest = eventStore.latest();
        assertTrue(latest.isPresent());
        assertEquals("2", latest.get().id());

        // Cleanup
        close(eventStore);
    }

    @Test
    public void multipleReads() throws Exception {
        // Given
        final EventStore eventStore = createEventStore();


        // When
        // Create some events
        final int max = 10000;
        for (int i = 0; i < max; i++) {
            eventStore.append(createEvent(Integer.toString(i)));
        }

        final Callable<Long> task = () -> eventStore.replay("20").filter(e -> {
            if (e.id().equals("50")) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            return true;
        }).count();

        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final Future<Long> f1 = executorService.submit(task);
        final Future<Long> f2 = executorService.submit(task);

        final Long c1 = f1.get();
        final Long c2 = f2.get();
        close(eventStore);

        // Then
        assertEquals(c1, Long.valueOf(max - 21));
        assertEquals(c2, Long.valueOf(max - 21));
    }

    @Test
    public void replayAll() throws Exception {
        // Given
        final EventStore eventStore = createEventStore();
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));
        eventStore.append(createEvent("3"));

        // When
        final long count = eventStore.replayAll().count();

        // Then
        assertEquals(3L, count);

        // Cleanup
        close(eventStore);
    }

    @Test
    public void verifyThatLatestDiffersWhenNewElementsHaveBeenAdded() throws Exception {
        // Given
        final EventStore eventStore = createEventStore();
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));
        final Optional<Event> latest1 = eventStore.latest();

        // When
        eventStore.append(createEvent("3"));
        final Optional<Event> latest2 = eventStore.latest();

        // Then
        assertTrue(latest1.isPresent());
        assertTrue(latest2.isPresent());
        assertEquals("2", latest1.get().id());
        assertEquals("3", latest2.get().id());

        // Cleanup
        close(eventStore);
    }

    protected void close(final EventStore eventStore) {
        if (eventStore instanceof AutoCloseable) {
            StreamUtil.close((AutoCloseable) eventStore);
        }
    }

    protected Event createEvent(final String id) {
        return new DefaultEvent(id, "testEvent", id.getBytes());
    }

    protected abstract EventStore createEventStore() throws Exception;
}
