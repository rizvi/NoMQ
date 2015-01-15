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

package org.nomq.store.journalio;

import org.junit.Test;
import org.nomq.core.EventStore;
import org.nomq.core.store.AbstractEventStoreTest;
import org.nomq.store.journalio.JournalEventStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

/**
 * Tests on the event store.
 *
 * @author Tommy Wassgren
 */
public class JournalEventStoreTest extends AbstractEventStoreTest {
    // Given
    private final Path tempDirectory;

    public JournalEventStoreTest() throws IOException {
        tempDirectory = Files.createTempDirectory("org.nomq.test");
    }

    @Test
    public void appendCloseAndAppend() throws Exception {
        // Given
        EventStore eventStore = createEventStore();
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));
        close(eventStore);

        // When
        eventStore = createEventStore();
        eventStore.append(createEvent("3"));

        // Then
        final long count = eventStore.replay("2").count();
        assertEquals(1L, count);

        // Cleanup
        close(eventStore);
    }

    @Override
    protected EventStore createEventStore() throws IOException {
        return new JournalEventStore(tempDirectory.toString());
    }
}
