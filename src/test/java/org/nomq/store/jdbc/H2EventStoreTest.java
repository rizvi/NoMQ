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

package org.nomq.store.jdbc;

import org.junit.Test;
import org.nomq.core.EventStore;
import org.nomq.core.store.AbstractEventStoreTest;
import org.nomq.store.jdbc.H2EventStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * @author Tommy Wassgren
 */
public class H2EventStoreTest extends AbstractEventStoreTest {
    private final String uniqueDbName;

    public H2EventStoreTest() throws IOException {
        uniqueDbName = UUID.randomUUID().toString();
    }

    @Test
    public void appendCloseAndAppend() throws Exception {
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");

        // Given
        EventStore eventStore = new H2EventStore("jdbc:h2:" + tempDirectory.toString()).start();
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));
        close(eventStore);

        // When
        eventStore = new H2EventStore("jdbc:h2:" + tempDirectory.toString()).start();
        eventStore.append(createEvent("3"));

        // Then
        final long count = eventStore.replay("2").count();
        assertEquals(1L, count);

        // Cleanup
        close(eventStore);
    }

    @Override
    protected EventStore createEventStore() throws IOException {
        return new H2EventStore("jdbc:h2:mem:" + uniqueDbName).start();
    }
}
