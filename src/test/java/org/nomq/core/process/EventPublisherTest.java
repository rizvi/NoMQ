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

import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.nomq.core.NoMQ;
import org.nomq.core.setup.NoMQBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Tommy Wassgren
 */
public class EventPublisherTest {
    @Test
    public void testSimplePubSubWithMultipleHazelcastInstances() throws IOException, InterruptedException {
        final EventStore playbackEventStore = newEventStore();

        final NoMQ noMQ1 = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(playbackEventStore)
                .build()
                .start();

        final NoMQ noMQ2 = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(newEventStore())
                .build()
                .start();

        noMQ2.publish(create("payload1"));
        noMQ2.publish(create("payload2"));

        // Wait a while to allow for the player to process the events
        Thread.sleep(500);

        // Assertions
        assertEquals(2, playbackEventStore.replayAll().count());
        final Event event = playbackEventStore.latest().get();
        assertNotNull(event);
        assertEquals("payload2", new String(event.payload()));

        // Cleanup
        noMQ1.stop();
        noMQ2.stop();
    }


    private byte[] create(final String payload) {
        return payload.getBytes();
    }

    private EventStore newEventStore() throws IOException {
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");
        return new JournalEventStore(tempDirectory.toString());
    }
}
