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

package org.nomq.core.setup;

import com.hazelcast.core.Hazelcast;
import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.process.JournalEventStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * @author Tommy Wassgren
 */
public class NoMQTest {
    @Test
    public void testAdvancedSetup() throws InterruptedException, IOException {
        // Given
        final Path recordFolder = Files.createTempDirectory("org.nomq.test");
        final JournalEventStore recordEventStore = new JournalEventStore(recordFolder.toString());

        final Path playFolder = Files.createTempDirectory("org.nomq.test");
        final JournalEventStore playEventStore = new JournalEventStore(playFolder.toString());

        final ExecutorService executorService = Executors.newFixedThreadPool(2);

        final List<Event> result = new ArrayList<>();
        final NoMQ noMQ =
                new NoMQ.Builder()
                        .play(playEventStore)
                        .record(recordEventStore)
                        .playQueue(new LinkedBlockingQueue<>())
                        .executorService(executorService)
                        .topic("testTopic")
                        .eventSubscribers(result::add)
                        .hazelcast(Hazelcast.newHazelcastInstance())
                        .build();
        noMQ.start();

        // When
        noMQ.publisher().publish("Simple event".getBytes());

        // Wait for the message to be delivered
        Thread.sleep(200);

        // Then
        assertEquals(1, result.size());

        // Cleanup
        noMQ.stop();
    }

    @Test
    public void testSetupWithFolders() throws InterruptedException, IOException {
        // Given
        final Path recordFolder = Files.createTempDirectory("org.nomq.test");
        final Path playFolder = Files.createTempDirectory("org.nomq.test");

        final List<Event> result = new ArrayList<>();
        final NoMQ noMQ =
                new NoMQ.Builder()
                        .play(playFolder.toString())
                        .record(recordFolder.toString())
                        .eventSubscribers(result::add)
                        .build();
        noMQ.start();

        // When
        noMQ.publisher().publish("Simple event".getBytes());

        // Wait for the message to be delivered
        Thread.sleep(200);

        // Then
        assertEquals(1, result.size());

        // Cleanup
        noMQ.stop();
    }
}
