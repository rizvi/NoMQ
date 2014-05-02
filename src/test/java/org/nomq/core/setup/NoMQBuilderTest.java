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
import org.nomq.core.NoMQ;
import org.nomq.core.process.JournalEventStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

/**
 * Test case for the setup classes. Illustrates various setup options.
 *
 * @author Tommy Wassgren
 */
public class NoMQBuilderTest {
    @Test
    public void testAdvancedSetup() throws InterruptedException, IOException {
        // Given
        final Path recordFolder = Files.createTempDirectory("org.nomq.test");
        final JournalEventStore recordEventStore = new JournalEventStore(recordFolder.toString());

        final Path playbackFolder = Files.createTempDirectory("org.nomq.test");
        final JournalEventStore playbackEventStore = new JournalEventStore(playbackFolder.toString());

        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

        final List<Event> result = new ArrayList<>();
        final NoMQ noMQ = NoMQBuilder.builder()
                .playback(playbackEventStore)
                .record(recordEventStore)
                .playbackQueue(new LinkedBlockingQueue<>())
                .executorService(executorService)
                .topic("testTopic")
                .syncAttempts(4)
                .syncTimeout(6000)
                .subscribe(result::add)
                .hazelcast(Hazelcast.newHazelcastInstance())
                .build()
                .start();

        // When
        noMQ.publish("Simple event".getBytes());

        // Wait for the message to be delivered
        Thread.sleep(500);

        // Then
        assertEquals(1, result.size());

        // Cleanup
        noMQ.stop();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSetupWithFolders() throws InterruptedException, IOException {
        // Given
        final Path recordFolder = Files.createTempDirectory("org.nomq.test");
        final Path playbackFolder = Files.createTempDirectory("org.nomq.test");

        final List<Event> result = new ArrayList<>();
        final NoMQ noMQ = NoMQBuilder.builder()
                .playback(playbackFolder.toString())
                .record(recordFolder.toString())
                .subscribe(result::add)
                .build()
                .start();

        // When
        noMQ.publish("Simple event".getBytes());

        // Wait for the message to be delivered
        Thread.sleep(200);

        // Then
        assertEquals(1, result.size());

        // Cleanup
        noMQ.stop();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSetupWithMultipleSubscribers() throws InterruptedException, IOException {
        // Given
        final Path recordFolder = Files.createTempDirectory("org.nomq.test");
        final Path playbackFolder = Files.createTempDirectory("org.nomq.test");

        final List<Event> result1 = new ArrayList<>();
        final List<Event> result2 = new ArrayList<>();
        final NoMQ noMQ = NoMQBuilder.builder()
                .playback(playbackFolder.toString())
                .record(recordFolder.toString())
                .subscribe(result1::add)
                .subscribe(result2::add)
                .build()
                .start();

        // When
        noMQ.publish("Simple event".getBytes());

        // Wait for the message to be delivered
        Thread.sleep(500);

        // Then - Assert that both subscribers were notified.
        assertEquals(1, result1.size());
        assertEquals(1, result2.size());

        // Cleanup
        noMQ.stop();
        Hazelcast.shutdownAll();
    }
}
