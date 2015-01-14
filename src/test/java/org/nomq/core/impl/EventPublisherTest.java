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

package org.nomq.core.impl;

import com.hazelcast.core.Hazelcast;
import org.junit.Assert;
import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.nomq.core.NoMQ;
import org.nomq.core.NoMQBuilder;
import org.nomq.core.store.JournalEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Various tests for publishing.
 *
 * @author Tommy Wassgren
 */
public class EventPublisherTest {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testMultiplePublishersAndVerifyOrderOfMessages() throws IOException, InterruptedException {
        final int nrOfMessages = 10000;
        final CountDownLatch countDownLatch = new CountDownLatch(nrOfMessages);

        final EventStore p1 = newEventStore();
        final NoMQ noMQ1 = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(p1)
                .subscribe(e -> countDownLatch.countDown())
                .build()
                .start();

        final EventStore p2 = newEventStore();
        final NoMQ noMQ2 = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(p2)
                .build()
                .start();

        new Thread(() -> {
            for (int i = 0; i < nrOfMessages / 2; i++) {
                if (i % 500 == 0) {
                    Thread.yield(); // Yield, let the other thread publish
                }
                final CountDownLatch waitForOne = new CountDownLatch(1);
                noMQ2.publishAsync("testEvent", "m1", String::getBytes, e -> waitForOne.countDown(), thr -> {
                });
                try {
                    waitForOne.await();
                } catch (InterruptedException e) {
                    Assert.fail();
                }
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < nrOfMessages / 2; i++) {
                if (i % 500 == 0) {
                    Thread.yield(); // Yield, let the other thread publish.
                }
                final CountDownLatch waitForOne = new CountDownLatch(1);
                noMQ2.publishAsync("testEvent", "m2", String::getBytes, e -> waitForOne.countDown(), thr -> {
                });
                try {
                    waitForOne.await();
                } catch (InterruptedException e) {
                    Assert.fail();
                }
            }
        }).start();

        // Wait for the messages (and also make sure that all messages have arrived)
        countDownLatch.await();

        assertStores(p1, p2);

        // Cleanup
        noMQ1.stop();
        noMQ2.stop();
    }

    @Test
    public void testPubSubAsync() throws IOException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        final NoMQ noMQ = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(newEventStore())
                .build()
                .start();

        noMQ.publishAsync(
                "testEvent", "payload1", String::getBytes, event -> countDownLatch.countDown(), thr -> {
                });
        noMQ.publishAsync(
                "testEvent", "payload2", String::getBytes, event -> countDownLatch.countDown(), thr -> {
                });

        // Wait for the messages
        countDownLatch.await();

        noMQ.stop();
    }

    @Test
    public void testPubSubOfPayloadAsync() throws IOException, InterruptedException {
        final CountDownLatch type1Counter = new CountDownLatch(1);
        final CountDownLatch type2Counter = new CountDownLatch(1);

        final NoMQ noMQ = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(newEventStore())
                .subscribe("testEventType1", payload -> type1Counter.countDown(), String::new) // Subscribe using payload and converter
                .subscribe("testEventType2", payload -> type2Counter.countDown(), String::new) // Subscribe using payload and converter
                .build()
                .start();

        noMQ.publishAsync("testEventType1", "payload1", String::getBytes); // Publish using converter
        noMQ.publishAsync("testEventType2", "payload2", String::getBytes); // Publish using converter

        // Wait for the messages
        type1Counter.await();
        type2Counter.await();

        noMQ.stop();
    }

    @Test
    public void testPubSubWithFailureAsync() throws IOException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        final NoMQ noMQ = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(newEventStore())
                .build()
                .start();

        // Closing hazelcast causes publishing to fail
        Hazelcast.shutdownAll();

        noMQ.publishAsync("testEvent", "payload1", String::getBytes, event -> fail("Should not happen"), thr -> countDownLatch.countDown());
        noMQ.publishAsync("testEvent", "payload2", String::getBytes, event -> fail("Should not happen"), thr -> countDownLatch.countDown());

        // Wait for the messages
        countDownLatch.await();

        noMQ.stop();
    }

    @Test
    public void testPubSubWithMultipleHazelcastInstances() throws IOException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        final EventStore playbackEventStore = newEventStore();

        final NoMQ noMQ1 = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(playbackEventStore)
                .subscribe(e -> countDownLatch.countDown())
                .build()
                .start();

        final NoMQ noMQ2 = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(newEventStore())
                .build()
                .start();

        noMQ2.publishAsync("testEvent", "payload1", String::getBytes);
        noMQ2.publishAsync("testEvent", "payload2", String::getBytes);

        // Wait for the messages
        countDownLatch.await();

        // Assertions
        assertEquals(2, playbackEventStore.replayAll().count());
        final Event event = playbackEventStore.latest().get();
        assertNotNull(event);
        assertEquals("testEvent", event.type());
        assertEquals("payload2", new String(event.payload()));

        // Cleanup
        noMQ1.stop();
        noMQ2.stop();
    }

    private void assertStores(final EventStore s1, final EventStore s2) {
        final long start = System.currentTimeMillis();
        final List<String> l1 = s1.replayAll().map(Event::id).collect(Collectors.toList());
        final List<String> l2 = s2.replayAll().map(Event::id).collect(Collectors.toList());
        final long stop = System.currentTimeMillis();

        log.info("Replay took {}ms", (stop - start));
        assertEquals(l1, l2);

    }

    private EventStore newEventStore() throws IOException {
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");
        return new JournalEventStore(tempDirectory.toString());
    }
}
