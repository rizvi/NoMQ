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

import com.hazelcast.core.Hazelcast;
import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.EventPublisherTemplate;
import org.nomq.core.EventStore;
import org.nomq.core.NoMQ;
import org.nomq.core.setup.NoMQBuilder;
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
            final EventPublisherTemplate eventPublisherTemplate = new EventPublisherTemplate(noMQ1);
            for (int i = 0; i < nrOfMessages / 2; i++) {
                if (i % 500 == 0) {
                    Thread.yield(); // Yield, let the other thread publish
                }
                eventPublisherTemplate.publishAndWait(create("m1"));
            }
        }).start();

        new Thread(() -> {
            final EventPublisherTemplate eventPublisherTemplate = new EventPublisherTemplate(noMQ2);
            for (int i = 0; i < nrOfMessages / 2; i++) {
                if (i % 500 == 0) {
                    Thread.yield(); // Yield, let the other thread publish.
                }
                eventPublisherTemplate.publishAndWait(create("m2"));
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

        noMQ.publishAsync(create("payload1"), e -> countDownLatch.countDown());
        noMQ.publishAsync(create("payload2"), e -> countDownLatch.countDown());

        // Wait for the messages
        countDownLatch.await();

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

        noMQ.publishAsync(create("payload1"), e -> fail("Should not be possible"), thr -> countDownLatch.countDown());
        noMQ.publishAsync(create("payload2"), e -> fail("Should not be possible"), thr -> countDownLatch.countDown());

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

        final EventPublisherTemplate eventPublisherTemplate = new EventPublisherTemplate(noMQ2);
        eventPublisherTemplate.publishAndWait("payload1", i -> i.getBytes());
        eventPublisherTemplate.publishAndWait("payload2", i -> i.getBytes());

        // Wait for the messages
        countDownLatch.await();

        // Assertions
        assertEquals(2, playbackEventStore.replayAll().count());
        final Event event = playbackEventStore.latest().get();
        assertNotNull(event);
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

    private byte[] create(final String payload) {
        return payload.getBytes();
    }

    private EventStore newEventStore() throws IOException {
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");
        return new JournalEventStore(tempDirectory.toString());
    }
}
