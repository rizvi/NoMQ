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
import org.nomq.core.support.InMemoryEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    public void testMultiplePublishersAndVerifyOrderOfMessages() throws Exception {
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
                noMQ2.publish("testEvent", "m1", String::getBytes)
                        .onSuccess(e -> waitForOne.countDown())
                        .onFailure(thr -> Assert.fail());

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
                noMQ2.publish("testEvent", "m2", String::getBytes)
                        .onSuccess(e -> waitForOne.countDown())
                        .onFailure(thr -> Assert.fail());
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
        noMQ1.close();
        noMQ2.close();
    }

    @Test
    public void testPubSubAsync() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        final NoMQ noMQ = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(newEventStore())
                .build()
                .start();

        noMQ.publish("testEvent", "payload1", String::getBytes)
                .onSuccess(event -> countDownLatch.countDown())
                .onFailure(thr -> Assert.fail());
        noMQ.publish("testEvent", "payload2", String::getBytes)
                .onSuccess(event -> countDownLatch.countDown())
                .onFailure(thr -> Assert.fail());

        // Wait for the messages
        countDownLatch.await();

        noMQ.close();
    }

    @Test
    public void testPubSubOfPayloadAsync() throws Exception {
        final CountDownLatch type1Counter = new CountDownLatch(1);
        final CountDownLatch type2Counter = new CountDownLatch(1);

        final NoMQ noMQ = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(newEventStore())
                .subscribe("testEventType1", payload -> type1Counter.countDown(), String::new) // Subscribe using payload and converter
                .subscribe("testEventType2", payload -> type2Counter.countDown(), String::new) // Subscribe using payload and converter
                .build()
                .start();

        noMQ.publish("testEventType1", "payload1", String::getBytes); // Publish using converter
        noMQ.publish("testEventType2", "payload2", String::getBytes); // Publish using converter

        // Wait for the messages
        type1Counter.await();
        type2Counter.await();

        noMQ.close();
    }

    @Test
    public void testPubSubWithFailureAsync() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        final NoMQ noMQ = NoMQBuilder.builder()
                .record(newEventStore())
                .playback(newEventStore())
                .build()
                .start();

        // Closing hazelcast causes publishing to fail
        Hazelcast.shutdownAll();

        noMQ.publish("testEvent", "payload1", String::getBytes)
                .onSuccess(event -> fail("Should not happen"))
                .onFailure(thr -> countDownLatch.countDown());

        noMQ.publish("testEvent", "payload2", String::getBytes)
                .onSuccess(event -> fail("Should not happen"))
                .onFailure(thr -> countDownLatch.countDown());

        // Wait for the messages
        countDownLatch.await();

        noMQ.close();
    }

    @Test
    public void testPubSubWithMultipleHazelcastInstances() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        final EventStore playbackEventStore = newEventStore();

        final NoMQ noMQ1 = NoMQBuilder.builder()
                .playback(playbackEventStore)
                .subscribe(e -> countDownLatch.countDown())
                .build()
                .start();

        final NoMQ noMQ2 = NoMQBuilder.builder()
                .build()
                .start();

        noMQ2.publish("testEvent", "payload1", String::getBytes);
        noMQ2.publish("testEvent", "payload2", String::getBytes);

        // Wait for the messages
        countDownLatch.await();

        // Assertions
        assertEquals(2, playbackEventStore.replayAll().count());
        final Event event = playbackEventStore.latest().get();
        assertNotNull(event);
        assertEquals("testEvent", event.type());
        assertEquals("payload2", new String(event.payload()));

        // Cleanup
        noMQ1.close();
        noMQ2.close();
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
        return new InMemoryEventStore();
    }
}
