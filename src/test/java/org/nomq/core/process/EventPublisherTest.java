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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.EventPublisher;
import org.nomq.core.EventStore;
import org.nomq.core.EventSubscriber;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Tommy Wassgren
 */
public class EventPublisherTest {
    @SafeVarargs
    private static <T> T[] array(final T... elements) {
        return elements;
    }

    @Test
    public void testCatchup() throws IOException, InterruptedException {
        final HazelcastInstance publishHazelcastInstance = newHazelcastInstance();
        // Create the event stores
        final EventStore recordEventStore = newEventStore();
        final EventStore playbackEventStore = newEventStore();

        // Create the shared queue
        final BlockingQueue<Event> playbackQueue = new LinkedBlockingQueue<>();

        // Setup the topic
        final String topic = "test" + System.currentTimeMillis();

        // Create the player and recorder, then start them
        final EventPlayer player = new EventPlayer(playbackQueue, playbackEventStore, recordEventStore, Executors.newFixedThreadPool(2));
        final EventRecorder recorder = new EventRecorder(playbackQueue, topic, newHazelcastInstance(), recordEventStore);
        player.start(); // Player should always be started before the recorder
        recorder.start();

        // Publish two events
        publishTwoEvents(topic, publishHazelcastInstance);

        // Wait for a while, allow for the player to process the events
        Thread.sleep(500);

        // Wait for the player to stop properly
        player.stop();
        Thread.sleep(600);

        // Verify the results
        assertEquals(2L, playbackEventStore.replayAll().count());


        // So, now there are some entries in the playback event store. Lets fill the recording store and create new components. This
        // way the catchup functionality is tested.
        publishTwoEvents(topic, publishHazelcastInstance);

        // Wait for the recorder to finish
        Thread.sleep(500);

        // Then, reset the local queue. This means that when the player starts, no elements will be on the queue but the
        // recording and player event stores will be out of sync.
        playbackQueue.clear();

        // Finally, start the player again and do the catchup
        player.start();

        // Wait for a while, allow for the player to process the new
        Thread.sleep(200);

        // Verify the results
        assertEquals(4L, playbackEventStore.replayAll().count());
    }

    @Test
    public void testSimplePubSubWithMultipleHazelcastInstances() throws IOException, InterruptedException {
        // Publish some messages
        final String topic = "test" + System.currentTimeMillis();


        // Create the event stores
        final EventStore recordEventStore = newEventStore();
        final EventStore playbackEventStore = newEventStore();

        // Create the shared queue
        final BlockingQueue<Event> playbackQueue = new LinkedBlockingQueue<>();

        // Wire the recorder and player
        final List<Event> result = new ArrayList<>();
        final EventRecorder recorder = new EventRecorder(playbackQueue, topic, newHazelcastInstance(), recordEventStore);
        final EventPlayer player = new EventPlayer(playbackQueue, playbackEventStore, recordEventStore, Executors.newFixedThreadPool(2), array((EventSubscriber) result::add));

        // First, start the player
        player.start();

        // Then, start the recorder
        recorder.start();

        final EventPublisher eventPublisher = new NoMQEventPublisher(topic, newHazelcastInstance());
        eventPublisher.publish(create("payload1"));
        eventPublisher.publish(create("payload2"));

        // Wait a while to allow for the player to process the events
        Thread.sleep(500);

        // Assertions
        assertEquals(2, result.size());
        final Event event = result.get(0);
        assertNotNull(event);
        assertEquals("payload1", new String(event.payload()));

        // Cleanup
        Hazelcast.shutdownAll();
    }

    private byte[] create(final String payload) {
        return payload.getBytes();
    }

    private EventStore newEventStore() throws IOException {
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");
        return new JournalEventStore(tempDirectory.toString());
    }

    private HazelcastInstance newHazelcastInstance() {
        final Config cfg = new Config();
        return Hazelcast.newHazelcastInstance(cfg);
    }

    private void publishTwoEvents(final String topic, final HazelcastInstance hazelcastInstance) throws InterruptedException {
        // Publish some messages
        final EventPublisher eventPublisher = new NoMQEventPublisher(topic, hazelcastInstance);
        long seqNo = System.currentTimeMillis();
        eventPublisher.publish(create(Long.toString(++seqNo)));
        eventPublisher.publish(create(Long.toString(++seqNo)));
        Thread.sleep(2);
    }
}
