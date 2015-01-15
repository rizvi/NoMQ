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

package org.nomq.core;

import com.hazelcast.core.Hazelcast;
import org.junit.Test;
import org.nomq.core.support.InMemoryEventStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * Test case for the setup classes. Illustrates various setup options.
 *
 * @author Tommy Wassgren
 */
public class NoMQBuilderTest {
    @Test
    public void testAdvancedSetup() throws Exception {
        // Given
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final List<Event> result = new ArrayList<>();
        final NoMQ noMQ = NoMQBuilder.builder()
                .playback(new InMemoryEventStore())
                .record(new InMemoryEventStore())
                .playbackQueue(new LinkedBlockingQueue<>())
                .executorService(Executors.newScheduledThreadPool(3))
                .topic("testTopic")
                .syncAttempts(4)
                .syncTimeout(6000)
                .subscribe(result::add)
                .subscribe(e -> countDownLatch.countDown())
                .hazelcast(Hazelcast.newHazelcastInstance())
                .build()
                .start();

        // When
        noMQ.publish("testEvent", "Simple event", String::getBytes);

        // Wait for the message to be delivered
        countDownLatch.await();

        // Then
        assertEquals(1, result.size());

        // Cleanup
        noMQ.close();
    }


    @Test
    public void testSetupWithMultipleSubscribers() throws Exception {
        // Given
        final List<Event> result1 = new ArrayList<>();
        final List<Event> result2 = new ArrayList<>();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final NoMQ noMQ = NoMQBuilder.builder()
                .subscribe(result1::add)
                .subscribe(result2::add)
                .subscribe(e -> countDownLatch.countDown())
                .build()
                .start();

        // When
        noMQ.publish("testEvent", "Simple event", String::getBytes);

        // Wait for the message to be delivered
        countDownLatch.await();

        // Then - Assert that both subscribers were notified.
        assertEquals(1, result1.size());
        assertEquals(1, result2.size());

        // Cleanup
        noMQ.close();
    }
}
