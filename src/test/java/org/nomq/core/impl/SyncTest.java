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
import com.hazelcast.core.HazelcastInstance;
import org.junit.Assert;
import org.junit.Test;
import org.nomq.core.EventStore;
import org.nomq.core.NoMQ;
import org.nomq.core.NoMQBuilder;
import org.nomq.core.store.mem.InMemoryEventStore;

import java.util.concurrent.CountDownLatch;

/**
 * Tests various aspects of syncing.
 *
 * @author Tommy Wassgren
 */
public class SyncTest {
    @Test
    public void verifyThatNewInstancesAreSynced() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        // Given
        final EventStore r1 = new InMemoryEventStore();

        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final NoMQ noMQ1 = NoMQBuilder.builder()
                .hazelcast(hz1)
                .record(r1)
                .subscribe(e -> countDownLatch.countDown())
                .build()
                .start();

        noMQ1.publish("testEvent", "m1", String::getBytes);
        noMQ1.publish("testEvent", "m2", String::getBytes);
        countDownLatch.await();
        Assert.assertEquals(2, r1.replayAll().count());

        // Okay, now the messages have been published and handled by the the first instance.
        // Let's start a new instance and see if it syncs correctly.
        final EventStore r2 = new InMemoryEventStore();

        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        final NoMQ noMQ2 = NoMQBuilder.builder()
                .hazelcast(hz2)
                .record(r2)
                .build()
                .start();

        Assert.assertEquals(2, r2.replayAll().count());

        // Cleanup
        noMQ1.close();
        noMQ2.close();
    }

}
