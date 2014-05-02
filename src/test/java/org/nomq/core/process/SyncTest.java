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
import com.hazelcast.core.HazelcastInstance;
import org.junit.Assert;
import org.junit.Test;
import org.nomq.core.NoMQ;
import org.nomq.core.setup.NoMQBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;

/**
 * Tests various aspects of syncing.
 *
 * @author Tommy Wassgren
 */
public class SyncTest {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void verifyThatNewInstancesAreSynced() throws IOException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        // Given
        final JournalEventStore r1 = new JournalEventStore(tempFolder());

        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final NoMQ noMQ1 = NoMQBuilder.builder()
                .hazelcast(hz1)
                .record(r1)
                .playback(tempFolder())
                .subscribe(e -> countDownLatch.countDown())
                .build()
                .start();

        publish(noMQ1, "m1");
        publish(noMQ1, "m2");
        countDownLatch.await();
        Assert.assertEquals(2, r1.replayAll().count());

        // Okay, now the messages have been published and handled by the the first instance.
        // Let's start a new instance and see if it syncs correctly.
        final JournalEventStore r2 = new JournalEventStore(tempFolder());

        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        final NoMQ noMQ2 = NoMQBuilder.builder()
                .hazelcast(hz2)
                .record(r2)
                .playback(tempFolder())
                .build()
                .start();

        Assert.assertEquals(2, r2.replayAll().count());

        // Cleanup
        noMQ1.stop();
        noMQ2.stop();
    }

    private void publish(final NoMQ noMQ, final String message) {
        final String id = noMQ.publish(message.getBytes());
        log.debug("Published message [id={}]", id);
    }

    private String tempFolder() throws IOException {
        return Files.createTempDirectory("org.nomq.test").toString();
    }
}
