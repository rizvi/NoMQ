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
import com.hazelcast.core.HazelcastInstance;
import org.junit.Assert;
import org.junit.Test;
import org.nomq.core.process.JournalEventStore;
import org.nomq.core.setup.NoMQBuilder;

import java.io.IOException;
import java.nio.file.Files;

/**
 * Tests various aspects of syncing
 *
 * @author Tommy Wassgren
 */
public class SyncTest {
    @Test
    public void testSyncWhenMasterListIsEmpty() throws IOException, InterruptedException {
        // Given
        final JournalEventStore r1 = new JournalEventStore(tempFolder());
        final JournalEventStore p1 = new JournalEventStore(tempFolder());

        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final NoMQ noMq1 = NoMQBuilder.builder()
                .hazelcast(hz1)
                .record(r1)
                .playback(p1)
                .build()
                .start();

        noMq1.publish("m1".getBytes());
        noMq1.publish("m2".getBytes());

        Thread.sleep(500);
        Assert.assertEquals(2, r1.replayAll().count());

        // Now, messages have been published and delivered - clear the master queue and verify that the results are synced
        // anyway.
        hz1.getList(NoMQBuilder.DEFAULT_TOPIC).clear();

        final JournalEventStore r2 = new JournalEventStore(tempFolder());
        final JournalEventStore p2 = new JournalEventStore(tempFolder());

        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        final NoMQ noMq2 = NoMQBuilder.builder()
                .hazelcast(hz2)
                .record(r2)
                .playback(p2)
                .build()
                .start();

        Assert.assertEquals(2, r2.replayAll().count());

        // Cleanup
        noMq1.stop();
        noMq2.stop();
    }

    private String tempFolder() throws IOException {
        return Files.createTempDirectory("org.nomq.test").toString();
    }
}
