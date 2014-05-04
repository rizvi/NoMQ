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

package org.nomq.core.performance;

import com.hazelcast.core.Hazelcast;
import org.nomq.core.NoMQ;
import org.nomq.core.setup.NoMQBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;

/**
 * @author Tommy Wassgren
 */
public class PerformanceTest {
    private final Logger log = LoggerFactory.getLogger(getClass());

    public static void main(final String[] args) throws IOException, InterruptedException {
        new PerformanceTest().sendLoadsOfEvents();
    }

    private void sendLoadsOfEvents() throws IOException, InterruptedException {
        final NoMQ noMQ1 = NoMQBuilder.builder()
                .record(tempFolder())
                .playback(tempFolder())
                .build()
                .start();

        final int nrOfEvents = 100000;
        final CountDownLatch subscriptionCounter = new CountDownLatch(nrOfEvents);
        final NoMQ noMQ2 = NoMQBuilder.builder()
                .record(tempFolder())
                .playback(tempFolder())
                .subscribe(e -> subscriptionCounter.countDown())
                .build()
                .start();

        final long start = System.currentTimeMillis();

        final CountDownLatch publishedCounter = new CountDownLatch(nrOfEvents);
        for (int i = 0; i < nrOfEvents; i++) {
            noMQ1.publishAsync(
                    "Payload #" + Integer.toString(i),
                    s -> s.getBytes(),
                    e -> publishedCounter.countDown());
        }
        publishedCounter.countDown();
        final long publishCompleted = System.currentTimeMillis();

        subscriptionCounter.await();
        final long subscriptionCompleted = System.currentTimeMillis();

        noMQ1.stop();
        noMQ2.stop();

        log.info("Performance test completed: [total={}, publish={}]", (subscriptionCompleted - start), (publishCompleted - start));
        Hazelcast.shutdownAll();
    }

    private String tempFolder() throws IOException {
        return Files.createTempDirectory("org.nomq.test").toString();
    }
}
