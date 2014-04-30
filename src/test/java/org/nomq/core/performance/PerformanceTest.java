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

import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.NoMQ;
import org.nomq.core.process.JournalEventStore;
import org.nomq.core.setup.NoMQBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Tommy Wassgren
 */
public class PerformanceTest {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void sendLoadsOfEvents() throws IOException, InterruptedException {
        final NoMQ noMQ1 = NoMQBuilder.builder()
                .record(tempFolder())
                .playback(tempFolder())
                .build()
                .start();

        final AtomicInteger counter = new AtomicInteger(1);
        final JournalEventStore recordEventStore = new JournalEventStore(tempFolder().toString()) {
            @Override
            public void append(final Event event) {
                super.append(event);
                counter.incrementAndGet();
            }
        };

        final NoMQ noMQ2 = NoMQBuilder.builder()
                .record(recordEventStore)
                .playback(tempFolder())
                .build()
                .start();


        final long start = System.currentTimeMillis();
        final int nrOfEvents = 100000;
        for (int i = 0; i < nrOfEvents; i++) {
            noMQ1.publish(("Payload #" + Integer.toString(i)).getBytes());
        }
        final long publishedCompleted = System.currentTimeMillis();


        while (true) {
            Thread.sleep(50);
            if (counter.get() >= nrOfEvents) {

                break;
            }
        }
        final long receiveCompleted = System.currentTimeMillis();

        noMQ1.stop();
        noMQ2.stop();

        log.info("Performance test completed: [total={}, publish={}]", (receiveCompleted - start), (publishedCompleted - start));
    }

    private String tempFolder() throws IOException {
        return Files.createTempDirectory("org.nomq.test").toString();
    }
}
