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
import org.junit.Assert;
import org.nomq.core.NoMQ;
import org.nomq.core.NoMQBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author Tommy Wassgren
 */
class PerformanceTest {
    private final Logger log = LoggerFactory.getLogger(getClass());

    public static void main(final String[] args) throws Exception {
        new PerformanceTest().sendLoadsOfEvents();
    }

    private void sendLoadsOfEvents() throws Exception {
        final NoMQ noMQ1 = NoMQBuilder.builder()
                .build()
                .start();

        final int nrOfEvents = 100000;
        final CountDownLatch subscriptionCounter = new CountDownLatch(nrOfEvents);
        final NoMQ noMQ2 = NoMQBuilder.builder()
                .subscribe(e -> subscriptionCounter.countDown())
                .build()
                .start();

        final long start = System.currentTimeMillis();

        final CountDownLatch publishedCounter = new CountDownLatch(nrOfEvents);
        for (int i = 0; i < nrOfEvents; i++) {
            noMQ1.publish("performanceEvent", "Payload #" + Integer.toString(i), String::getBytes)
                    .onSuccess(event -> publishedCounter.countDown())
                    .onFailure(thr -> Assert.fail());
        }

        publishedCounter.countDown();
        final long publishCompleted = System.currentTimeMillis();

        subscriptionCounter.await();
        final long subscriptionCompleted = System.currentTimeMillis();

        noMQ1.close();
        noMQ2.close();

        log.info("Performance test completed: [total={}, publish={}]", (subscriptionCompleted - start), (publishCompleted - start));
        Hazelcast.shutdownAll();
    }
}
