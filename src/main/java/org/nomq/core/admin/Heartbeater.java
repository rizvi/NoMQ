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

package org.nomq.core.admin;

import org.nomq.core.NoMQ;
import org.nomq.core.NoMQBuilder;
import org.nomq.core.store.JournalEventStore;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

/**
 * Simple heartbeat application that can be used for diagnostic purposes.
 *
 * @author Tommy Wassgren
 */
public class Heartbeater {
    public static void main(final String[] args) throws IOException {
        new Heartbeater();
    }

    private Heartbeater() throws IOException {
        System.out.println("#####################################################################");
        System.out.println("#####################################################################");
        System.out.println("#####################################################################");
        System.out.println("Config: " + System.getProperty("hazelcast.config", "NO CONFIG PROVIDED"));
        System.out.println("#####################################################################");
        System.out.println("#####################################################################");
        System.out.println("#####################################################################");

        final JournalEventStore recordEventStore = new JournalEventStore(tempFolder());
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
        final NoMQ noMQ = NoMQBuilder
                .builder()
                .record(recordEventStore)
                .executorService(scheduledExecutorService)
                .subscribe(e -> System.out.println(format("Ping [id=%s, count=%s]", e.id(), new String(e.payload()))))
                .build()
                .start();

        final AtomicInteger integer = new AtomicInteger(0);
        scheduledExecutorService.scheduleWithFixedDelay(
                () -> noMQ.publishAsync(
                        "ping",
                        integer.incrementAndGet(),
                        i -> Integer.toString(i).getBytes()),
                5000,
                5000,
                TimeUnit.MILLISECONDS
        );
    }

    private String tempFolder() throws IOException {
        return Files.createTempDirectory("org.nomq.test").toString();
    }
}
