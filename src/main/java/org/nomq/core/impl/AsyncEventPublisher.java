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

import com.hazelcast.core.HazelcastInstance;
import org.nomq.core.Event;
import org.nomq.core.PublishResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * The implementation of the event publisher, this publisher uses {@link CompletableFuture} for async requests and also provides
 * a blocking version of the publish-method since this is used during sync.
 *
 * @author Tommy Wassgren
 */
public class AsyncEventPublisher {
    private final ExecutorService executorService;
    private final HazelcastInstance hz;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String topic;

    public AsyncEventPublisher(final String topic, final HazelcastInstance hz, final ExecutorService executorService) {
        this.hz = hz;
        this.topic = topic;
        this.executorService = executorService;
    }

    /**
     * Publishes the provided payload to the NoMQ-system (asynchronously). The result of the operation is provided to the
     * callbacks (success or failure).
     *
     * @param type    The event type.
     * @param payload The payload that will published with the event.
     * @return The result of the publish operation, this result can later be used to chain success- and failure-handlers.
     */
    public PublishResult publishAsync(
            final String type,
            final byte[] payload) {

        return new CompletableFutureAsyncResult(doPublish(type, payload));
    }

    Event create(final String type, final byte[] payload) {
        return NoMQHelper.createEvent(NoMQHelper.generateUuid(), type, payload);
    }

    HazelcastInstance hazelcastInstance() {
        return hz;
    }

    /**
     * Protected since sync events can be sent this way.
     */
    Event publishAndWait(final Event event) {
        log.debug("Publish event [id={}]", event.id());
        NoMQHelper.sharedTopic(hazelcastInstance(), topic()).publish(event);
        return event;
    }

    String topic() {
        return topic;
    }

    private CompletableFuture<Event> doPublish(final String type, final byte[] payload) {
        return supplyAsync(() -> {
            Event event = create(type, payload);
            publishAndWait(event);
            return event;
        }, executorService);
    }
}
