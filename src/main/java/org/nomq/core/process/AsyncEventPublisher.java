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

import com.hazelcast.core.HazelcastInstance;
import org.nomq.core.Event;
import org.nomq.core.EventPublisher;
import org.nomq.core.EventPublisherCallback;
import org.nomq.core.ExceptionCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.Arrays.stream;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.nomq.core.process.NoMQHelper.createEvent;
import static org.nomq.core.process.NoMQHelper.generateUuid;
import static org.nomq.core.process.NoMQHelper.sharedTopic;

/**
 * The implementation of the event publisher, this publisher uses {@link CompletableFuture} for async requests and also provides
 * a blocking version of the publish-method since this is used during sync.
 *
 * @author Tommy Wassgren
 */
public class AsyncEventPublisher implements EventPublisher {
    private static final ExceptionCallback[] EMPTY_EXCEPTION_CALLBACK = new ExceptionCallback[0];
    private final ExecutorService executorService;
    private final HazelcastInstance hz;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String topic;

    public AsyncEventPublisher(final String topic, final HazelcastInstance hz, final ExecutorService executorService) {
        this.hz = hz;
        this.topic = topic;
        this.executorService = executorService;
    }

    @Override
    public void publishAsync(
            final byte[] payload,
            final EventPublisherCallback publisherCallback,
            final ExceptionCallback... exceptionCallbacks) {

        doPublish(payload)
                .handleAsync((event, exception) -> {
                    if (event != null) {
                        publisherCallback.eventPublished(event);
                    } else {
                        notifyExceptionHandlers(exception, exceptionCallbacks);
                    }
                    return event;
                }, executorService);
    }

    protected Event create(final byte[] payload) {
        return createEvent(generateUuid(), payload);
    }

    protected HazelcastInstance hazelcastInstance() {
        return hz;
    }

    protected ExceptionCallback[] notNull(final ExceptionCallback... exceptionCallbacks) {
        return exceptionCallbacks == null ? EMPTY_EXCEPTION_CALLBACK : exceptionCallbacks;
    }

    protected void notifyExceptionHandlers(final Throwable thr, final ExceptionCallback... exceptionCallbacks) {
        stream(notNull(exceptionCallbacks)).forEach(callback -> callback.onException(thr));
    }

    /**
     * Protected since sync events can be sent this way.
     */
    protected Event publishAndWait(final Event event) {
        log.debug("Publish event [id={}]", event.id());
        sharedTopic(hazelcastInstance(), topic()).publish(event);
        return event;
    }

    protected String topic() {
        return topic;
    }

    private CompletableFuture<Event> doPublish(final byte[] payload) {
        return supplyAsync(() -> {
            Event event = create(payload);
            publishAndWait(event);
            return event;
        }, executorService);
    }
}
