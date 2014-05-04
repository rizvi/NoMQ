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
import org.nomq.core.EventPublisherCallback;
import org.nomq.core.ExceptionCallback;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * @author Tommy Wassgren
 */
public class AsyncEventPublisher extends EventPublisherSupport {
    private final ExecutorService executorService;

    public AsyncEventPublisher(final String topic, final HazelcastInstance hz, final ExecutorService executorService) {
        super(topic, hz);
        this.executorService = executorService;

    }

    @Override
    public Event publish(final byte[] payload) {
        final CompletableFuture<Event> future = doPublish(payload);
        try {
            return future.get();
        } catch (final InterruptedException e) {
            throw new IllegalStateException("Publishing interrupted", e);
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Unable to publish message", e.getCause());
        }
    }

    @Override
    public void publish(
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


    private CompletableFuture<Event> doPublish(final byte[] payload) {
        return supplyAsync(() -> create(payload), executorService).thenApplyAsync(this::publish, executorService);
    }
}