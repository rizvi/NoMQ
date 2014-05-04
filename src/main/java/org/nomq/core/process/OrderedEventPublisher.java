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
import org.nomq.core.lifecycle.Startable;
import org.nomq.core.lifecycle.Stoppable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.runAsync;

/**
 * The event publisher, it simply adds messages to the Hazelcast topic.
 *
 * @author Tommy Wassgren
 */
public class OrderedEventPublisher extends EventPublisherSupport implements Startable<EventPublisher>, Stoppable {
    private static class EventPublishResult {
        private Event event;
        private Throwable throwable;

        public void failure(final Throwable throwable) {
            this.throwable = throwable;
        }

        public Event returnOrThrow() {
            if (throwable != null) {
                throw new IllegalStateException(throwable);
            }
            return event;
        }

        public void success(final Event event) {
            this.event = event;
        }
    }

    private static class EventWithContext {
        private final Event event;
        private final ExceptionCallback[] exceptionCallbacks;
        private final EventPublisherCallback publisherCallback;

        private EventWithContext(final Event event, final EventPublisherCallback publisherCallback, final ExceptionCallback[] exceptionCallbacks) {
            this.event = event;
            this.publisherCallback = publisherCallback;
            this.exceptionCallbacks = exceptionCallbacks;
        }

        Event event() {
            return event;
        }

        ExceptionCallback[] exceptionCallbacks() {
            return exceptionCallbacks;
        }

        EventPublisherCallback publisherCallback() {
            return publisherCallback;
        }
    }

    private final ExecutorService executorService;
    private final BlockingQueue<EventWithContext> outbound = new LinkedBlockingQueue<>();
    private Future<?> runner;

    public OrderedEventPublisher(final String topic, final HazelcastInstance hz, final ExecutorService executorService) {
        super(topic, hz);
        this.executorService = executorService;
    }

    @Override
    public Event publish(final byte[] payload) {
        final CountDownLatch latch = new CountDownLatch(1);
        final EventPublishResult result = new EventPublishResult();
        enqueue(payload,
                event -> {
                    result.success(event);
                    latch.countDown();

                },
                new ExceptionCallback[]{
                        throwable ->
                        {
                            result.failure(throwable);
                            latch.countDown();
                        }
                }
        );

        try {
            latch.await();
            return result.returnOrThrow();
        } catch (final InterruptedException e) {
            throw new IllegalStateException("Publishing interrupted", e);
        }
    }

    @Override
    public void publish(
            final byte[] payload,
            final EventPublisherCallback publisherCallback,
            final ExceptionCallback... exceptionCallbacks) {

        enqueue(payload, publisherCallback, exceptionCallbacks);
    }

    @Override
    public EventPublisher start() {
        runner = executorService.submit((Runnable) () -> {
            while (true) {
                try {
                    final EventWithContext eventToPublish = outbound.poll(1000, TimeUnit.MILLISECONDS);

                    try {
                        final Event publishedEvent = publish(eventToPublish.event());
                        final EventPublisherCallback publisherCallback = eventToPublish.publisherCallback();
                        if (publisherCallback != null) {
                            runAsync(() -> publisherCallback.eventPublished(publishedEvent));
                        }
                    } catch (Throwable throwable) {
                        runAsync(() -> notifyExceptionHandlers(throwable, eventToPublish.exceptionCallbacks()));
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        return this;
    }

    @Override
    public void stop() {
        if (runner != null) {
            runner.cancel(true);
        }
    }

    private void enqueue(
            final byte[] payload,
            final EventPublisherCallback publisherCallback,
            final ExceptionCallback[] exceptionCallbacks) {

        outbound.add(new EventWithContext(create(payload), publisherCallback, exceptionCallbacks));
    }
}

