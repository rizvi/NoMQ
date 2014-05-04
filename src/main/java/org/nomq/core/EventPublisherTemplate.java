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

import java.util.concurrent.CountDownLatch;

/**
 * @author Tommy Wassgren
 */
public class EventPublisherTemplate {
    @FunctionalInterface
    public interface Converter<I, O> {
        O convert(I input);
    }

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

    private final EventPublisher eventPublisher;

    public EventPublisherTemplate(final EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public Event publishAndWait(final byte[] payload) {
        final CountDownLatch latch = new CountDownLatch(1);
        final EventPublishResult result = new EventPublishResult();

        publishAsync(
                payload,
                t -> t,
                event -> {
                    result.success(event);
                    latch.countDown();

                },
                throwable ->
                {
                    result.failure(throwable);
                    latch.countDown();
                }
        );
        try {
            latch.await();
            return result.returnOrThrow();
        } catch (final InterruptedException e) {
            throw new IllegalStateException("Publishing interrupted", e);
        }
    }

    public <T> Event publishAndWait(final T t, final Converter<T, byte[]> converter) {
        return publishAndWait(converter.convert(t));
    }

    public void publishAsync(final byte[] payload) {
        // No conversion (p->p), simply use the provided payload
        publishAsync(payload, p -> p);
    }

    public <T> void publishAsync(final T input, final Converter<T, byte[]> converter) {
        // Use the provided converter and use a noop callback
        publishAsync(input, converter, e -> {
        });
    }

    public <T> void publishAsync(
            final T payloadObject,
            final Converter<T, byte[]> converter,
            final EventPublisherCallback publisherCallback,
            final ExceptionCallback... exceptionCallbacks) {

        // Convert the payload and use the provided callbacks
        eventPublisher.publishAsync(converter.convert(payloadObject), publisherCallback, exceptionCallbacks);
    }
}
