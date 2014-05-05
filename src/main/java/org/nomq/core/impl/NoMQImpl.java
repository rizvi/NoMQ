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
import org.nomq.core.Converter;
import org.nomq.core.Event;
import org.nomq.core.EventPublisherCallback;
import org.nomq.core.EventStore;
import org.nomq.core.ExceptionCallback;
import org.nomq.core.NoMQ;
import org.nomq.core.Startable;
import org.nomq.core.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * The implementation of the NoMQ system that "connects" all the various components. This instance should NOT be instantiated
 * directly but rather via the NoMQBuilder class.
 *
 * @author Tommy Wassgren
 */
public class NoMQImpl implements NoMQ {
    private static class PublishResult {
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

    private final HazelcastInstance hz;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final EventStore playbackEventStore;
    private final EventPlayer player;
    private final AsyncEventPublisher publisher;
    private final EventStore recordEventStore;
    private final EventRecorder recorder;

    public NoMQImpl(
            final HazelcastInstance hz,
            final EventStore playbackEventStore,
            final EventStore recordEventStore,
            final EventRecorder recorder,
            final EventPlayer player,
            final AsyncEventPublisher publisher) {

        this.hz = hz;
        this.playbackEventStore = playbackEventStore;
        this.recordEventStore = recordEventStore;
        this.recorder = recorder;
        this.player = player;
        this.publisher = publisher;
    }

    @Override
    public Event publish(final String type, final byte[] payload) {
        final CountDownLatch latch = new CountDownLatch(1);
        final PublishResult result = new PublishResult();

        publishAsync(
                type,
                payload,
                (EventPublisherCallback) event -> {
                    result.success(event);
                    latch.countDown();

                },
                throwable -> {
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

    @Override
    public <T> Event publish(final String type, final T payloadObject, final Converter<T, byte[]> converter) {
        return publish(type, converter.convert(payloadObject));
    }

    @Override
    public void publishAsync(final String type, final byte[] payload) {
        // No conversion (p->p), simply use the provided payload
        publishAsync(type, payload, p -> p);
    }


    @Override
    public <T> void publishAsync(final String type, final T payloadObject, final Converter<T, byte[]> converter) {
        // Use the provided converter and use a noop callback
        publishAsync(type, payloadObject, converter, e -> { /* Do nothing */ });
    }

    @Override
    public <T> void publishAsync(
            final String type,
            final T payloadObject,
            final Converter<T, byte[]> converter,
            final EventPublisherCallback publisherCallback,
            final ExceptionCallback... exceptionCallbacks) {

        publishAsync(type, converter.convert(payloadObject), publisherCallback, exceptionCallbacks);
    }

    @Override
    public void publishAsync(final String type, final byte[] payload, final EventPublisherCallback publisherCallback, final ExceptionCallback... exceptionCallbacks) {
        publisher.publishAsync(type, payload, publisherCallback, exceptionCallbacks);
    }

    @Override
    public NoMQ start() {
        start(playbackEventStore);
        start(recordEventStore);
        start(player);
        start(recorder);
        start(publisher);

        log.debug("NoMQ started [nodeId={}]", hz.getLocalEndpoint().getUuid());
        return this;
    }

    @Override
    public void stop() {
        stop(publisher);
        stop(recorder);
        stop(player);
        stop(recordEventStore);
        stop(playbackEventStore);
        hz.getLifecycleService().shutdown();
    }

    private void start(final Object startable) {
        if (startable instanceof Startable) {
            ((Startable) startable).start();
        }
    }

    private void stop(final Object stoppable) {
        if (stoppable instanceof Stoppable) {
            try {
                ((Stoppable) stoppable).stop();
            } catch (final Throwable throwable) {
                log.error("Error while invoking stop", throwable);
            }
        }
    }
}
