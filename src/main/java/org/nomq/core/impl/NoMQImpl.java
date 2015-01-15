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
import org.nomq.core.EventStore;
import org.nomq.core.NoMQ;
import org.nomq.core.PublishResult;
import org.nomq.core.Startable;
import org.nomq.core.support.StreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * The implementation of the NoMQ system that "connects" all the various components. This instance should NOT be instantiated
 * directly but rather via the NoMQBuilder class.
 *
 * @author Tommy Wassgren
 */
public class NoMQImpl implements NoMQ {
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
    public void close() {
        close(publisher);
        close(recorder);
        close(player);
        close(recordEventStore);
        close(playbackEventStore);
        hz.getLifecycleService().shutdown();
    }

    @Override
    public <T> PublishResult publish(final String type, final T payloadObject, final Function<T, byte[]> converter) {
        return publish(type, converter.apply(payloadObject));
    }

    @Override
    public PublishResult publish(final String type, final byte[] payload) {
        return publisher.publishAsync(type, payload);
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

    private void close(final Object closeable) {
        if (closeable instanceof AutoCloseable) {
            StreamUtil.closeSilently((AutoCloseable) closeable);
        }
    }

    private void start(final Object startable) {
        if (startable instanceof Startable) {
            ((Startable) startable).start();
        }
    }
}
