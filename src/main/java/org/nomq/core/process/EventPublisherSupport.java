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
import org.nomq.core.ExceptionCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.stream;
import static org.nomq.core.process.NoMQHelper.createEvent;
import static org.nomq.core.process.NoMQHelper.generateUuid;
import static org.nomq.core.process.NoMQHelper.sharedTopic;

/**
 * @author Tommy Wassgren
 */
public abstract class EventPublisherSupport implements EventPublisher {
    private static final ExceptionCallback[] EMPTY_EXCEPTION_CALLBACK = new ExceptionCallback[0];
    private final HazelcastInstance hz;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String topic;

    protected EventPublisherSupport(final String topic, final HazelcastInstance hz) {
        this.hz = hz;
        this.topic = topic;
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

    protected String topic() {
        return topic;
    }

    /**
     * Package protected since sync events can be sent this way.
     */
    Event publish(final Event event) {
        log.debug("Publish event [id={}]", event.id());
        sharedTopic(hazelcastInstance(), topic()).publish(event);
        return event;
    }
}
