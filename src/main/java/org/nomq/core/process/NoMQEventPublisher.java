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
import com.hazelcast.core.IList;
import org.nomq.core.Event;
import org.nomq.core.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * The event publisher that simply adds messages to the Hazelcast queue.
 *
 * @author Tommy Wassgren
 */
public class NoMQEventPublisher implements EventPublisher {
    private final HazelcastInstance hz;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String topic;

    public NoMQEventPublisher(final String topic, final HazelcastInstance hz) {
        this.topic = topic;
        this.hz = hz;
    }

    @Override
    public String publish(final byte[] payload) {
        return publish(create(payload));
    }

    String publish(final Event event) {
        log.debug("Publish event [id={}]", event.id());
        final IList<Event> q = hz.getList(topic);
        q.add(event);
        return event.id();
    }

    private Event create(final byte[] payload) {
        return new SerializableEvent(newId(), payload);
    }

    private String newId() {
        return UUID.randomUUID().toString();
    }
}

