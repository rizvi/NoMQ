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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.nomq.core.process.NoMQHelper.createEvent;
import static org.nomq.core.process.NoMQHelper.generateUuid;
import static org.nomq.core.process.NoMQHelper.sharedTopic;

/**
 * The event publisher, it simply adds messages to the Hazelcast topic.
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

    /**
     * Package protected since sync events can be sent this way.
     */
    String publish(final Event event) {
        log.debug("Publish event [id={}]", event.id());
        sharedTopic(hz, topic).publish(event);
        return event.id();
    }

    private Event create(final byte[] payload) {
        return createEvent(generateUuid(), payload);
    }
}

