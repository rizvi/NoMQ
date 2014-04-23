package org.nomq.core.process;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.nomq.core.Event;
import org.nomq.core.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class HazelcastEventPublisher implements EventPublisher {
    private final HazelcastInstance hazelcastInstance;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String topic;

    public HazelcastEventPublisher(final String topic, final HazelcastInstance hazelcastInstance) {
        this.topic = topic;
        this.hazelcastInstance = hazelcastInstance;
    }


    @Override
    public void publish(final byte[] payload) {
        final Event event = create(payload);
        log.debug("Publish event [id={}]", event.id());
        final IList<Event> q = hazelcastInstance.getList(topic);
        q.add(event);
    }

    private Event create(final byte[] payload) {
        return new SerializableEvent(newId(), payload);
    }

    private String newId() {
        return UUID.randomUUID().toString();
    }
}

