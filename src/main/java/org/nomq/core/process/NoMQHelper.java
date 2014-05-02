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
import org.nomq.core.EventStore;

import java.util.Optional;
import java.util.UUID;

/**
 * Various helper functions.
 *
 * @author Tommy Wassgren
 */
class NoMQHelper {
    private static final String SYNC_REQUEST_SEPARATOR = ":";

    static String all() {
        return "all";
    }

    static Event createEvent(final String id, final byte[] payload) {
        return new SerializableEvent(id, payload);
    }

    static Event createEvent(final String id) {
        return createEvent(id, null);
    }

    static String generateSyncRequestId(final EventStore recordEventStore) {
        // Find the id of the latest processed event
        final Optional<Event> latestProcessedEvent = recordEventStore.latest();
        final String latestProcessedId = latestProcessedEvent.isPresent() ? latestProcessedEvent.get().id() : all();

        return generateUuid() + SYNC_REQUEST_SEPARATOR + latestProcessedId;
    }

    static String generateUuid() {
        return UUID.randomUUID().toString();
    }

    static boolean isSyncRequest(final Event event) {
        return event.id().contains(SYNC_REQUEST_SEPARATOR);
    }

    static LockTemplate lockTemplate(final HazelcastInstance hz, final String lockName, final long timeout) {
        return new LockTemplate(hz.getLock(lockName), timeout);
    }

    static IList<Event> sharedTopic(final HazelcastInstance hz, final String topic) {
        return hz.getList(topic);
    }

    static LockTemplate topicLock(final HazelcastInstance hz, final String topic, final long timeout) {
        return lockTemplate(hz, topic + "-topicLock", timeout);
    }
}
