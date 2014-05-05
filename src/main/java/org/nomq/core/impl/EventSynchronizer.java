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
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Synchronizes events during startup. When NoMQ starts a sync request is published and hopefylly some other NoMQ-node replies.
 * This component also listens to these requests and sends responses to the nodes that requests it.
 *
 * @author Tommy Wassgren
 */
class EventSynchronizer {
    private final AsyncEventPublisher eventPublisher;
    private final HazelcastInstance hz;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final int maxSyncAttempts;
    private final BlockingQueue<Event> playbackQueue;
    private final EventStore recordEventStore;
    private final long timeout;
    private final String topic;

    EventSynchronizer(final AsyncEventPublisher eventPublisher,
                      final BlockingQueue<Event> playbackQueue,
                      final String topic,
                      final HazelcastInstance hz,
                      final EventStore recordEventStore,
                      final long timeout,
                      final int maxSyncAttempts) {
        this.eventPublisher = eventPublisher;
        this.playbackQueue = playbackQueue;
        this.hz = hz;
        this.topic = topic;
        this.recordEventStore = recordEventStore;
        this.timeout = timeout;
        this.maxSyncAttempts = maxSyncAttempts;
    }

    /**
     * Starting point of the actual syncing. Attempt to sync from the cluster a few times and then sync the rest from the master
     * list.
     *
     * @return A set containing all processed keys.
     */
    Set<String> sync() {
        // Sync from the cluster
        final Set<String> processedIds = syncFromCluster(maxSyncAttempts);

        // Recordings are now all synced up, NoMQ is now ready for responding to sync requests as well so add the sync-request
        // listener
        startRespondingToSyncRequests();

        // return the already "processed" ids
        return processedIds;
    }

    private boolean doesClusterContainEnoughMembersForSync() {
        final Set<Member> members = hz.getCluster().getMembers();
        return members.size() > 1;
    }

    private Event generateSyncEvent() {
        return NoMQHelper.createEvent(NoMQHelper.generateSyncRequestId(recordEventStore), "__sync_request", null);
    }

    private boolean isSyncRequestAlreadyHandled(final String syncId) {
        final IMap<String, String> statusMap = hz.getMap(requestMapName());
        final String previous = statusMap.putIfAbsent(syncId, hz.getLocalEndpoint().getUuid());
        return previous != null;
    }

    /**
     * Polls the response queue and processes the received events. If no events are received this operation times out.
     */
    private boolean pollAndProcess(final Set<String> processedKeys, final String syncRequestId, final IQueue<Event> responseQueue)
            throws InterruptedException, SyncFailureException {

        final Event event = responseQueue.poll(timeout, TimeUnit.MILLISECONDS);
        if (event != null) {
            if (event.id().equals(syncRequestId)) {
                // Sync completed
                return true;
            } else {
                processEvent(event, processedKeys);
            }
        } else {
            // No response - unable to sync
            throw new SyncFailureException("Unable to sync, no response from queue");
        }
        return false;
    }

    private void processEvent(final Event event, final Set<String> processedKeys) {
        log.debug("Recording event [id={}] - sync", event.id());
        processedKeys.add(event.id());
        recordEventStore.append(event);

        if (!NoMQHelper.isSyncRequest(event)) {
            playbackQueue.add(event);
        }
    }

    private void publishSyncRequest(final String syncRequestId) {
        final ITopic<String> syncRequests = hz.getTopic(requestTopicName());

        // Publish the resend request on the command queue
        syncRequests.publish(syncRequestId);
    }

    private boolean replayAll(final String str) {
        return NoMQHelper.all().equals(str);
    }

    private String replayFromId(final Message<String> message) {
        return message.getMessageObject().split(":")[1];
    }

    private Stream<Event> replayStream(final String replayFromId) {
        final Stream<Event> replay;
        if (replayAll(replayFromId)) {
            replay = recordEventStore.replayAll();
        } else {
            replay = recordEventStore.replay(replayFromId);
        }
        return replay;
    }

    private String requestMapName() {
        return topic + "-status";
    }

    private ITopic<String> requestTopic() {
        return hz.getTopic(requestTopicName());
    }

    private String requestTopicName() {
        return topic + "-requests";
    }

    private void sendSyncResponse(final String syncId, final String replayFromId) {
        final IQueue<Event> queue = hz.getQueue(syncId);

        replayStream(replayFromId).forEach(event -> {
            try {
                queue.put(event);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Unable to respond to sync request", e);
            }
        });
    }

    private void startRespondingToSyncRequests() {
        requestTopic().addMessageListener(request -> {
            log.info("Handling resend request [request={}]", request.getMessageObject());
            final String syncRequestId = request.getMessageObject();
            NoMQHelper.lockTemplate(hz, syncRequestId, timeout).tryLock(() -> {
                final String replayFromId = replayFromId(request);
                if (isSyncRequestAlreadyHandled(syncRequestId)) {
                    log.info("Skipping resend request [request={}]", request.getMessageObject());
                } else {
                    sendSyncResponse(syncRequestId, replayFromId);
                }
            });
        });
    }

    private Set<String> syncFromCluster() throws SyncFailureException {
        final Set<String> processedKeys = new HashSet<>();

        // Publish a "sync" event on the real event queue. This id will be used later as a check to see that NoMQ has
        // been completely synced.
        final String syncRequestId = eventPublisher
                .publishAndWait(generateSyncEvent())
                .id();

        // Publish the sync request
        publishSyncRequest(syncRequestId);

        // Then, wait for the sync-response (an exception will be thrown if sync fails)
        waitForSyncResponse(processedKeys, syncRequestId);

        return processedKeys;
    }

    /**
     * Attempts to sync the events from the cluster repeatedly.
     */
    private Set<String> syncFromCluster(final int nrOfAttempts) {
        if (doesClusterContainEnoughMembersForSync()) {
            for (int i = 0; i < nrOfAttempts; i++) {
                try {
                    return syncFromCluster();
                } catch (final SyncFailureException e) {
                    // Sync failed, try again...
                    log.error("Failed to sync [attempt={}/{}]", i + 1, nrOfAttempts, e);
                }
            }
        }
        return Collections.emptySet();
    }

    private void waitForSyncResponse(final Set<String> processedKeys, final String syncRequestId) throws SyncFailureException {
        // Wait/handle the response
        try {
            // The response queue uses the same name as the entire sync-operation
            final IQueue<Event> responseQueue = hz.getQueue(syncRequestId);

            while (true) {
                if (pollAndProcess(processedKeys, syncRequestId, responseQueue)) {
                    break;
                }
            }
        } catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
