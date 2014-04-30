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
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
    private final HazelcastInstance hz;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final int maxSyncAttempts;
    private final BlockingQueue<Event> playbackQueue;
    private final EventStore recordEventStore;
    private final long timeout;
    private final String topic;

    EventSynchronizer(final BlockingQueue<Event> playbackQueue,
                      final String topic,
                      final HazelcastInstance hz,
                      final EventStore recordEventStore,
                      final long timeout,
                      final int maxSyncAttempts) {
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
        // Do the actual sync. First sync from the cluster
        final Set<String> processedClusterIds = syncFromCluster(maxSyncAttempts);

        // Then sync from the master list
        final Set<String> processedMasterIds = syncFromMasterList();

        // Recordings are now all synced up, NoMQ is now ready for responding to sync requests as well so add the sync-request
        // listener
        handleSyncRequests();

        // return the already "processed" ids
        return merge(processedClusterIds, processedMasterIds);
    }

    private String createSyncRequest(final String syncId) {
        // Find the id of the latest processed event
        final Optional<Event> latestProcessedEvent = recordEventStore.latest();
        final String latestProcessedId = latestProcessedEvent.isPresent() ? latestProcessedEvent.get().id() : "all";

        return syncId + ":" + latestProcessedId;
    }

    private boolean doesClusterContainEnoughMembersForSync() {
        final Set<Member> members = hz.getCluster().getMembers();
        return members.size() > 1;
    }

    private String generateSyncId() {
        return UUID.randomUUID().toString();
    }

    private void handleSyncRequests() {
        requestTopic().addMessageListener(request -> {
            log.info("Handling resend request [request={}]", request.getMessageObject());
            final String syncId = syncId(request);
            final ILock lock = hz.getLock(syncId);
            try {
                lock.tryLock(timeout, TimeUnit.MILLISECONDS);
                final String replayFromId = replayFromId(request);

                if (isSyncRequestAlreadyHandled(syncId)) {
                    log.info("Skipping resend request [request={}]", request.getMessageObject());
                } else {
                    sendSyncResponse(syncId, replayFromId);
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException("Unable to respond to sync request", e);
            } finally {
                lock.unlock();
            }
        });

    }


    private boolean isSyncRequestAlreadyHandled(final String syncId) {
        final IMap<String, String> statusMap = hz.getMap(requestMapName());
        final String previous = statusMap.putIfAbsent(syncId, hz.getLocalEndpoint().getUuid());
        return previous != null;
    }

    private Set<String> merge(final Set<String> processedClusterIds, final Set<String> processedMasterIds) {
        final Set<String> processedIds = new HashSet<>();
        processedIds.addAll(processedClusterIds);
        processedIds.addAll(processedMasterIds);
        return processedIds;
    }

    /**
     * Polls the response queue and processes the received events. If no events are received this operation times out.
     */
    private boolean pollAndProcess(final Set<String> processedKeys, final String syncId, final IQueue<Event> responseQueue) throws InterruptedException, SyncFailureException {
        final Event event = responseQueue.poll(timeout, TimeUnit.MILLISECONDS);
        if (event != null) {
            if (event.id().equals(syncId)) {
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
        playbackQueue.add(event);
    }

    private void publishSyncRequest(final String syncId) {
        final ITopic<String> syncRequests = hz.getTopic(requestTopicName());

        // Publish the resend request on the command queue
        syncRequests.publish(createSyncRequest(syncId));
    }

    private boolean replayAll(final String str) {
        return "all".equals(str);
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

        // Finally - send an event with the "syncId" to indicate that the resend has completed.
        queue.add(new SerializableEvent(syncId, "".getBytes()));
    }

    private Set<String> syncFromCluster() throws SyncFailureException {
        final Set<String> processedKeys = new HashSet<>();

        if (doesClusterContainEnoughMembersForSync()) {
            // First, generate a sync id to use for this sync-session
            final String syncId = generateSyncId();

            // Publish the sync request
            publishSyncRequest(syncId);

            // Then, wait for the sync-response
            waitForSyncResponse(processedKeys, syncId);
        }

        return processedKeys;
    }

    /**
     * Attempts to sync the events from the cluster repeatedly.
     */
    private Set<String> syncFromCluster(final int nrOfAttempts) {
        final Set<String> allProcessedKeys = new HashSet<>();

        for (int i = 0; i < nrOfAttempts; i++) {
            try {
                final Set<String> processedKeys = syncFromCluster();
                allProcessedKeys.addAll(processedKeys);
            } catch (final SyncFailureException e) {
                // Sync failed, try again...
                log.error("Failed to sync [attempt={}/{}]", i + 1, nrOfAttempts, e);
            }
        }
        return allProcessedKeys;
    }

    private Set<String> syncFromMasterList() {
        final RecordEventProcessingStatus status = new RecordEventProcessingStatus(recordEventStore.latest());

        // Then, process all the messages and store a key-ref for later use
        final IList<Event> coll = hz.getList(topic);
        final Set<String> processedKeys = new HashSet<>();
        coll.forEach(event -> {
            if (status.shouldProcess(event.id())) {
                processEvent(event, processedKeys);
            }
        });

        return processedKeys;
    }

    private String syncId(final Message<String> message) {
        return message.getMessageObject().split(":")[0];
    }

    private void waitForSyncResponse(final Set<String> processedKeys, final String syncId) throws SyncFailureException {
        // Wait/handle the response
        try {
            // The response queue uses the same name as the entire sync-operation
            final IQueue<Event> responseQueue = hz.getQueue(syncId);

            while (true) {
                if (pollAndProcess(processedKeys, syncId, responseQueue)) {
                    break;
                }
            }
        } catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
