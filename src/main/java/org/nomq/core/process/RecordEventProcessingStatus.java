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

import org.nomq.core.Event;

import java.util.Optional;

/**
 * Keeps the "should process" state for events during sync. When the event recorder is initialized it typically starts with a
 * "latest processed" event. All events up to that point (including the latest processed event) should be ignored but all events
 * after that point should be handled.
 *
 * @author Tommy Wassgren
 */
class RecordEventProcessingStatus {
    private final String latestProcessedId;
    private boolean startEventProcessed = false;

    RecordEventProcessingStatus(final Optional<Event> event) {
        this.latestProcessedId = event.isPresent() ? event.get().id() : null;
    }

    boolean shouldProcess(final String id) {
        if (latestProcessedId == null) {
            return true;
        } else if (startEventProcessed) {
            return true;
        } else if (id.equals(latestProcessedId)) {
            startEventProcessed = true;
            return true;
        }
        return false;
    }
}
