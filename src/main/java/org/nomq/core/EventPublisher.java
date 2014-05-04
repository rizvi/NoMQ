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

package org.nomq.core;

/**
 * Publish events to the NoMQ-system.
 *
 * @author Tommy Wassgren
 */
public interface EventPublisher {
    /**
     * Publishes the provided payload to the NoMQ-system (asynchronously). This method is non blocking, to publish events in
     * blocking way use the EventPublisherTemplate. The result of the operation is provided to the callbacks (success or
     * failure).
     *
     * @param payload            The payload that will published with the event.
     * @param publisherCallback  Success - the callback that will be invoked when the publish has completed.
     * @param exceptionCallbacks Failure - invoked when an exception occurs.
     */
    void publishAsync(byte[] payload, EventPublisherCallback publisherCallback, ExceptionCallback... exceptionCallbacks);
}
