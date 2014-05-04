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
     * Publishes the provided payload to the NoMQ-system (synchronously).
     *
     * @param payload The payload that will published with the event.
     * @return The published event
     */
    Event publish(byte[] payload);

    /**
     * Non-blocking version of the {@link #publish(byte[])}-method. The result of the operation is provided to the callback.
     *
     * @param payload            The payload that will published with the event.
     * @param publisherCallback  The callback that will be invoked when the publish has completed.
     * @param exceptionCallbacks Invoked when an exception occurs.
     */
    void publish(byte[] payload, EventPublisherCallback publisherCallback, ExceptionCallback... exceptionCallbacks);
}