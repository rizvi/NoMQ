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
 * Events can be published by event publishers and received by event subscribers. Events contains a generated id, the payload
 * which is basically a byte array and an type. The type is used to semantically differentiate events from each other and to
 * find the correct subscribers.
 *
 * @author Tommy Wassgren
 */
public interface Event {
    /**
     * A generated unique id (UUID) to identify the event.
     *
     * @return The unique id of the event.
     */
    String id();

    /**
     * This returns the payload of the event - the Event class can only carry byte[] but conversions to and from other types is
     * possible via the {@link PayloadConverter}-interface.
     *
     * @return The payload of the event.
     */
    byte[] payload();

    /**
     * The event type is used to semantically describe an event. Furthermore the type is used to route the event to the correct
     * subscriber.
     *
     * @return The event type.
     */
    String type();
}
