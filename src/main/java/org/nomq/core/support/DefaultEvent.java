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

package org.nomq.core.support;

import org.nomq.core.Event;

import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * This is the event implementation - it is serializable to work with both the event store and Hazelcast.
 *
 * @author Tommy Wassgren
 */
public class DefaultEvent implements Event, Serializable {
    private static final long serialVersionUID = 1L;
    private static final byte[] EMPTY = new byte[0];
    private final String id;
    private final byte[] payload;
    private final String type;

    public DefaultEvent(final String id, final String type, final byte[] payload) {
        this.id = requireNonNull(id, "Event id must not be null");
        this.type = requireNonNull(type, "Event type must not be null");
        this.payload = payload == null ? EMPTY : payload;
    }

    @Override
    public boolean equals(final Object otherObject) {
        if (otherObject instanceof DefaultEvent) {
            final DefaultEvent otherEvent = (DefaultEvent) otherObject;
            return Objects.equals(id, otherEvent.id) && Objects.equals(type, otherEvent.type);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public byte[] payload() {
        return payload;
    }

    @Override
    public String type() {
        return type;
    }
}
