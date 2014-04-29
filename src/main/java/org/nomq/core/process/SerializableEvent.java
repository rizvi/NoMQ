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

import java.io.Serializable;

/**
 * @author Tommy Wassgren
 */
public class SerializableEvent implements Event, Serializable {
    private static final long serialVersionUID = 1L;
    private static final byte[] EMPTY = new byte[0];
    private final String id;
    private final byte[] payload;

    public SerializableEvent(final String id, final byte[] payload) {
        this.id = id;
        this.payload = payload == null ? EMPTY : payload;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public byte[] payload() {
        return payload;
    }
}
