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

package org.nomq.store.journalio;

import org.nomq.core.Event;

/**
 * A basic interface for serialization and deserialization of Objects to byte arrays (binary data).
 *
 * @author Tommy Wassgren
 */
public interface EventSerializer {
    /**
     * Deserialize an object from the given binary data.
     *
     * @param data object binary representation
     * @return the equivalent object instance
     */
    Event deserialize(byte[] data);

    /**
     * Serialize the given object to binary data.
     *
     * @param event object to serialize
     * @return the equivalent binary data
     */
    byte[] serialize(Event event);
}

