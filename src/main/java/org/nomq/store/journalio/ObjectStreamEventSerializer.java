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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author Tommy Wassgren
 */
class ObjectStreamEventSerializer implements EventSerializer {
    @Override
    public Event deserialize(final byte[] data) {
        try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(data))) {
            final Object obj = is.readObject();
            if (obj instanceof Event) {
                return Event.class.cast(obj);
            }
            throw new IllegalStateException("Deserialization failed, incorrect type "
                    + (obj != null ? obj.getClass() : "null"));
        } catch (final IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Unable to deserialize event", e);
        }
    }

    @Override
    public byte[] serialize(final Event event) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
             final ObjectOutputStream os = new ObjectOutputStream(bos)) {

            os.writeObject(event);
            os.flush();
            return bos.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to serialize event", e);
        }
    }
}
