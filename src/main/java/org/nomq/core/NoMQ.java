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

import java.util.function.Function;

/**
 * The entry point for working with NoMQ. The NoMQ-instance is created via the class {@link NoMQBuilder} and this instance is
 * used for lifecycle management and publishing of events. Subscribers register during creation - also via the {@link
 * NoMQBuilder} class.
 *
 * @author Tommy Wassgren
 * @see #publish(String, byte[])
 */
public interface NoMQ extends Startable<NoMQ>, Stoppable {

    /**
     * A non-blocking method for publishing a payload to the NoMQ-system.
     *
     * @param type    The event type.
     * @param payload The payload to publish.
     * @return This method is non-blocking, the returned result contains methods for registering both success- and
     * failure-callbacks.
     * @see #publish(String, Object, java.util.function.Function)
     */
    PublishResult publish(final String type, final byte[] payload);

    /**
     * A non-blocking method for publishing a payload object of any type to the NoMQ-system. This is pretty much the same as
     * using the standard {@link #publish(String, byte[])}-method but the difference is that a "payload converter" is part of
     * the signature. The converter is used to convert from the provided object of type T to a byte[].
     *
     * @param type          The event type.
     * @param payloadObject The payload object to publish.
     * @param converter     The converter (converts from type T to type byte[]).
     * @return This method is non-blocking, the returned result contains methods for registering both success- and
     * failure-callbacks.
     */
    <T> PublishResult publish(final String type, T payloadObject, Function<T, byte[]> converter);
}
