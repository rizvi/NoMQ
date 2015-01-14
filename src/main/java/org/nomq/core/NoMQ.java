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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The entry point for working with NoMQ. The NoMQ-instance is created via the class {@link NoMQBuilder} and this instance is
 * used for lifecycle management and publishing of events. Subscribers register during creation - also via the {@link
 * NoMQBuilder} class.
 *
 * @author Tommy Wassgren
 * @see #publishAsync(String, byte[])
 */
public interface NoMQ extends Startable<NoMQ>, Stoppable {

    /**
     * Publishes the payload to the NoMQ-system (asynchronously). This is the same as using the {@link #publishAsync(String,
     * byte[], Consumer, Consumer)}-method with empty callbacks.
     *
     * @param type    The event type.
     * @param payload The payload to publish.
     */
    void publishAsync(final String type, final byte[] payload);

    /**
     * A non-blocking method for publishing a payload object of any type without any callbacks. This is pretty much the same as
     * using the standard {@link #publishAsync(String, byte[], Consumer, Consumer)}-method with noop-callbacks.
     *
     * The object to be published can be of any type and it will be converted to a byte array via the provided converter.
     *
     * @param type          The event type.
     * @param payloadObject The payload object to publish.
     * @param converter     The converter (converts from type T to type byte[]).
     */
    <T> void publishAsync(final String type, T payloadObject, Function<T, byte[]> converter);

    /**
     * A non-blocking method for publishing a payload object of any type. The payload object will be converted to a byte array
     * via the provided converter. The result of the operation is returned via callbacks (success or failure).
     *
     * @param type            The event type.
     * @param payloadObject   The payload object to publish.
     * @param converter       The converter responsible for converting the payload object to a byte array.
     * @param successCallback The success callback, invoked when publishing has completed
     * @param errorCallback   The failure callback, invoked if publishing fails
     */
    <T> void publishAsync(
            final String type,
            T payloadObject,
            Function<T, byte[]> converter,
            Consumer<Event> successCallback,
            Consumer<Throwable> errorCallback);


    /**
     * A non-blocking method for publishing a payload object of any type. The result of the operation is returned via callbacks
     * (success or failure).
     *
     * @param type            The event type.
     * @param payload         The payload to publish.
     * @param successCallback The success callback, invoked when publishing has completed
     * @param errorCallback   The failure callback, invoked if publishing fails
     */
    void publishAsync(
            final String type,
            byte[] payload,
            Consumer<Event> successCallback,
            Consumer<Throwable> errorCallback);


}
