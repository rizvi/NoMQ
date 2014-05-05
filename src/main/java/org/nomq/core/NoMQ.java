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

import org.nomq.core.lifecycle.Startable;
import org.nomq.core.lifecycle.Stoppable;

/**
 * The entry point for working with the NoMQ-system. The NoMQ-instance is created via the class NoMQBuilder and this instance is
 * used for lifecycle management and publishing of events. Subscribers register during creation - also via the NoMQBuilder
 * class.
 *
 * @see #publishAsync(byte[])
 *
 * @author Tommy Wassgren
 */
public interface NoMQ extends Startable<NoMQ>, Stoppable {

    /**
     * Blocking version of the {@link #publishAsync(byte[])}-method. This methods blocks until the event has been published.
     *
     * @param payload The payload to publish
     * @return The published event
     */
    Event publish(final byte[] payload);

    /**
     * Blocking version of the {@link #publishAsync(Object, Converter)}-method. The method blocks until the event has been
     * published.
     *
     * The object to be published can be of any type, it will be converted to a byte array via the provided {@link Converter}.
     *
     * @param payloadObject The payload object.
     * @param converter     The converter to use (converts to a byte array).
     * @return The published event
     */
    <T> Event publish(final T payloadObject, final Converter<T, byte[]> converter);

    /**
     * Publishes the payload to the NoMQ-system (asynchronously). This is the same as using the {@link #publishAsync(byte[],
     * EventPublisherCallback, ExceptionCallback...)}-method with empty callbacks.
     *
     * @param payload The payload to publish.
     */
    void publishAsync(final byte[] payload);

    /**
     * A non-blocking method for publishing a payload object of any type without any callbacks. This is pretty much the same as
     * using the standard {@link #publishAsync(byte[], EventPublisherCallback, ExceptionCallback...)}-method with
     * noop-callbacks.
     *
     * The object to be published can be of any type and it will be converted to a byte array via the provided {@link
     * Converter}.
     *
     * @param payloadObject The payload object to publish.
     * @param converter     The converter.
     */
    <T> void publishAsync(T payloadObject, Converter<T, byte[]> converter);

    /**
     * A non-blocking method for publishing a payload object of any type. The payload object will be converted to a byte array
     * via the provided {@link Converter}. The result of the operation is returned via callbacks (success or failure).
     *
     * @param payloadObject      The payload object to publish.
     * @param converter          The converter responsible for converting the payload object to a byte array.
     * @param publisherCallback  The success callback, invoked when publishing has completed
     * @param exceptionCallbacks The failure callbacks, invoked if publishing fails
     */
    <T> void publishAsync(
            T payloadObject,
            Converter<T, byte[]> converter,
            EventPublisherCallback publisherCallback,
            ExceptionCallback... exceptionCallbacks);


    /**
     * A non-blocking method for publishing a payload object of any type. The payload object will be converted to a byte array
     * via the provided {@link Converter}. The result of the operation is returned via callbacks (success or failure).
     *
     * @param payload            The payload to publish.
     * @param publisherCallback  The success callback, invoked when publishing has completed
     * @param exceptionCallbacks The failure callbacks, invoked if publishing fails
     */
    <T> void publishAsync(
            byte[] payload,
            EventPublisherCallback publisherCallback,
            ExceptionCallback... exceptionCallbacks);
}
