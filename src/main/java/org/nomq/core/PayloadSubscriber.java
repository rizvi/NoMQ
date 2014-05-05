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
 * The payload subscriber is basically an {@link org.nomq.core.EventSubscriber} that uses a registered {@link
 * PayloadConverter} to convert the payload from a <code>byte[]</code> to the requested type.
 *
 * @author Tommy Wassgren
 */
@FunctionalInterface
public interface PayloadSubscriber<T> {
    /**
     * Invoked when the event has been received and converted to the requested type.
     *
     * @param payload The payload (converted by the provided converter).
     */
    void onPayload(T payload);
}
