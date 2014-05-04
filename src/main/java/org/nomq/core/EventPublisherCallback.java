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
 * When an event is published async this callback is used to capture when the event has actually been published.
 *
 * @author Tommy Wassgren
 */
@FunctionalInterface
public interface EventPublisherCallback {
    /**
     * Invoked when an event has been successfully published.
     *
     * @param event The published event.
     */
    void eventPublished(Event event);
}
