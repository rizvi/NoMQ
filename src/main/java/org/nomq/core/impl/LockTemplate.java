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

package org.nomq.core.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Internal abstraction/template for working with locks (to avoid boilerplate code).
 *
 * @author Tommy Wassgren
 */
class LockTemplate {
    private final Lock lock;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final long timeout;

    LockTemplate(final Lock lock, final long timeout) {
        this.lock = lock;
        this.timeout = timeout;
    }

    void lock(final Runnable callback) {
        log.debug("Obtaining lock");
        lock.lock();

        try {
            callback.run();
        } catch (final Exception e) {
            throw new IllegalStateException("Error when executing in lock block", e);
        } finally {
            log.debug("Unlocking");
            lock.unlock();
        }
    }

    void tryLock(final Runnable callback) {
        try {
            log.debug("Obtaining lock");
            lock.tryLock(timeout, TimeUnit.MILLISECONDS);

            callback.run();
        } catch (final InterruptedException e) {
            throw new IllegalStateException("Timeout when obtaining lock", e);
        } catch (final Exception e) {
            throw new IllegalStateException("Error when executing in lock block", e);
        } finally {
            log.debug("Unlocking");
            lock.unlock();
        }
    }
}
