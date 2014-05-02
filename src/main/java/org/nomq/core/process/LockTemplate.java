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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Simple template for working with locks to avoid boilerplate code.
 *
 * @author Tommy Wassgren
 */
public class LockTemplate {
    @FunctionalInterface
    public interface LockCallback {
        void execute();
    }

    private final Lock lock;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final long timeout;

    LockTemplate(final Lock lock, final long timeout) {
        this.lock = lock;
        this.timeout = timeout;
    }

    public void lock(final LockCallback callback) {
        log.debug("Obtaining lock");
        lock.lock();

        try {
            callback.execute();
        } catch (final Exception e) {
            throw new IllegalStateException("Error when executing in lock block", e);
        } finally {
            log.debug("Unlocking");
            lock.unlock();
        }
    }

    public void tryLock(final LockCallback callback) {
        try {
            log.debug("Obtaining lock");
            lock.tryLock(timeout, TimeUnit.MILLISECONDS);

            callback.execute();
        } catch (final InterruptedException e) {
            throw new IllegalStateException("Timeout when obtaining lock", e);
        } catch (final Exception e) {
            throw new IllegalStateException("Error when executing in lock block", e);
        } finally {
            log.debug("Unlocking");
            lock.unlock();
        }
    }

    private void doInLock(final LockCallback callback) {
        try {
            callback.execute();
        } catch (final Exception e) {
            throw new IllegalStateException("Error when executing in lock block", e);
        } finally {
            log.debug("Unlocking");
            lock.unlock();
        }
    }
}
