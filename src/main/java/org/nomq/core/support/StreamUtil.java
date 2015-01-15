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

package org.nomq.core.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * Contains utility functions that are typically used with the EventStore-classes.
 *
 * @author Tommy Wassgren
 */
public class StreamUtil {
    private static final Logger logger = LoggerFactory.getLogger(StreamUtil.class);

    public static void close(final AutoCloseable... closeables) {
        requireNonNull(closeables, "Closeables must not be null");
        Arrays.stream(closeables).forEach(StreamUtil::closeWithRuntimeException);
    }

    public static void closeSilently(final AutoCloseable... closeables) {
        requireNonNull(closeables, "Closeables must not be null");
        Arrays.stream(closeables).forEach(StreamUtil::closeSilentlyAndLog);
    }

    /**
     * Creates a stream based on the provided iterator.
     */
    public static <T> Stream<T> createStream(final Iterator<T> iterator) {
        // TODO: is this the way to go?
        requireNonNull(iterator, "Iterator must not be null");
        return StreamSupport.stream(Spliterators.spliterator(iterator, 0L, 0), false);
    }

    /**
     * @see #delegate(Iterator, Function, Consumer)
     */
    public static <I extends Iterator<T>, T, R> Iterator<R> delegate(final I delegate, final Function<T, R> conversion) {
        return delegate(
                delegate,
                conversion,
                (itr) -> {
                    // No closing op
                });
    }

    /**
     * Creates a delegating iterator with some nice extra features. For each call to next a converter function may be provided
     * (the simplest version is something along the lines of (arg) -> arg. The last invocation (when {@code Iterator#hasNext}
     * returns false) a close handler can be invoked (where the simplest version is (itr)->{}).
     *
     * @param delegate     The original iterator
     * @param conversion   A conversion handler, called for each call to {@code Iterator#next}
     * @param closeHandler A handler that is invoked for the last element, useful for closing underlying resources.
     * @return The delegating iterator.
     */
    public static <I extends Iterator<T>, T, R> Iterator<R> delegate(final I delegate, final Function<T, R> conversion, final Consumer<I> closeHandler) {
        requireNonNull(delegate, "Iterator must not be null");
        requireNonNull(conversion, "Close-method must not be null");

        return new Iterator<R>() {
            @Override
            public boolean hasNext() {
                final boolean hasNext = delegate.hasNext();
                if (!hasNext) {
                    closeHandler.accept(delegate);
                }

                return hasNext;
            }

            @Override
            public R next() {
                return conversion.apply(delegate.next());
            }
        };
    }

    /**
     * Close the provided
     *
     * @param closeable
     */
    private static void closeSilentlyAndLog(final AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (final Exception e) {
            logger.error("Unable to close resource", e);
        }
    }

    /**
     * Close the provided
     *
     * @param closeable
     */
    private static void closeWithRuntimeException(final AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to close resource");
        }
    }
}
