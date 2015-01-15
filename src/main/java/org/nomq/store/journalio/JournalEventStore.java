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

import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import journal.io.api.Location;
import org.nomq.core.Event;
import org.nomq.core.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static org.nomq.core.support.StreamUtil.createStream;

/**
 * The default implementation of the event store. This version is backed by <a href="https://github.com/sbtourist/Journal.IO/">Journal.IO</a>
 * for persistent storage.
 *
 * @author Tommy Wassgren
 */
public class JournalEventStore implements EventStore, AutoCloseable {
    private final EventSerializer eventSerializer;
    private final Journal journal;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public JournalEventStore(final String path) {
        this(path, new ObjectStreamEventSerializer());
    }

    public JournalEventStore(final String path, final EventSerializer eventSerializer) {
        this.journal = createJournal(path);
        this.eventSerializer = eventSerializer;
    }

    @Override
    public void append(final Event event) {
        try {
            log.trace("Appending event [id={}]", event.id());
            journal.write(eventSerializer.serialize(event), Journal.WriteType.SYNC);
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to append event to log", e);
        }
    }

    @Override
    public void close() {
        try {
            log.debug("Closing event store");
            journal.close();
        } catch (final IOException e) {
            log.error("Unable to close event store", e);
        }
    }

    @Override
    public Optional<Event> latest() {
        try {
            final Iterator<Location> itr = journal.undo().iterator();
            if (itr.hasNext()) {
                return Optional.of(eventSerializer.deserialize(itr.next().getData()));
            }
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to find latest event", e);
        }
        return Optional.empty();
    }

    @Override
    public Stream<Event> replay(final String latestProcessedId) {
        final Optional<Location> location = findLocation(latestProcessedId);
        return location.isPresent() ? createStreamFromLocation(location.get()) : emptyStream();
    }

    @Override
    public Stream<Event> replayAll() {
        return createStreamFromLocation(null);
    }

    private Journal createJournal(final String path) {
        try {
            log.info("Creating journal event store [path={}]", path);
            return JournalBuilder.of(writeableFolder(path)).open();
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to create event store", e);
        }
    }

    private Stream<Event> createStreamFromLocation(final Location location) {
        try {
            final Iterator<Location> locationIterator;

            if (location != null) {
                locationIterator = journal.redo(location).iterator();

                // Skip the current step
                locationIterator.next();
            } else {
                locationIterator = journal.redo().iterator();
            }


            final Iterator<Event> eventIterator = new Iterator<Event>() {
                @Override
                public boolean hasNext() {
                    return locationIterator.hasNext();
                }

                @Override
                public Event next() {
                    final Location next = locationIterator.next();
                    return eventSerializer.deserialize(next.getData());
                }
            };

            return createStream(eventIterator);
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to create stream", e);
        }
    }

    private Iterator<Event> emptyIterator() {
        final Collection<Event> events = Collections.emptyList();
        return events.iterator();
    }

    private Stream<Event> emptyStream() {
        return createStream(emptyIterator());
    }

    private Optional<Location> findLocation(final String id) {
        try {
            for (final Location location : journal.redo()) {
                final Event event = eventSerializer.deserialize(location.getData());
                if (event.id().equals(id)) {
                    return Optional.of(location);
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(String.format("Unable to find location [id=%s]", id));
        }
        return Optional.empty();
    }

    private File writeableFolder(final String folder) {
        final File f = new File(folder);
        f.mkdirs();
        final Path path = f.toPath();

        if (!Files.isDirectory(path) || !Files.isWritable(path)) {
            throw new IllegalStateException(String.format("%s is an invalid path, make sure it is writeable", folder));
        }
        return f;
    }
}
