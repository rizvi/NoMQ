package org.nomq.core.persistence;

import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import journal.io.api.Location;
import org.nomq.core.Event;
import org.nomq.core.lifecycle.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JournalEventStore implements EventStore, Stoppable {
    private final Journal journal;
    private final Logger log = LoggerFactory.getLogger(getClass());

    public JournalEventStore(final String path) {
        journal = createJournal(path);
    }

    @Override
    public void append(final Event event) {
        try {
            log.trace("Appending event [id={}]", event.id());
            journal.write(serialize(event), Journal.WriteType.SYNC);
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to append event to log", e);
        }
    }

    @Override
    public Optional<Event> latest() {
        try {
            final Iterator<Location> itr = journal.undo().iterator();
            if (itr.hasNext()) {
                return Optional.of(deserialize(itr.next().getData()));
            }
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to find latest event", e);
        }
        return Optional.empty();
    }

    @Override
    public Stream<Event> replay(final String latestProcessedId) {
        final Optional<Location> location = findLocation(latestProcessedId);
        return location.isPresent() ? createStream(location.get()) : emptyStream();
    }

    @Override
    public Stream<Event> replayAll() {
        return createStream(null);
    }

    public void stop() {
        try {
            log.debug("Closing event store");
            journal.close();
        } catch (final IOException e) {
            log.error("Unable to close event store", e);
        }
    }

    private Stream<Event> createEventStream(final Iterator<Event> eventIterator) {
        // TODO: is this the way to go?
        final Spliterator<Event> spliterator = Spliterators.spliterator(eventIterator, 0L, 0);
        return StreamSupport.stream(spliterator, false);
    }

    private Journal createJournal(final String path) {
        try {
            log.info("Creating journal event store [path={}]", path);

            // TODO: error handling for path errors etc
            return JournalBuilder.of(new File(path)).open();
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to create event store", e);
        }
    }

    private Stream<Event> createStream(final Location location) {
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
                    return deserialize(next.getData());
                }
            };

            return createEventStream(eventIterator);
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to create stream", e);
        }

    }

    private Event deserialize(final byte[] data) {
        // TODO: is this the way to go?
        try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (Event) is.readObject();
        } catch (final IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Unable to deserialize event", e);
        }
    }

    private Iterator<Event> emptyIterator() {
        final Collection<Event> events = Collections.emptyList();
        return events.iterator();
    }

    private Stream<Event> emptyStream() {
        return createEventStream(emptyIterator());
    }

    private Optional<Location> findLocation(final String id) {
        try {
            for (final Location location : journal.redo()) {
                final Event event = deserialize(location.getData());
                if (event.id().equals(id)) {
                    return Optional.of(location);
                }
            }
        } catch (final IOException e) {
            throw new IllegalStateException(String.format("Unable to find location [id=%s]", id));
        }
        return Optional.empty();
    }

    private byte[] serialize(final Event event) {
        // TODO: is this the way to go?
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
            os.writeObject(event);
            os.flush();
            return bos.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to serialize event", e);
        }
    }
}
