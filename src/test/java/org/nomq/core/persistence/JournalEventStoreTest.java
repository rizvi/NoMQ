package org.nomq.core.persistence;

import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.transport.SerializableEvent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JournalEventStoreTest {
    @Test
    public void appendCloseAndAppend() throws IOException {
        // Given
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");

        // When
        JournalEventStore eventStore = new JournalEventStore(tempDirectory.toString());
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));
        eventStore.stop();

        eventStore = new JournalEventStore(tempDirectory.toString());
        eventStore.append(createEvent("3"));

        // Then
        final long count = eventStore.replay("2").count();
        assertEquals(1L, count);

        // Cleanup
        eventStore.stop();
    }

    @Test
    public void emptyStoreShouldReturnEmptyLatest() throws IOException {
        // Given
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");

        // When
        final JournalEventStore eventStore = new JournalEventStore(tempDirectory.toString());

        // Then
        final Optional<Event> latest = eventStore.latest();
        assertFalse(latest.isPresent());

        // Cleanup
        eventStore.stop();
    }

    @Test
    public void emptyStoreShouldReturnEmptyReplay() throws IOException {
        // Given
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");
        final JournalEventStore eventStore = new JournalEventStore(tempDirectory.toString());

        // When
        final Stream<Event> replay = eventStore.replay("non-existing-id");

        // Then
        assertEquals(0L, replay.count());

        // Cleanup
        eventStore.stop();
    }

    @Test
    public void latest() throws IOException {
        // Given
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");

        // When
        final JournalEventStore eventStore = new JournalEventStore(tempDirectory.toString());
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));

        // Then
        final Optional<Event> latest = eventStore.latest();
        assertTrue(latest.isPresent());
        assertEquals("2", latest.get().id());

        // Cleanup
        eventStore.stop();
    }

    @Test
    public void multipleReads() throws IOException, ExecutionException, InterruptedException {
        // Given
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");

        // When
        final JournalEventStore eventStore = new JournalEventStore(tempDirectory.toString());

        // Create some events
        final int max = 10000;
        for (int i = 0; i < max; i++) {
            eventStore.append(createEvent(Integer.toString(i)));
        }

        final Callable<Long> task = () -> eventStore.replay("20").filter(e -> {
            if (e.id().equals("50")) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            return true;
        }).count();

        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final Future<Long> f1 = executorService.submit(task);
        final Future<Long> f2 = executorService.submit(task);

        final Long c1 = f1.get();
        final Long c2 = f2.get();
        eventStore.stop();

        // Then
        assertEquals(c1, Long.valueOf(max - 21));
        assertEquals(c2, Long.valueOf(max - 21));
    }

    @Test
    public void replayAll() throws IOException {
        // Given
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");

        // When
        final JournalEventStore eventStore = new JournalEventStore(tempDirectory.toString());
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));
        eventStore.append(createEvent("3"));

        // Then
        final long count = eventStore.replayAll().count();
        assertEquals(3L, count);

        // Cleanup
        eventStore.stop();
    }

    @Test
    public void retrieveStreamAndThenAddMoreElements() throws IOException {
        // Given
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");
        final JournalEventStore eventStore = new JournalEventStore(tempDirectory.toString());
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));

        // When
        final Stream<Event> replay = eventStore.replay("1");
        eventStore.append(createEvent("3"));

        // Then
        final long count = replay.count();
        assertEquals(2L, count); // This means that the event store is lazy which is good.
    }

    @Test
    public void verifyThatLatestDiffersWhenNewElementsHaveBeenAdded() throws IOException {
        // Given
        final Path tempDirectory = Files.createTempDirectory("org.nomq.test");

        // When
        final JournalEventStore eventStore = new JournalEventStore(tempDirectory.toString());
        eventStore.append(createEvent("1"));
        eventStore.append(createEvent("2"));
        final Optional<Event> latest1 = eventStore.latest();
        eventStore.append(createEvent("3"));
        final Optional<Event> latest2 = eventStore.latest();

        // Then
        assertTrue(latest1.isPresent());
        assertTrue(latest2.isPresent());
        assertEquals("2", latest1.get().id());
        assertEquals("3", latest2.get().id());

        // Cleanup
        eventStore.stop();
    }

    private Event createEvent(final String id) {
        return new SerializableEvent(id, id.getBytes());
    }
}
