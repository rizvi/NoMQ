package org.nomq.core.setup;

import org.junit.Test;
import org.nomq.core.Event;
import org.nomq.core.setup.NoMQ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class NoMQTest {
    @Test
    public void testSetupWithFolders() throws InterruptedException, IOException {
        // Given
        final Path recordFolder = Files.createTempDirectory("org.nomq.test");
        final Path playFolder = Files.createTempDirectory("org.nomq.test");

        final List<Event> result = new ArrayList<>();
        final NoMQ noMQ =
                new NoMQ.Builder()
                        .play(playFolder.toString())
                        .record(recordFolder.toString())
                        .eventSubscribers(e -> result.add(e))
                        .build();
        noMQ.start();

        // When
        noMQ.publisher().publish("Simple event".getBytes());

        // Wait for the message to be delivered
        Thread.sleep(200);

        // Then
        assertEquals(1, result.size());

        // Cleanup
        noMQ.stop();
    }
}
