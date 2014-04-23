package org.nomq.core;

public interface EventPublisher {
    void publish(byte[] payload);
}
