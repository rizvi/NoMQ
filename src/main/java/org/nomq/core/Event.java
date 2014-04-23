package org.nomq.core;

public interface Event {
    String id();

    byte[] payload();
}
