package org.nomq.core.transport;

import org.nomq.core.Event;

import java.io.Serializable;

public class SerializableEvent implements Event, Serializable {
    private static final long serialVersionUID = 1L;
    private static final byte[] EMPTY = new byte[0];
    private final String id;
    private final byte[] payload;

    public SerializableEvent(final String id, final byte[] payload) {
        this.id = id;
        this.payload = payload == null ? EMPTY : payload;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public byte[] payload() {
        return payload;
    }
}
