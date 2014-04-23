package org.nomq.core;

public interface EventSubscriber {
    void onEvent(Event event);
}
