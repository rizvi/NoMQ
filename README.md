NoMQ
====

Not a Message Queue (but a simple drop-in event queue).

NoMQ is a simple distributed event queue that has features such as:
 * durability
 * late join
 * no additional installation required - simply include the jars and configure NoMQ from within your Java-code.

Events can be published by any node in the cluster. The event will later be dispatched to all EventSubscribers in the same
order.

The events simply contains a generated unique id together with the payload provided by the publisher.
```java
public interface Event {
    String id();
    byte[] payload();
}
```

EventSubscribers simply implements the onEvent-method.
```java
public interface EventSubscriber {
    void onEvent(Event event);
}
```

The following snippet shows how to do the default configuration of the NoMQ-instance.
```java
// Initialize and start NoMQ
NoMQ noMQ = NoMQBuilder.builder()
    .build()  // Build the NoMQ-instance
    .start(); // Start it

// Publish a message
noMQ.publish("Some payload".getBytes());
```

If you want to handle events you need to register an EventSubscriber. The following shows how to register two subscribers that
receives all events in the cluster in the correct order.

```java
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe(e -> System.out.println(e.id())) // Register a subscriber
    .subscribe(e -> doSomethingWithEvent(e))    // Register another subscriber
    .build()                                    // Build the instance
    .start();                                   // Start it
```
