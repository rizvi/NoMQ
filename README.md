NoMQ
====

Not a Message Queue (but a simple drop-in event queue).

NoMQ is a simple distributed event queue that has features such as:
 * durability
 * late join

```java
// Initialize and start NoMQ
NoMQ noMQ = NoMQBuilder.builder()
    .build() // Build the NoMQ-instance
    .start(); // Start it

// Publish a message
noMQ.publish("Some payload".getBytes());
```

To register a simple event subscriber that will receive all events in the cluster in the correct order:

```java
// Initialize NoMQ
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe(e -> System.out.println(e.id())) // Register a subscriber
    .subscribe(e -> doSomethingWithEvent(e)) // Register another subscriber
    .build() // Build the instance
    .start(); // Start it
```
