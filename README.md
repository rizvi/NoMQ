NoMQ
====

Not a Message Queue (but a simple drop-in event queue).

NoMQ is a simple distributed event queue that has features such as:
 * durability
 * late join

```java
// Initialize NoMQ
NoMQ noMq = new NoMQ.Builder().build();

// Start it
noMq.start();

// Publish a message
noMq.publisher().publish("Some payload".getBytes());
```

To register a simple event subscriber:

```java
// Initialize NoMQ
NoMQ noMq = new NoMQ.Builder().eventSubscribers(e -> System.out.println(e.id())).build();
```


