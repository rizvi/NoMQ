__NoMQ__ is short for _Not a Message Queue_. It is a distributed event queue that is based on Java 8 and Hazelcast. NoMQ requires
__NO__ installation, simply add the jar to your project.

# Prerequisites
* Java 8

# Features
 * __durability:__ events survive reboots
 * __late join:__ new nodes can join later on
 * __ordering:__ events are delivered in the same order on all nodes
 * __no additional installation required:__ simply include the jars and configure NoMQ from within your Java-code.

# Getting started
If you want to subscribe for events in the NoMQ cluster you need to register an _EventSubscriber_. The following code starts a
NoMQ instance and registers an event subscriber that simply echoes the event id on System.out. Events can be published by any
node in the cluster, they will arrive at all event subscribers in the same order.

```java
NoMQ noMQ = NoMQBuilder.builder()               // Obtain a reference to the builder
    .subscribe(e -> System.out.println(e.id())) // Register a subscriber that echoes the id
    .subscribe(e -> doSomethingWithEvent(e))    // Register another subscriber that does something with the event
    .build()                                    // Build the NoMQ-instance
    .start();                                   // Start it (enable publish and subscribe)

// Publish an event
noMQ.publish("Some payload".getBytes());
```

The event interface is pretty straightforward, it contains a generated unique id together with the payload provided by the
publisher of the event.
```java
public interface Event {
    String id();
    byte[] payload();
}
```

The _EventSubscribers: simply implements the method _onEvent_.
```java
public interface EventSubscriber {
    void onEvent(Event event);
}
```

## Configuring the cluster
NoMQ is based on Hazelcast - to set up a cluster follow the instructions on the Hazelcast website
(http://hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#network-configuration)


