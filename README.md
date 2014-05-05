# NoMQ
__NoMQ__ is short for _Not a Message Queue_. It is a distributed event queue that is based on Java 8 and Hazelcast. NoMQ
requires __NO__ installation, simply add the jar to your project.

## Prerequisites
* Java 8

## Features
 * __durability:__ events survive reboots
 * __late join:__ new nodes can join later on
 * __ordering:__ events are delivered in the same order on all nodes
 * __no additional installation required:__ simply include the jars and configure NoMQ from within your Java-code.

# Getting started
If you want to subscribe for events in the NoMQ cluster you need to register an _EventSubscriber_. The following code starts a
NoMQ instance and registers an event subscriber that simply echoes the event id on _System.out_. Events can be published by any
node in the cluster, they will arrive at all event subscribers in the same order.

```java
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe(e -> System.out.println(e.id()))
    .subscribe(e -> doSomethingWithEvent(e))
    .build()
    .start();

// Publish an event asynchronously
noMQ.publishAsync("Some payload".getBytes());
```

NoMQ only supports dispatching events where the payload is of type _byte[]_. This may seem like a harsh limitation but the
solution to this is to use _Converter_s.

```
// Convert the String to a byte[]
noMQ.publishAsync("Some payload", str -> str.getBytes());
```

Subscription of events is done via _EventSubscribers_. Subscribers are registered setup and simply implements the method
_onEvent_.

```java
public interface EventSubscriber {
    void onEvent(Event event);
}
```

The event interface is straightforward, it contains a generated unique id together with the payload provided by the
publisher of the event.
```java
public interface Event {
    String id();
    byte[] payload();
}
```

It is also possible to register a converter when subscribing to events. The byte[] is then converted back to any type and
dispatches the event using a _PayloadSubscriber_.

```
public interface PayloadSubscriber<T> {
    void onPayload(T payload);
}
```

To register the _PayloadSubscriber_ and _Converter_ use the following code:
```
// Register subscriber using a converter
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe(str -> System.out.println(str), bytes -> new String(bytes))
    .build()
    .start();

// Publish using a converter
noMQ.publishAsync("A string", str -> str.getBytes());
```

## Configuring the cluster
NoMQ is based on Hazelcast - to set up a cluster follow the instructions on the Hazelcast website
(http://hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#network-configuration)


