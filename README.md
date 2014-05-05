# NoMQ [![build status](https://secure.travis-ci.org/wassgren/NoMQ.png)](http://travis-ci.org/wassgren/NoMQ)

__NoMQ__ is short for _Not a Message Queue_. It is a distributed event queue that is based on Java 8 and Hazelcast. NoMQ
requires __NO__ installation, simply add the jar to your project.

## Prerequisites
* Java 8

## Features
 * __durability:__ events survive reboots
 * __late join:__ new nodes can join later on
 * __ordering:__ events are delivered in the same order on all nodes
 * __no additional installation required:__ simply include the jars and configure NoMQ from within your Java-code.

## Getting started
Triggering an event requires a handle to the NoMQ-instance, a payload that is sent to all subscribers and a name of the event
known as the event type.

If you want to subscribe to events in the NoMQ cluster you need to register an _EventSubscriber_. The following code starts a
NoMQ-instance and registers an event subscriber that simply echoes the event id on _System.out_. Events can be published by any
node in the cluster, they will arrive at all event subscribers in the same order.

```java
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe(e -> System.out.println(e.id()))
    .subscribe(e -> doSomethingWithEvent(e))
    .build()
    .start();

// Publish an event asynchronously
noMQ.publishAsync("myEvent", "Some payload".getBytes());
```

NoMQ can only dispatch events where the payload is a byte array. This may seem like a strict limitation but if you need to
dispatch richer objects the solution to this is to use _Converter_s.

```
// Converter that converts the payload of type String to a byte[]
noMQ.publishAsync("myEvent", "Some payload", str -> str.getBytes());
```

Subscription of events is done via _EventSubscribers_. Subscribers are registered setup and simply implements the method
_onEvent_.

```java
public interface EventSubscriber {
    void onEvent(Event event);
}
```

The event interface is straightforward, it contains a generated unique id, the event type and the payload provided by the
publisher of the event.
```java
public interface Event {
    String id();
    String type();
    byte[] payload();
}
```

It is also possible to register a converter when subscribing to events. The payload (byte array) is then converted back to the
correct type and dispatched using a _PayloadSubscriber_.

```
public interface PayloadSubscriber<T> {
    void onPayload(T payload);
}
```

To register the _PayloadSubscriber_ and _Converter_ use the following code:
```
// Register subscriber using a converter
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe("myEvent", str -> System.out.println(str), bytes -> new String(bytes))
    .build()
    .start();

// Publish using a converter
noMQ.publishAsync("myEvent", "A string", str -> str.getBytes());
```

## Configuring the cluster
NoMQ is based on Hazelcast - to set up a cluster follow the instructions on the Hazelcast website
(http://hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#network-configuration)

# Issues and feature requests
NoMQ uses [GitHub Issues](https://github.com/wassgren/NoMQ/issues) for feature requests and issue tracking.

# License
   Copyright 2014 the original author or authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
