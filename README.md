# NoMQ [![build status](https://secure.travis-ci.org/wassgren/NoMQ.png)](http://travis-ci.org/wassgren/NoMQ)

__NoMQ__ is short for _Not a Message Queue_. It is a __distributed event queue__ that is based on Java 8 and
[Hazelcast](http://hazelcast.org/). NoMQ requires __NO__ installation, __simply add the jar to your project__.

So, what exactly is a distributed event queue? Well, events are published somewhere in a cluster (separate JVM:s). They are then
dispatched to all event subscribers in the entire cluster, in the same order, everywhere.

## Prerequisites
* Java 8.
* A sane build environment (e.g. [Maven](http://maven.apache.org/) or [Gradle](http://www.gradle.org/)).

## Features
 * __durability:__ events survive reboots
 * __late join:__ new nodes can join later on
 * __ordering:__ events are delivered in the same order on all nodes
 * __no additional installation required:__ simply include the jars and configure NoMQ from within your Java-code.

## Getting started
Publishing an event requires a handle to a NoMQ-instance that contains various publishing methods. ``Event```s are simple types
that contains:
* an _event type_ that describes what kind of event you're triggering
* a _payload_ that is the actual event data.

When subscribing to events you must register a subscriber using the _subscribe_ method.

The code below creates a NoMQ-instance. The way to do it is to use the _NoMQBuilder_. In the example you can also see the
registration of an event subscriber that simply echoes the id of all received events to _System.out_.

```java
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe(event -> System.out.println(event.id())) // register subscriber
    .build()  // Create the NoMQ-instance
    .start(); // Then start it

// Publish an event asynchronously
noMQ.publish("myEvent", "Some payload".getBytes());
```

The payload for an event is always a byte array (```byte[]```). This may seem like a strict limitation but there is a a reason
for this. Since Hazelcast relies heavy on serialization (Hazelcast distributes the objects to several nodes) it exposes various
ways of transforming objects to byte arrays. These include standard Java Serialization where you implement
```java.io.Serializable```, serialization using HazelCast's ```DataSerializable``` or ```Portable``` etc. We find that these
restrictions (```Serializable```, ```Portable``` etc) are something that we don't want in the NoMQ-API. NoMQ-users should be
able to publish any type of event and therefore the entire responsibility of serialization/deserialization is in your hands
- that's fair, isn't it?

With that being said, in order to publish payloads of *any* type simply convert the payload to a byte array using a
*payload converter*. The code below uses a converter that converts a payload of type _String_ to a _byte[]_.

```java
noMQ.publish(
    "myEvent",          // Event name
    "Some payload",     // The payload (String)
    String::getBytes);  // Converter
```

The published byte array is part of the event object that is delivered to the event subscribers. If you want to subscribe to
the payload in some other format than a byte array it is possible to provide a reverse payload converter for
subscriptions as well.

```java
// Register payload subscriber and converter,
// the byte[] is converted to a String
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe(
        "myEvent",                      // Event name
        str -> System.out.println(str), // Subscriber
        bytes -> new String(bytes))     // Converter
    .build()
    .start();

// Publish using a converter as in the example above
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
