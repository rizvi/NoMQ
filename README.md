# NoMQ [![build status](https://secure.travis-ci.org/wassgren/NoMQ.png)](http://travis-ci.org/wassgren/NoMQ)

__NoMQ__ is short for _Not a Message Queue_. It is a __distributed event queue__ that is based on Java 8 and
[Hazelcast](http://hazelcast.org/). NoMQ requires __NO__ installation, simply add the jar to your project.

Events are published somewhere in a cluster and dispatched to all event subscribers in the entire cluster, in the same order,
everywhere.

## Prerequisites
* Java 8.
* A sane build environment (e.g. [Maven](http://maven.apache.org/), [Ivy](https://ant.apache.org/ivy/) or [Gradle](http://www.gradle.org/).

## Features
 * __durability:__ events survive reboots
 * __late join:__ new nodes can join later on
 * __ordering:__ events are delivered in the same order on all nodes
 * __no additional installation required:__ simply include the jars and configure NoMQ from within your Java-code.

## Getting started
Publishing an event requires a handle to a NoMQ-instance, instances are created using the _NoMQBuilder_. The event contains an
event type that describes what kind of event you're triggering and a payload which is the actual event data.

Subscribing to an event also requires a handle to a NoMQ-instance. Furthermore, you must register one or more instances of the
class _EventSubscriber_.

The code below creats a NoMQ-instance using the _NoMQBuilder_. It also registers an _EventSubscriber_ that simply echoes the id
of all received events on _System.out_.

```java
NoMQ noMQ = NoMQBuilder.builder()
    .subscribe(e -> System.out.println(e.id()))
    .build()
    .start();

// Publish an event asynchronously
noMQ.publishAsync("myEvent", "Some payload".getBytes());
```

The payload for an event is always a byte array. This may seem like a strict limitation so if you need to dispatch richer
objects the solution to this is to use a _PayloadConverter_. The code below uses a converter that converts a _String_ to a
_byte[]_.

```java
noMQ.publishAsync("myEvent", "Some payload", str -> str.getBytes());
```

If you want to subscribe to the payload in some other format than a byte array it is possible to use the _PayloadConverter_ for
subscriptions as well. The _PayloadSubscriber_ is used together with a _PayloadConverter_ as in the example below.

```java
// Register payload subscriber and converter,
// the byte[] is converted to a String
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
