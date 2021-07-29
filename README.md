# xoom-symbio-geode

[![Javadocs](http://javadoc.io/badge/io.vlingo.xoom/xoom-symbio-geode.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo.xoom/xoom-symbio-geode) [![Build](https://github.com/vlingo/xoom-symbio-geode/workflows/Build/badge.svg)](https://github.com/vlingo/xoom-symbio-geode/actions?query=workflow%3ABuild) [![Download](https://img.shields.io/maven-central/v/io.vlingo.xoom/xoom-symbio-geode?label=maven)](https://search.maven.org/artifact/io.vlingo.xoom/xoom-symbio-geode) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/vlingo-platform-java/symbio)

The VLINGO XOOM platform SDK implementation of XOOM SYMBIO for Apache Geode, providing reactive storage for services and applications.

Docs: https://docs.vlingo.io/xoom-symbio

Supports Apache Geode transations, State Storage (Key-Value) and Object Storage, but with `Source<T>` / `Entry<T>` for `DomainEvent` and `Command` journaling.

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.

Support for Apache Geode is provided by `GeodeStateStoreActor`

### Object Storage
The `ObjectStore` is an object storage mechanism managing persistent objects in grid.

See `GeodeObjectStoreActor`

### Installation

```xml
  <dependencies>
    <dependency>
      <groupId>io.vlingo.xoom</groupId>
      <artifactId>xoom-symbio</artifactId>
      <version>1.8.3</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo.xoom</groupId>
      <artifactId>xoom-symbio-geode</artifactId>
      <version>1.8.3</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo.xoom:xoom-symbio:1.8.3'
    compile 'io.vlingo.xoom:xoom-symbio-geode:1.8.3'
}
```

License (See LICENSE file for full license)
-------------------------------------------
Copyright © 2012-2021 VLINGO LABS. All rights reserved.

This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at https://mozilla.org/MPL/2.0/.
