# vlingo-symbio-geode

[![Javadocs](http://javadoc.io/badge/io.vlingo/vlingo-symbio-geode.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo/vlingo-symbio-geode) [![Build](https://github.com/vlingo/vlingo-symbio-geode/workflows/Build/badge.svg)](https://github.com/vlingo/vlingo-symbio-geode/actions?query=workflow%3ABuild) [![Download](https://img.shields.io/maven-central/v/io.vlingo/vlingo-symbio-geode?label=maven)](https://search.maven.org/artifact/io.vlingo/vlingo-symbio-geode) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/vlingo-platform-java/symbio)

The VLINGO XOOM platform SDK implementation of XOOM SYMBIO for Apache Geode, providing reactive storage for services and applications.

Docs: https://docs.vlingo.io/vlingo-symbio

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
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio</artifactId>
      <version>1.5.2</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-geode</artifactId>
      <version>1.5.2</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:1.5.2'
    compile 'io.vlingo:vlingo-symbio-geode:1.5.2'
}
```

License (See LICENSE file for full license)
-------------------------------------------
Copyright © 2012-2020 VLINGO LABS. All rights reserved.

This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at https://mozilla.org/MPL/2.0/.
