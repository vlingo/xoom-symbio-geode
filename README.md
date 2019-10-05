# vlingo-symbio-geode

[![Javadocs](http://javadoc.io/badge/io.vlingo/vlingo-symbio-geode.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo/vlingo-symbio-geode) [![Build Status](https://travis-ci.org/vlingo/vlingo-symbio-geode.svg?branch=master)](https://travis-ci.org/vlingo/vlingo-symbio-geode) [ ![Download](https://api.bintray.com/packages/vlingo/vlingo-platform-java/vlingo-symbio-geode/images/download.svg) ](https://bintray.com/vlingo/vlingo-platform-java/vlingo-symbio-geode/_latestVersion) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/vlingo-platform-java/symbio)

The vlingo/PLATFORM implementation of vlingo/symbio for Apache Geode providing reactive storage for services and applications.

Supports Apache Geode transations, State Storage (K-V) and Object Storage, but with `Source<T>` / `Entry<T>` for `DomainEvent` and `Command` journaling.

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.

Support for Apache Geode is provided by `GeodeStateStoreActor`

### Object Storage
The `ObjectStore` is an object storage mechanism managing persistent objects in grid.

See `GeodeObjectStoreActor`


### Bintray

```xml
  <repositories>
    <repository>
      <id>jcenter</id>
      <url>https://jcenter.bintray.com/</url>
    </repository>
  </repositories>
  <dependencies>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio</artifactId>
      <version>0.9.0-RC1</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-geode</artifactId>
      <version>0.9.0-RC1</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:0.9.0-RC1'
    compile 'io.vlingo:vlingo-symbio-geode:0.9.0-RC1'
}

repositories {
    jcenter()
}
```

License (See LICENSE file for full license)
-------------------------------------------
Copyright © 2012-2018 Vaughn Vernon. All rights reserved.

This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at https://mozilla.org/MPL/2.0/.
