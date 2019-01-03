# vlingo-symbio-geode
Implementation of vlingo-symbio for Apache Geode.

[![Javadocs](http://javadoc.io/badge/io.vlingo/vlingo-symbio-geode.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo/vlingo-symbio-geode) [![Build Status](https://travis-ci.org/vlingo/vlingo-symbio-geode.svg?branch=master)](https://travis-ci.org/vlingo/vlingo-symbio-geode) [ ![Download](https://api.bintray.com/packages/vlingo/vlingo-platform-java/vlingo-symbio-geode/images/download.svg) ](https://bintray.com/vlingo/vlingo-platform-java/vlingo-symbio-geode/_latestVersion)

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.

Support for Apache Geode is provided by `GeodeStateStoreActor`

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
      <version>0.7.8</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-geode</artifactId>
      <version>0.7.8</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:0.7.8'
    compile 'io.vlingo:vlingo-symbio-geode:0.7.8'
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
