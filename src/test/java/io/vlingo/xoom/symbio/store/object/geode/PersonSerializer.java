// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.object.geode;

import org.apache.geode.cache.Declarable;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;

public class PersonSerializer implements PdxSerializer, Declarable {

  @Override
  public boolean toData(Object o, PdxWriter out) {
    boolean result = false;
    if (o instanceof Person) {
      final Person p = (Person) o;
      out
        .writeLong("persistenceId", p.persistenceId())
        .markIdentityField("persistenceId")
        .writeLong("version", p.version())
        .writeInt("age", p.age)
        .writeString("name", p.name);
      result = true;
    }
    return result;
  }

  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    long persistenceId = in.readLong("persistenceId");
    long version = in.readLong("version");
    int age = in.readInt("age");
    String name = in.readString("name");
    return new Person(name, age, persistenceId, version);
  }
}
