// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.geode;

import io.vlingo.xoom.symbio.store.object.StateObject;

public class Person extends StateObject {
  private static final long serialVersionUID = 1L;

  public final int age;
  public final String name;

  public Person(final String name, final int age, final long persistenceId) {
    super(persistenceId);
    this.name = name;
    this.age = age;
  }
  
  public Person(final String name, final int age, final long persistenceId, final long version) {
    super(persistenceId, version);
    this.name = name;
    this.age = age;
  }
  
  public Person withAge(final int age) {
    return new Person(name, age, persistenceId(), version());
  }

  public Person withName(final String name) {
    return new Person(name, age, persistenceId(), version());
  }

  @Override
  public int hashCode() {
    return 31 * name.hashCode() * age * (int) persistenceId();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    }

    final Person otherPerson = (Person) other;

    return persistenceId() == otherPerson.persistenceId() && name.equals(otherPerson.name) && age == otherPerson.age;
  }

  /* @see java.lang.Object#toString() */
  @Override
  public String toString() {
    return new StringBuilder()
      .append("Person(")
      .append("persistenceId=").append(persistenceId())
      .append(", version=").append(version())
      .append(", age=").append(age)
      .append(", name=").append(name)
      .append(")")
      .toString();
  }
}
