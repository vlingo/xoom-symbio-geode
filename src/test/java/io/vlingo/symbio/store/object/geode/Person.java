// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.geode;

import io.vlingo.symbio.store.object.PersistentObject;

public class Person extends PersistentObject {
  private static final long serialVersionUID = 1L;

  public final long id;
  public final int age;
  public final String name;

  public Person(final String name, final int age, final long persistenceId) {
    super(persistenceId);
    this.name = name;
    this.age = age;
    this.id = persistenceId;
  }
  
  protected Person(final String name, final int age, final long persistenceId, final long version) {
    super(persistenceId, version);
    this.name = name;
    this.age = age;
    this.id = persistenceId;
  }
  
  public Person withAge(final int age) {
    return new Person(name, age, id, version());
  }

  public Person withName(final String name) {
    return new Person(name, age, id, version());
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
      .append(", id=").append(id)
      .append(", name=").append(name)
      .append(")")
      .toString();
  }
}
