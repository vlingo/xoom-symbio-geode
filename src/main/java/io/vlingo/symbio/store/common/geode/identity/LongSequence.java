// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode.identity;

import java.io.Serializable;
/**
 * LongSequence models a monotonic sequence of Long-valued identifiers.
 */
public class LongSequence implements Serializable {
  
  private static final long serialVersionUID = 1L;
  
  private final String name;
  private Long last = 0L;

  /**
   * Constructs a {@link LongSequence} named {@code name}.
   * 
   * @param name the name of the sequence
   */
  public LongSequence(final String name) {
    super();
    this.name = name;
  }
  
  /**
   * Returns the receiver's name.
   * 
   * @return the receiver's name
   */
  public String name() {
    return name;
  }
  
  /**
   * Returns the last identifier allocated from the sequence.
   * 
   * @return the last identifier allocated from the sequence
   */
  public Long last() {
    return last;
  }
  
  /**
   * Returns a {@link LongIDAllocation} containing {@code allocationSize}
   * identifiers.
   * 
   * @param allocationSize the number of identifiers to allocate
   * 
   * @return a {@link LongIDAllocation} containing {@code allocationSize}
   * identifiers
   */
  public LongIDAllocation allocate(final Long allocationSize) {
    LongIDAllocation allocation = new LongIDAllocation(last + 1, last + allocationSize);
    last += allocationSize;
    return allocation;
  }

  /* @see java.lang.Object#hashCode() */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  /* @see java.lang.Object#equals(java.lang.Object) */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    LongSequence other = (LongSequence) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }

  /* @see java.lang.Object#toString() */
  @Override
  public String toString() {
    return new StringBuilder()
      .append("LongSequence(")
      .append("name=").append(name)
      .append(", last=").append(last)
      .append(")")
      .toString();
  }
}
