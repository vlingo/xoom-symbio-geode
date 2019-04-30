// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.geode.identity;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
/**
 * LongIDAllocation represents a monotonically increasing, gapless
 * block of Long valued identifiers that have been allocated from
 * a sequence.
 */
public class LongIDAllocation implements Serializable {
  
  private static final long serialVersionUID = 1L;
  
  private final AtomicLong next;
  private Long last;

  /**
   * Constructs a {@link LongIDAllocation} that has no
   * unused identifiers.
   */
  public LongIDAllocation() {
    this(0L, -1L);
  }
  
  /**
   * Constructs a {@link LongIDAllocation} whose block starts
   * with {@code first} and ends with {@code last}.
   * 
   * @param first the first ID in the allocation
   * @param last the last ID in the allocation
   */
  public LongIDAllocation(final Long first, final Long last) {
    super();
    this.next = new AtomicLong(first);
    this.last = last;
  }

  /**
   * Returns the next identifier in the block.
   * 
   * @return the next identifier in the block
   */
  public Long next()
  {
      return next.getAndIncrement();
  }

  /**
   * Returns true if this block contains unused identifiers.
   * 
   * @return true if this block contains unused identifiers
   */
  public boolean hasNext()
  {
      return next.longValue() <= last;
  }
}
