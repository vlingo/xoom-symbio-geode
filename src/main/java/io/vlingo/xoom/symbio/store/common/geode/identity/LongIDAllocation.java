// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common.geode.identity;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

import java.util.concurrent.atomic.AtomicLong;
/**
 * LongIDAllocation represents a monotonically increasing, gapless
 * block of Long valued identifiers that have been allocated from
 * a sequence.
 */
public class LongIDAllocation implements PdxSerializable {

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

  @Override
  public void toData(PdxWriter out) {
    out
      .writeLong("next", next.longValue())
      .writeLong("last", last);
  }

  @Override
  public void fromData(PdxReader in) {
    next.set(in.readLong("next"));
    this.last = in.readLong("last");
  }
}
