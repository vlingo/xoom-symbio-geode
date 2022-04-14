// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode.identity;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.common.Completes;
/**
 * LongIDGeneratorActor
 */
public class LongIDGeneratorActor extends Actor implements IDGenerator<Long> {
  
  private final LongIDGenerator delegate;
  
  public LongIDGeneratorActor(final Long allocationSize) {
    this.delegate = new LongIDGenerator(allocationSize);
  }

  public LongIDGeneratorActor(final String sequenceRegionPath, final Long allocationSize) {
    this.delegate = new LongIDGenerator(sequenceRegionPath, allocationSize);
  }

  /* @see io.vlingo.xoom.symbio.store.common.geode.IDGenerator#next(java.lang.String) */
  @Override
  public Completes<Long> next(String sequenceName) {
    return completes().with(delegate.next(sequenceName));
  }
}
