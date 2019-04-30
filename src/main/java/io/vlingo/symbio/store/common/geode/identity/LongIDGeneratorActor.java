// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode.identity;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.store.common.geode.Configuration;
/**
 * LongIDGeneratorActor
 */
public class LongIDGeneratorActor extends Actor implements IDGenerator<Long> {
  
  private final LongIDGenerator delegate;
  
  public LongIDGeneratorActor(final Configuration config, final String sequenceRegionPath, final Long allocationSize) {
    this.delegate = new LongIDGenerator(config, sequenceRegionPath, allocationSize);
  }

  /* @see io.vlingo.symbio.store.common.geode.IDGenerator#next(java.lang.String) */
  @Override
  public Completes<Long> next(String sequenceName) {
    return completes().with(delegate.next(sequenceName));
  }
}
