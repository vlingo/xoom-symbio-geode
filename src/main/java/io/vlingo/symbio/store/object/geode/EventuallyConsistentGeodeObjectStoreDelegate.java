// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.common.geode.identity.LongIDGenerator;
import io.vlingo.symbio.store.object.ObjectStoreDelegate;
/**
 * EventuallyConsistentGeodeObjectStoreDelegate is an eventually-consistent
 * implementation of {@link GeodeObjectStoreDelegate}.
 */
public class EventuallyConsistentGeodeObjectStoreDelegate extends GeodeObjectStoreDelegate {

  private LongIDGenerator idGenerator;

  public EventuallyConsistentGeodeObjectStoreDelegate(
    final World world,
    final String originatorId,
    final StateAdapterProvider stateAdapterProvider) {
    super(world, originatorId, stateAdapterProvider);
  }

  @Override
  public ObjectStoreDelegate copy() {
    return new EventuallyConsistentGeodeObjectStoreDelegate(
      world(),
      getOriginatorId(),
      stateAdapterProvider());
  }

  @Override
  public void completeTransaction() {
    final Long uowId = nextUowId();
    unitOfWork().withId(uowId);
    regionFor(uowRegionPath()).put(uowId, unitOfWork());
    super.completeTransaction();
  }

  private LongIDGenerator idGenerator() {
    if (idGenerator == null) {
      new LongIDGenerator(GeodeQueries.SEQUENCE_REGION_PATH, 1L);
    }
    return idGenerator;
  }

  private Long nextUowId() {
    return idGenerator().next(GeodeQueries.UOW_SEQUENCE_NAME);
  }

  @Override
  protected Long nextEntryId(Entry<?> entry) {
    return idGenerator().next(GeodeQueries.ENTRY_SEQUENCE_NAME);
  }

  private String uowRegionPath() {
    return GeodeQueries.UOW_REGION_PATH;
  }
}