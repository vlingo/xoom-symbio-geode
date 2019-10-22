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
import io.vlingo.symbio.store.object.ObjectStoreDelegate;

import java.util.Map;

/**
 * AtomicGeodeObjectStoreDelegate
 */
public class AtomicGeodeObjectStoreDelegate extends GeodeObjectStoreDelegate {

  public AtomicGeodeObjectStoreDelegate(
    final World world,
    final String originatorId,
    final StateAdapterProvider stateAdapterProvider) {
    super(world, originatorId, stateAdapterProvider);
  }

  @Override
  public ObjectStoreDelegate copy() {
    return new AtomicGeodeObjectStoreDelegate(
      world(),
      getOriginatorId(),
      stateAdapterProvider());
  }

  @Override
  protected Long nextEntryId(Entry<?> entry) {
    final GeodePersistentObjectMapping mapping = mappingFor(entry.typed());
    Map<Object,Object> registered = unitOfWork().registeredForPath(mapping.regionPath);
    Long lastId = (Long) registered.computeIfAbsent("ENTRY_ID", o -> lookupEntryId(mapping.regionPath));
    Long nextId = lastId + 1L;
    registered.put("ENTRY_ID", nextId);
    return nextId;
  }

  private Long lookupEntryId(final String path) {
    return (Long) regionFor(path).getOrDefault("ENTRY_ID", 0L);
  }
}