// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.uow.GeodeUnitOfWork;
import io.vlingo.symbio.store.object.StateObject;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

import java.util.HashMap;
import java.util.Map;

public class GeodeObjectUOW extends GeodeUnitOfWork implements PdxSerializable {

  private Map<String, Map<Long, StateObject>> entitiesByRegion;

  public GeodeObjectUOW() {
    super();
    this.entitiesByRegion = new HashMap<>();
  }

  public Map<String, Map<Long, StateObject>> entitiesByRegion() {
    return entitiesByRegion;
  }

  public void persistEntity(final String regionPath, final StateObject entityToPersist) {
    entitiesByRegion
      .computeIfAbsent(regionPath, k -> new HashMap<>())
      .put(entityToPersist.persistenceId(), entityToPersist);
  }

  @Override
  public void applyTo(GemFireCache cache) throws StorageException {
    super.applyTo(cache);
    applyEntitiesTo(cache);
  }

  private void applyEntitiesTo(final GemFireCache cache) throws StorageException {
    for (final String regionPath : entitiesByRegion.keySet()) {
      regionFor(cache, regionPath).putAll(entitiesByRegion.get(regionPath));
    }
  }

  @Override
  public void toData(final PdxWriter out) {
    super.toData(out);
    out
      .writeObject("entitiesByRegionPath", entitiesByRegion);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void fromData(final PdxReader in) {
    super.fromData(in);
    this.entitiesByRegion = (Map<String, Map<Long, StateObject>>) in.readObject("entitiesByRegionPath");
  }

  @Override
  public String toString() {
    return "GeodeObjectUOW{" +
      "id=" + id +
      ", timestamp=" + timestamp +
      ", entitiesByRegion=" + entitiesByRegion +
      ", entriesById=" + entriesById +
      ", dispatchablesById=" + dispatchablesById +
      '}';
  }
}
