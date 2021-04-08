// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode.dispatch;

import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.State.ObjectState;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.xoom.symbio.store.common.geode.GeodeQueries;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
/**
 * GeodeDispatcherControlDelegate is responsible for implementing functions required by {@code DispatcherControl} actor.
 */
public class GeodeDispatcherControlDelegate implements DispatcherControl.DispatcherControlDelegate<Entry<?>, State<?>> {

  private final String originatorId;
  private Query allUnconfirmedDispatchablesQuery;
  
  public GeodeDispatcherControlDelegate(final String originatorId) {
    this.originatorId = originatorId;
  }

  private Query allUnconfirmedDispatchablesQuery() {
    if (allUnconfirmedDispatchablesQuery == null) {
      QueryService queryService = cache().getQueryService();
      allUnconfirmedDispatchablesQuery = queryService.newQuery(GeodeQueries.ALL_UNCONFIRMED_DISPATCHABLES_SELECT);
    }
    return allUnconfirmedDispatchablesQuery;
  }


  private GemFireCache cache() {
    Optional<GemFireCache> cacheOrNull = GemFireCacheProvider.getAnyInstance();
    if (cacheOrNull.isPresent()) {
      return cacheOrNull.get();
    }
    else {
      throw new StorageException(Result.NoTypeStore, "No GemFireCache has been created in this JVM");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Dispatchable<Entry<?>, State<?>>> allUnconfirmedDispatchableStates() throws Exception {
    SelectResults<GeodeDispatchable<State<?>>> selected =
            (SelectResults<GeodeDispatchable<State<?>>>) allUnconfirmedDispatchablesQuery().execute(originatorId);
    return new ArrayList<>(selected);
  }

  @Override
  public void confirmDispatched(final String dispatchId) {
    Region<String, GeodeDispatchable<ObjectState<Object>>> region =
            cache().getRegion(GeodeQueries.DISPATCHABLES_REGION_PATH);
    region.remove(dispatchId);
  }

  @Override
  public void stop() {

  }

  public String getOriginatorId() {
    return originatorId;
  }
}
