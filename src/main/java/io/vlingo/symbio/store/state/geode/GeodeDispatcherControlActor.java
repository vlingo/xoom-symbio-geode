// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import io.vlingo.actors.Actor;
import io.vlingo.common.Cancellable;
import io.vlingo.common.Scheduled;
import io.vlingo.symbio.State.ObjectState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.state.StateStore.ConfirmDispatchedResultInterest;
import io.vlingo.symbio.store.state.StateStore.Dispatchable;
import io.vlingo.symbio.store.state.StateStore.Dispatcher;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;
/**
 * GeodeDispatcherControlActor is responsible for requesting re-dispatch
 * of the unconfirmed {@link Dispatchable} of a GeodeStateStoreActor on a
 * configurable, periodic basis. This allows the work of re-dispatching
 * to be shifted to a different thread than the one responsible for
 * reading and writing in the state store.
 */
public class GeodeDispatcherControlActor extends Actor
implements DispatcherControl, Scheduled<Object> {
  
  public final static long DEFAULT_REDISPATCH_DELAY = 2000L;

  private final Cache cache;
  private final String originatorId;
  private final Dispatcher dispatcher;
  private final long confirmationExpiration;
  private final Cancellable cancellable;
  private Query allUnconfirmedDispatablesQuery;
  
  @SuppressWarnings("unchecked")
  public GeodeDispatcherControlActor(final String originatorId, final Dispatcher dispatcher, final Cache cache, final long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.originatorId = originatorId;
    this.cache = cache;
    this.dispatcher = dispatcher;
    this.confirmationExpiration = confirmationExpiration;
    this.cancellable = scheduler().schedule(selfAs(Scheduled.class), null, DEFAULT_REDISPATCH_DELAY, checkConfirmationExpirationInterval);
  }
  
  @Override
  public void intervalSignal(Scheduled<Object> scheduled, Object data) {
    dispatchUnconfirmed();
  }

  @Override
  public void confirmDispatched(String dispatchId, ConfirmDispatchedResultInterest interest) {
    Region<String, GeodeDispatchable<ObjectState<Object>>> region =
      cache.getRegion(GeodeQueries.DISPATCHABLES_REGION_NAME);
    region.remove(dispatchId);
    interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
  }

  @Override
  public void dispatchUnconfirmed() {
    try {
      final LocalDateTime now = LocalDateTime.now();
      Collection<GeodeDispatchable<ObjectState<Object>>> dispatchables = allUnconfirmedDispatchables();
      for (GeodeDispatchable<ObjectState<Object>> dispatchable : dispatchables) {
        final LocalDateTime then = dispatchable.createdAt;
        Duration duration = Duration.between(then, now);
        if (Math.abs(duration.toMillis()) > confirmationExpiration) {
          dispatcher.dispatch(dispatchable.id, dispatchable.state);
        }
      }
    }
    catch (Exception ex) {
      logger().log(getClass().getSimpleName() + " dispatchUnconfirmed() failed because: " + ex.getMessage(), ex);
    }
  }
  
  @SuppressWarnings("unchecked")
  private Collection<GeodeDispatchable<ObjectState<Object>>> allUnconfirmedDispatchables() throws Exception {
    SelectResults<GeodeDispatchable<ObjectState<Object>>> selected =
      (SelectResults<GeodeDispatchable<ObjectState<Object>>>) allUnconfirmedDispatchablesQuery().execute(originatorId);
    List<GeodeDispatchable<ObjectState<Object>>> dispatchables =
            new ArrayList<GeodeDispatchable<ObjectState<Object>>>();
    for (GeodeDispatchable<ObjectState<Object>> dispatchable : selected) {
      dispatchables.add(dispatchable);
    }
    return dispatchables;
  }
  
  private Query allUnconfirmedDispatchablesQuery() {
    if (allUnconfirmedDispatablesQuery == null) {
      QueryService queryService = cache.getQueryService();
      allUnconfirmedDispatablesQuery = queryService.newQuery(GeodeQueries.OQL_DISPATCHABLES_SELECT);
    }
    return allUnconfirmedDispatablesQuery;
  }

  @Override
  protected void afterStop() {
    super.afterStop();
    if (cancellable != null) {
      cancellable.cancel();
    }
  }
}
