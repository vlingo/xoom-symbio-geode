// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

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
import io.vlingo.symbio.store.state.StateStore.Dispatcher;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;
import io.vlingo.symbio.store.state.StateStore.RedispatchControl;
/**
 * GeodeRedispatchControlActor is responsible for requesting re-dispatch
 * of the unconfirmed dispatchables of a GeodeStateStoreActor on a
 * configurable, periodic basis. This allows the work of re-dispatching
 * to be shifted to a different thread than the one responsible for
 * reading and writing in the state store.
 */
public class GeodeRedispatchControlActor extends Actor
implements DispatcherControl, RedispatchControl, Scheduled<Object> {

  private final Cache cache;
  private final Dispatcher dispatcher;
  private final Cancellable cancellable;
  private Query allUnconfirmedDispatablesQuery;
  
  public GeodeRedispatchControlActor(final Dispatcher dispatcher, final Cache cache, final long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.cache = cache;
    this.dispatcher = dispatcher;
    this.cancellable = scheduler().schedule(selfAs(Scheduled.class), null, confirmationExpiration, checkConfirmationExpirationInterval);
  }
  
  /* @see io.vlingo.common.Scheduled#intervalSignal(io.vlingo.common.Scheduled, java.lang.Object) */
  @Override
  public void intervalSignal(Scheduled<Object> scheduled, Object data) {
    dispatchUnconfirmed();
  }

  /* @see io.vlingo.symbio.store.state.StateStore.DispatcherControl#confirmDispatched(java.lang.String, io.vlingo.symbio.store.state.StateStore.ConfirmDispatchedResultInterest) */
  @Override
  public void confirmDispatched(String dispatchId, ConfirmDispatchedResultInterest interest) {
    //System.out.println("GeodeRedispatchControlActor::confirmDispatched - executing on " + Thread.currentThread().getName());
    Region<String, GeodeDispatchable<ObjectState<Object>>> region =
      cache.getRegion(GeodeQueries.DISPATCHABLES_REGION_NAME);
    region.remove(dispatchId);
    //System.out.println("GeodeRedispatchControlActor::confirmDispatched - removed dispatchId=" + dispatchId);
    interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
  }

  /* @see io.vlingo.symbio.store.state.StateStore.DispatcherControl#dispatchUnconfirmed() */
  @Override
  public void dispatchUnconfirmed() {
    //System.out.println("GeodeRedispatchControlActor::dispatchUnconfirmed - executing on " + Thread.currentThread().getName());
    try {
      Collection<GeodeDispatchable<ObjectState<Object>>> dispatchables = allUnconfirmedDispatchableStates();
      for (GeodeDispatchable<ObjectState<Object>> dispatchable : dispatchables) {
        //System.out.println("GeodeRedispatchControlActor::dispatchUnconfirmed - calling dispatcher.dispatch for " + dispatchable.id + " writtenAt " + dispatchable.writtenAt + " on " + Thread.currentThread().getName());
        dispatcher.dispatch(dispatchable.id, dispatchable.state);
      }
    } catch (Exception ex) {
      logger().log(getClass().getSimpleName() + " dispatchUnconfirmed() failed because: " + ex.getMessage(), ex);
    }
  }
  
  @SuppressWarnings("unchecked")
  private Collection<GeodeDispatchable<ObjectState<Object>>> allUnconfirmedDispatchableStates() throws Exception {
    List<GeodeDispatchable<ObjectState<Object>>> dispatchables =
      new ArrayList<GeodeDispatchable<ObjectState<Object>>>();
    SelectResults<GeodeDispatchable<ObjectState<Object>>> selected =
      (SelectResults<GeodeDispatchable<ObjectState<Object>>>) allUnconfirmedDispatchablesQuery().execute();
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

  /* @see io.vlingo.actors.Actor#afterStop() */
  @Override
  protected void afterStop() {
    super.afterStop();
    if (cancellable != null) {
      cancellable.cancel();
    }
  }
}
