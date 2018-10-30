// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;

import io.vlingo.actors.Actor;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.NullState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.state.ObjectStateStore;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
/**
 * GeodeStateStoreActor is responsible for reading and writing
 * objects from/to a GemFire cache.
 *
 * @author davem
 * @since Oct 14, 2018
 */
public class GeodeStateStoreActor extends Actor implements ObjectStateStore, DispatcherControl {
  
  private static final NullState<Object> EMPTY_STATE = NullState.Object;
  
  private final List<Dispatchable<Object>> dispatchables;
  private final ObjectDispatcher dispatcher;
  private final Configuration configuration;
  private final GemFireCache cache;

  public GeodeStateStoreActor(final ObjectDispatcher aDispatcher, final Configuration aConfiguration) {
    this.dispatchables = new ArrayList<>();

    if (aDispatcher == null) {
      throw new IllegalArgumentException("ObjectDispatcher must not be null.");
    }
    this.dispatcher = aDispatcher;
    
    if (aConfiguration == null) {
      throw new IllegalArgumentException("Configuration must not be null.");
    }
    this.configuration = aConfiguration;
    
    this.cache = GemFireCacheProvider.getAnyInstance(aConfiguration);

    dispatcher.controlWith(selfAs(DispatcherControl.class));
  }

  @Override
  public void confirmDispatched(final String dispatchId, final ConfirmDispatchedResultInterest interest) {
    dispatchables.remove(new Dispatchable<byte[]>(dispatchId, null));
    interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
  }

  @Override
  public void dispatchUnconfirmed() {
    for (int idx = 0; idx < dispatchables.size(); ++idx) {
      final Dispatchable<Object> dispatchable = dispatchables.get(idx);
      dispatch(dispatchable.id, dispatchable.state);
    }
  }

  protected void dispatch(final String dispatchId, final State<Object> state) {
    dispatcher.dispatchObject(dispatchId, state);
  }

  /*
   * @see io.vlingo.symbio.store.state.ObjectStateStore#read(java.lang.String,
   * java.lang.Class, io.vlingo.symbio.store.state.StateStore.ReadResultInterest)
   */
  @Override
  public void read(String id, Class<?> type, ReadResultInterest<Object> interest) {
    readFor(id, type, interest, null);
  }

  /*
   * @see io.vlingo.symbio.store.state.ObjectStateStore#read(java.lang.String,
   * java.lang.Class, io.vlingo.symbio.store.state.StateStore.ReadResultInterest,
   * java.lang.Object)
   */
  @Override
  public void read(String id, Class<?> type, ReadResultInterest<Object> interest, Object object) {
    readFor(id, type, interest, object);
  }

  protected void readFor(final String id, final Class<?> type, final ReadResultInterest<Object> interest, final Object object) {
    
    if (interest != null) {
      
      if (id == null || type == null) {
        interest.readResultedIn(
          Result.Error,
          new IllegalArgumentException(id == null ? "The id is null." : "The type is null."),
          id,
          EMPTY_STATE,
          object);
        return;
      }
      
      final String storeName = StateTypeStateStoreMap.storeNameFrom(type);
      logger().log("readFor - storeName: " + storeName);

      if (storeName == null) {
        interest.readResultedIn(
          Result.NoTypeStore,
          new IllegalStateException("No type store."),
          id,
          EMPTY_STATE,
          object);
        return;
      }

      final Region<Object, State<Object>> typeStore = cache.getRegion(storeName);
      logger().log("readFor - typeStore: " + typeStore);

      if (typeStore == null) {
        interest.readResultedIn(
          Result.NotFound,
          new IllegalStateException("Store not found: " + storeName),
          id,
          EMPTY_STATE, 
          object);
        return;
      }

      final State<Object> state = typeStore.get(id);
      logger().log("readFor - state: " + state);

      if (state != null) {
        interest.readResultedIn(Result.Success, id, state, object);
      } else {
        interest.readResultedIn(
          Result.NotFound,
          new IllegalStateException("Not found."),
          id,
          EMPTY_STATE,
          object);
      }
    } else {
      logger().log(getClass().getSimpleName() + " readFor() missing ReadResultInterest for: " + (id == null ? "unknown id" : id));
    }
  }

  /*
   * @see
   * io.vlingo.symbio.store.state.ObjectStateStore#write(io.vlingo.symbio.State,
   * io.vlingo.symbio.store.state.StateStore.WriteResultInterest)
   */
  @Override
  public void write(State<Object> state, WriteResultInterest<Object> interest) {
    writeWith(state, interest, null);
  }

  /*
   * @see
   * io.vlingo.symbio.store.state.ObjectStateStore#write(io.vlingo.symbio.State,
   * io.vlingo.symbio.store.state.StateStore.WriteResultInterest,
   * java.lang.Object)
   */
  @Override
  public void write(State<Object> state, WriteResultInterest<Object> interest, Object object) {
    writeWith(state, interest, object);
  }
  
  protected void writeWith(final State<Object> state, final WriteResultInterest<Object> interest, final Object object) {
    if (interest != null) {
      if (state == null) {
        interest.writeResultedIn(
          Result.Error,
          new IllegalArgumentException("The state is null."),
          null,
          EMPTY_STATE,
          object);
      } else {
        try {
          final String storeName = StateTypeStateStoreMap.storeNameFrom(state.type);
          logger().log("writeWith - storeName: " + storeName);

          if (storeName == null) {
            interest.writeResultedIn(
              Result.NoTypeStore,
              new IllegalStateException("Store not configured: " + storeName),
              state.id,
              state,
              object);
            return;
          }

          Region<Object, State<Object>> typeStore = cache.getRegion(storeName);
          logger().log("writeWith - typeStore: " + typeStore);

          if (typeStore == null) {
            interest.writeResultedIn(
                Result.NoTypeStore,
                new IllegalArgumentException("Store not found: " + storeName),
                state.id,
                state,
                object);
            return;
          }

          final State<Object> persistedState = typeStore.putIfAbsent(state.id, state);
          if (persistedState != null) {
            if (persistedState.dataVersion >= state.dataVersion) {
              interest.writeResultedIn(
                Result.ConcurrentyViolation,
                new IllegalArgumentException("Version conflict."),
                state.id,
                state,
                object);
              return;
            }
            typeStore.put(state.id, state);
          }
          
          final String dispatchId = storeName + ":" + state.id;
          dispatchables.add(new Dispatchable<Object>(dispatchId, state));
          dispatch(dispatchId, state);

          interest.writeResultedIn(Result.Success, state.id, state, object);
          
        } catch (Exception e) {
          logger().log(getClass().getSimpleName() + " writeWith() error because: " + e.getMessage(), e);
          interest.writeResultedIn(Result.Error, e, state.id, state, object);
        }
      }
    } else {
      logger().log(
        getClass().getSimpleName() +
        " writeWith() missing WriteResultInterest for: " +
        (state == null ? "unknown id" : state.id));
    }
  }
}
