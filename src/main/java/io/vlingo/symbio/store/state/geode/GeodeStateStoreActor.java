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
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.ObjectState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.ObjectStateStore;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
/**
 * GeodeStateStoreActor is responsible for reading and writing
 * objects from/to a GemFire cache.
 */
public class GeodeStateStoreActor extends Actor implements ObjectStateStore, DispatcherControl {
  
  private static final ObjectState<Object> EMPTY_STATE = ObjectState.Null;
  
  private final List<Dispatchable<ObjectState<Object>>> dispatchables;
  private final ObjectDispatcher dispatcher;
  //private final Configuration configuration;
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
    //this.configuration = aConfiguration;
    
    this.cache = GemFireCacheProvider.getAnyInstance(aConfiguration);

    dispatcher.controlWith(selfAs(DispatcherControl.class));
  }

  @Override
  public void confirmDispatched(final String dispatchId, final ConfirmDispatchedResultInterest interest) {
    dispatchables.remove(new Dispatchable<ObjectState<Object>>(dispatchId, null));
    interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
  }

  @Override
  public void dispatchUnconfirmed() {
    for (int idx = 0; idx < dispatchables.size(); ++idx) {
      final Dispatchable<ObjectState<Object>> dispatchable = dispatchables.get(idx);
      dispatch(dispatchable.id, dispatchable.state);
    }
  }

  protected void dispatch(final String dispatchId, final ObjectState<Object> state) {
    dispatcher.dispatchObject(dispatchId, state);
  }

  /*
   * @see io.vlingo.symbio.store.state.ObjectStateStore#read(java.lang.String,
   * java.lang.Class, io.vlingo.symbio.store.state.StateStore.ReadResultInterest)
   */
  @Override
  public void read(String id, Class<?> type, ReadResultInterest<ObjectState<Object>> interest) {
    readFor(id, type, interest, null);
  }

  /*
   * @see io.vlingo.symbio.store.state.ObjectStateStore#read(java.lang.String,
   * java.lang.Class, io.vlingo.symbio.store.state.StateStore.ReadResultInterest,
   * java.lang.Object)
   */
  @Override
  public void read(String id, Class<?> type, ReadResultInterest<ObjectState<Object>> interest, Object object) {
    readFor(id, type, interest, object);
  }

  protected void readFor(final String id, final Class<?> type, final ReadResultInterest<ObjectState<Object>> interest, final Object object) {
    
    if (interest != null) {
      
      if (id == null || type == null) {
        interest.readResultedIn(
          Failure.of(new StorageException(Result.Error, id == null ? "The id is null." : "The type is null.")),
          id,
          EMPTY_STATE,
          object);
        return;
      }
      
      final String storeName = StateTypeStateStoreMap.storeNameFrom(type);
      logger().log("readFor - storeName: " + storeName);

      if (storeName == null) {
        interest.readResultedIn(
          Failure.of(new StorageException(Result.NoTypeStore, "No type store.")),
          id,
          EMPTY_STATE,
          object);
        return;
      }

      final Region<Object, ObjectState<Object>> typeStore = cache.getRegion(storeName);
      logger().log("readFor - typeStore: " + typeStore);

      if (typeStore == null) {
        interest.readResultedIn(
          Failure.of(new StorageException(Result.NotFound, "Store not found: " + storeName)),
          id,
          EMPTY_STATE, 
          object);
        return;
      }

      final ObjectState<Object> state = typeStore.get(id);
      logger().log("readFor - state: " + state);

      if (state != null) {
        interest.readResultedIn(Success.of(Result.Success), id, state, object);
      } else {
        interest.readResultedIn(
          Failure.of(new StorageException(Result.NotFound, "Not found.")),
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
  public void write(ObjectState<Object> state, WriteResultInterest<ObjectState<Object>> interest) {
    writeWith(state, interest, null);
  }

  /*
   * @see
   * io.vlingo.symbio.store.state.ObjectStateStore#write(io.vlingo.symbio.State,
   * io.vlingo.symbio.store.state.StateStore.WriteResultInterest,
   * java.lang.Object)
   */
  @Override
  public void write(ObjectState<Object> state, WriteResultInterest<ObjectState<Object>> interest, Object object) {
    writeWith(state, interest, object);
  }
  
  protected void writeWith(final ObjectState<Object> state, final WriteResultInterest<ObjectState<Object>> interest, final Object object) {
    if (interest != null) {
      if (state == null) {
        interest.writeResultedIn(
          Failure.of(new StorageException(Result.Error, "The state is null.")),
          null,
          EMPTY_STATE,
          object);
      } else {
        try {
          final String storeName = StateTypeStateStoreMap.storeNameFrom(state.type);
          logger().log("writeWith - storeName: " + storeName);

          if (storeName == null) {
            interest.writeResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "Store not configured: " + storeName)),
              state.id,
              state,
              object);
            return;
          }

          Region<Object, State<Object>> typeStore = cache.getRegion(storeName);
          logger().log("writeWith - typeStore: " + typeStore);

          if (typeStore == null) {
            interest.writeResultedIn(
                Failure.of(new StorageException(Result.NoTypeStore, "Store not found: " + storeName)),
                state.id,
                state,
                object);
            return;
          }

          final State<Object> persistedState = typeStore.putIfAbsent(state.id, state);
          if (persistedState != null) {
            if (persistedState.dataVersion >= state.dataVersion) {
              interest.writeResultedIn(
                Failure.of(new StorageException(Result.ConcurrentyViolation, "Version conflict.")),
                state.id,
                state,
                object);
              return;
            }
            typeStore.put(state.id, state);
          }
          
          final String dispatchId = storeName + ":" + state.id;
          dispatchables.add(new Dispatchable<>(dispatchId, state));
          dispatch(dispatchId, state);

          interest.writeResultedIn(Success.of(Result.Success), state.id, state, object);
          
        } catch (Exception e) {
          logger().log(getClass().getSimpleName() + " writeWith() error because: " + e.getMessage(), e);
          interest.writeResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), state.id, state, object);
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
