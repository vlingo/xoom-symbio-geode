// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.actors.Protocols;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.ObjectState;
import io.vlingo.symbio.StateAdapter;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.GeodeDispatchableSerializer;
import io.vlingo.symbio.store.state.StatePdxSerializerRegistry;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStoreAdapterAssistant;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
/**
 * GeodeStateStoreActor is responsible for reading and writing
 * objects from/to a GemFire cache.
 */
public class GeodeStateStoreActor extends Actor implements StateStore {
    
  private final StateStoreAdapterAssistant adapterAssistant;
  private final Dispatcher dispatcher;
  private final GemFireCache cache;
  private final RedispatchControl redispatchControl;

  public GeodeStateStoreActor(final Dispatcher dispatcher, final Configuration configuration) {
    this(dispatcher, configuration, 1000L, 1000L);
  }
  
  public GeodeStateStoreActor(final Dispatcher dispatcher, final Configuration configuration, long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    if (dispatcher == null) {
      throw new IllegalArgumentException("Dispatcher must not be null.");
    }
    this.dispatcher = dispatcher;
    
    if (configuration == null) {
      throw new IllegalArgumentException("Configuration must not be null.");
    }
    
    this.adapterAssistant = new StateStoreAdapterAssistant();
    this.cache = GemFireCacheProvider.getAnyInstance(configuration);
    
    StatePdxSerializerRegistry.serializeTypeWith(GeodeDispatchable.class, GeodeDispatchableSerializer.class);

    Protocols protocols = stage().actorFor(
      new Class[] { DispatcherControl.class, RedispatchControl.class },
      Definition.has(GeodeRedispatchControlActor.class, Definition.parameters(dispatcher, cache, checkConfirmationExpirationInterval, confirmationExpiration))
    );
    final DispatcherControl control = protocols.get(0);
    redispatchControl = protocols.get(1);

    dispatcher.controlWith(control);
    control.dispatchUnconfirmed();
  }


  /* @see io.vlingo.actors.Actor#afterStop() */
  @Override
  protected void afterStop() {
    if (redispatchControl != null) {
      redispatchControl.stop();
    }
  }

//  @Override
//  public void confirmDispatched(final String dispatchId, final ConfirmDispatchedResultInterest interest) {
//    dispatchables.remove(new Dispatchable<ObjectState<Object>>(dispatchId, null));
//    interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
//  }
//
//  @Override
//  public void dispatchUnconfirmed() {
//    for (int idx = 0; idx < dispatchables.size(); ++idx) {
//      final Dispatchable<ObjectState<Object>> dispatchable = dispatchables.get(idx);
//      dispatch(dispatchable.id, dispatchable.state);
//    }
//  }

  protected void dispatch(final String dispatchId, final ObjectState<Object> state) {
    dispatcher.dispatch(dispatchId, state);
  }

  /*
   * @see io.vlingo.symbio.store.state.StateStore#read(java.lang.String, java.lang.Class, io.vlingo.symbio.store.state.StateStore.ReadResultInterest)
   */
  public void read(String id, Class<?> type, ReadResultInterest interest) {
    readFor(id, type, interest, null);
  }

  /*
   * @see io.vlingo.symbio.store.state.StateStore#read(java.lang.String, java.lang.Class, io.vlingo.symbio.store.state.StateStore.ReadResultInterest, java.lang.Object)
   */
  @Override
  public void read(String id, Class<?> type, ReadResultInterest interest, Object object) {
    readFor(id, type, interest, object);
  }

  protected void readFor(final String id, final Class<?> type, final ReadResultInterest interest, final Object object) {
    
    if (interest != null) {
      
      if (id == null || type == null) {
        interest.readResultedIn(
          Failure.of(new StorageException(Result.Error, id == null ? "The id is null." : "The type is null.")),
          id,
          null,
          -1,
          null,
          object);
        return;
      }
      
      final String storeName = StateTypeStateStoreMap.storeNameFrom(type);
      if (storeName == null) {
        interest.readResultedIn(
          Failure.of(new StorageException(Result.NoTypeStore, "No type store.")),
          id,
          null,
          -1,
          null,
          object);
        return;
      }

      final Region<Object, ObjectState<Object>> typeStore = cache.getRegion(storeName);
      if (typeStore == null) {
        interest.readResultedIn(
          Failure.of(new StorageException(Result.NotFound, "Store not found: " + storeName)),
          id,
          null,
          -1,
          null,
          object);
        return;
      }

      final ObjectState<Object> raw = typeStore.get(id);
      if (raw != null) {
        final Object state = adapterAssistant.adaptFromRawState(raw);
        interest.readResultedIn(Success.of(Result.Success), id, state, raw.dataVersion, raw.metadata, object);
      } 
      else {
        interest.readResultedIn(
          Failure.of(new StorageException(Result.NotFound, "Not found.")),
          id,
          null,
          -1,
          null,
          object);
      }
    } 
    else {
      logger().log(getClass().getSimpleName() + " readFor() missing ReadResultInterest for: " + (id == null ? "unknown id" : id));
    }
  }

  /*
   * @see io.vlingo.symbio.store.state.StateStore#write(java.lang.String, java.lang.Object, int, io.vlingo.symbio.store.state.StateStore.WriteResultInterest)
   */
  @Override
  public <S> void write(final String id, final S state, final int stateVersion, final WriteResultInterest interest) {
    writeWith(id, state, stateVersion, null, interest, null);
  }

  /*
   * @see io.vlingo.symbio.store.state.StateStore#write(java.lang.String, java.lang.Object, int, io.vlingo.symbio.Metadata, io.vlingo.symbio.store.state.StateStore.WriteResultInterest)
   */
  @Override
  public <S> void write(final String id, final S state, final int stateVersion, final Metadata metadata, final WriteResultInterest interest) {
    writeWith(id, state, stateVersion, metadata, interest, null);
  }

  /*
   * @see io.vlingo.symbio.store.state.StateStore#write(java.lang.String, java.lang.Object, int, io.vlingo.symbio.store.state.StateStore.WriteResultInterest, java.lang.Object)
   */
  @Override
  public <S> void write(final String id, final S state, final int stateVersion, final WriteResultInterest interest, final Object object) {
    writeWith(id, state, stateVersion, null, interest, object);
  }

  @Override
  public <S> void write(final String id, final S state, final int stateVersion, final Metadata metadata, final WriteResultInterest interest, final Object object) {
    writeWith(id, state, stateVersion, metadata, interest, object);
  }

  @Override
  public <S, R extends State<?>> void registerAdapter(final Class<S> stateType, final StateAdapter<S, R> adapter) {
    adapterAssistant.registerAdapter(stateType, adapter);
  }

  protected <S> void writeWith(final String id, final S state, final int stateVersion, final Metadata metadata, final WriteResultInterest interest, final Object object) {
        
    if (interest == null) {
      logger().log(
        getClass().getSimpleName() + 
        " writeWith() missing WriteResultInterest for: " +
        (state == null ? "unknown id" : id));
      return;
    }
    
    if (state == null) {
      interest.writeResultedIn(
        Failure.of(new StorageException(Result.Error, "The state is null.")),
        id,
        state,
        stateVersion,
        object);
      return;
    }
    
    final String storeName = StateTypeStateStoreMap.storeNameFrom(state.getClass());
    if (storeName == null) {
      interest.writeResultedIn(
        Failure.of(new StorageException(Result.NoTypeStore, "Store not configured: " + storeName)),
        id,
        state,
        stateVersion,
        object);
      return;
    }

    Region<Object, State<Object>> typeStore = cache.getRegion(storeName);
    if (typeStore == null) {
      interest.writeResultedIn(
          Failure.of(new StorageException(Result.NoTypeStore, "Store not found: " + storeName)),
          id,
          state,
          stateVersion,
          object);
      return;
    }
    
    final ObjectState<Object> raw = (metadata == null)
      ? adapterAssistant.adaptToRawState(state, stateVersion)
      : adapterAssistant.adaptToRawState(state, stateVersion, metadata);
    
    try {
      final State<Object> persistedState = typeStore.putIfAbsent(id, raw);
      if (persistedState != null) {
        if (persistedState.dataVersion >= raw.dataVersion) {
          interest.writeResultedIn(
            Failure.of(new StorageException(Result.ConcurrentyViolation, "Version conflict.")),
            id,
            state,
            stateVersion,
            object);
          return;
        }
        typeStore.put(id, raw);
      }
      //System.out.println("wrote " + id + " to " + typeStore.getName());
      final long writeTimestamp = System.currentTimeMillis();

      
      final String dispatchId = storeName + ":" + id;
      
      Region<String, GeodeDispatchable<ObjectState<Object>>> dispatchablesRegion =
        cache.getRegion(GeodeQueries.DISPATCHABLES_REGION_NAME);
      dispatchablesRegion.put(dispatchId, new GeodeDispatchable<>(writeTimestamp, dispatchId, raw));
      //System.out.println("wrote " + dispatchId + " to " + GeodeQueries.DISPATCHABLES_REGION_NAME);
      
      dispatch(dispatchId, raw);
      
      interest.writeResultedIn(Success.of(Result.Success), id, state, stateVersion, object);
    }
    catch (Exception e) {
      logger().log(getClass().getSimpleName() + " writeWith() error because: " + e.getMessage(), e);
      interest.writeResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), id, state, stateVersion, object);
    }
  }
}
