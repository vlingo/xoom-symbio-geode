// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.ObjectState;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.Configuration;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.pdx.PdxSerializerRegistry;
import io.vlingo.symbio.store.state.GeodeDispatchableSerializer;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStoreEntryReader;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * GeodeStateStoreActor is responsible for reading and writing
 * objects from/to a GemFire cache.
 */
public class GeodeStateStoreActor extends Actor implements StateStore {

  public static final long CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT = 1000L;
  public static final long CONFIRMATION_EXPIRATION_DEFAULT = 1000L;

  private final String originatorId;
  private final Dispatcher dispatcher;
  private final DispatcherControl dispatcherControl;
  private final GemFireCache cache;
  private final Configuration configuration;
  private final Map<String,StateStoreEntryReader<?>> entryReaders;
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;

  public GeodeStateStoreActor(final String originatorId, final Dispatcher dispatcher, final Configuration configuration) {
    this(
      originatorId,
      dispatcher,
      configuration,
      CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT,
      CONFIRMATION_EXPIRATION_DEFAULT);
  }

  public GeodeStateStoreActor(final String originatorId, final Dispatcher dispatcher, final Configuration configuration, long checkConfirmationExpirationInterval, final long confirmationExpiration) {

    if (originatorId == null)
      throw new IllegalArgumentException("originatorId must not be null.");
    this.originatorId = originatorId;

    if (dispatcher == null)
      throw new IllegalArgumentException("dispatcher must not be null.");
    this.dispatcher = dispatcher;

    if (configuration == null)
      throw new IllegalArgumentException("configuration must not be null.");
    this.cache = GemFireCacheProvider.getAnyInstance(configuration);

    this.configuration = configuration;

    this.entryReaders = new HashMap<>();

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());

    PdxSerializerRegistry.serializeTypeWith(GeodeDispatchable.class, GeodeDispatchableSerializer.class);

    dispatcherControl = stage().actorFor(
      DispatcherControl.class,
      Definition.has(GeodeDispatcherControlActor.class, Definition.parameters(originatorId, dispatcher, cache, checkConfirmationExpirationInterval, confirmationExpiration))
    );

    dispatcher.controlWith(dispatcherControl);
    dispatcherControl.dispatchUnconfirmed();
  }

  @Override
  public void stop() {
    if (dispatcherControl != null) {
      dispatcherControl.stop();
    }
    super.stop();
  }

  protected void dispatch(final String dispatchId, final ObjectState<Object> state) {
    dispatcher.dispatch(dispatchId, state);
  }

  /*
   * @see io.vlingo.symbio.store.state.StateStore#entryReader(java.lang.String)
   */
  @Override
  @SuppressWarnings("unchecked")
  public <ET extends Entry<?>> Completes<StateStoreEntryReader<ET>> entryReader(final String name) {
    StateStoreEntryReader<?> reader = entryReaders.get(name);
    if (reader == null) {
      final EntryReader.Advice advice =
              new EntryReader.Advice(configuration, GeodeStateStoreEntryReaderActor.class,  null, null);
      reader = childActorFor(StateStoreEntryReader.class, Definition.has(advice.entryReaderClass, Definition.parameters(advice, name)));
      entryReaders.put(name, reader);
    }
    return completes().with((StateStoreEntryReader<ET>) reader);
  }

  /*
   * @see io.vlingo.symbio.store.state.StateStore#read(java.lang.String, java.lang.Class, io.vlingo.symbio.store.state.StateStore.ReadResultInterest, java.lang.Object)
   */
  @Override
  public void read(final String id, final Class<?> type, final ReadResultInterest interest, final Object object) {
    readFor(id, type, interest, object);
  }

  private void readFor(final String id, final Class<?> type, final ReadResultInterest interest, final Object object) {

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
        final Object state = stateAdapterProvider.fromRaw(raw);
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
      logger().warn(getClass().getSimpleName() + " readFor() missing ReadResultInterest for: " + (id == null ? "unknown id" : id));
    }
  }

  @Override
  public <S,C> void write(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata, final WriteResultInterest interest, final Object object) {
    writeWith(id, state, stateVersion, sources, metadata, interest, object);
  }

  private <S,C> void writeWith(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata, final WriteResultInterest interest, final Object object) {

    if (interest == null) {
      logger().warn(
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
        sources,
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
        sources,
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
          sources,
          object);
      return;
    }

    final ObjectState<Object> raw = (metadata == null)
      ? stateAdapterProvider.asRaw(id, state, stateVersion)
      : stateAdapterProvider.asRaw(id, state, stateVersion, metadata);

    // TODO: Write sources
    entryAdapterProvider.asEntries(sources); // final List<Entry<?>> entries =

    try {
      final State<Object> persistedState = typeStore.putIfAbsent(id, raw);
      if (persistedState != null) {
        if (persistedState.dataVersion >= raw.dataVersion) {
          interest.writeResultedIn(
            Failure.of(new StorageException(Result.ConcurrentyViolation, "Version conflict.")),
            id,
            state,
            stateVersion,
            sources,
            object);
          return;
        }
        typeStore.put(id, raw);
      }
      final String dispatchId = storeName + ":" + id;

      Region<String, GeodeDispatchable<ObjectState<Object>>> dispatchablesRegion =
        cache.getRegion(GeodeQueries.DISPATCHABLES_REGION_NAME);
      dispatchablesRegion.put(dispatchId, new GeodeDispatchable<>(originatorId, LocalDateTime.now(), dispatchId, raw));

      dispatch(dispatchId, raw);

      interest.writeResultedIn(Success.of(Result.Success), id, state, stateVersion, sources, object);
    }
    catch (Exception e) {
      logger().error(getClass().getSimpleName() + " writeWith() error because: " + e.getMessage(), e);
      interest.writeResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), id, state, stateVersion, sources, object);
    }
  }
}
