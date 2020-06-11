// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;

import io.vlingo.actors.Actor;
import io.vlingo.actors.ActorInstantiator;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Outcome;
import io.vlingo.common.Success;
import io.vlingo.reactivestreams.Stream;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.ObjectState;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.QueryExpression;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatchable;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatcherControlDelegate;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.DispatcherControl.DispatcherControlInstantiator;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStoreEntryReader;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.geode.GeodeStateStoreEntryReaderActor.GeodeStateStoreEntryReaderInstantiator;
/**
 * GeodeStateStoreActor is responsible for reading and writing
 * objects from/to a GemFire cache.
 */
public class GeodeStateStoreActor extends Actor implements StateStore {

  public static final long CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT = 1000L;
  public static final long CONFIRMATION_EXPIRATION_DEFAULT = 1000L;

  private final String originatorId;
  private final List<Dispatcher<GeodeDispatchable<ObjectState<Object>>>> dispatchers;
  private final DispatcherControl dispatcherControl;
  private final Map<String,StateStoreEntryReader<?>> entryReaders;
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;
  private final ReadAllResultCollector readAllResultCollector;

  public GeodeStateStoreActor(final String originatorId, final Dispatcher<GeodeDispatchable<ObjectState<Object>>> dispatcher) {
    this(originatorId,
            dispatcher,
            CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT,
            CONFIRMATION_EXPIRATION_DEFAULT);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public GeodeStateStoreActor(
          final String originatorId,
          final List<Dispatcher<GeodeDispatchable<ObjectState<Object>>>> dispatchers,
          long checkConfirmationExpirationInterval,
          final long confirmationExpiration) {

    if (originatorId == null)
      throw new IllegalArgumentException("originatorId must not be null.");
    this.originatorId = originatorId;

    if (dispatchers == null)
      throw new IllegalArgumentException("dispatcher must not be null.");
    this.dispatchers = dispatchers;

    this.entryReaders = new HashMap<>();

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    this.readAllResultCollector = new ReadAllResultCollector();

    final GeodeDispatcherControlDelegate controlDelegate = new GeodeDispatcherControlDelegate(originatorId);
    dispatcherControl = stage().actorFor(
      DispatcherControl.class,
      Definition.has(
        DispatcherControlActor.class,
        new DispatcherControlInstantiator(dispatchers, controlDelegate, checkConfirmationExpirationInterval, confirmationExpiration))
    );
  }

  public GeodeStateStoreActor(
          final String originatorId,
          final Dispatcher<GeodeDispatchable<ObjectState<Object>>> dispatcher,
          long checkConfirmationExpirationInterval,
          final long confirmationExpiration) {
    this(originatorId, Arrays.asList(dispatcher), checkConfirmationExpirationInterval, confirmationExpiration);
  }

  @Override
  public void stop() {
    if (dispatcherControl != null) {
      dispatcherControl.stop();
    }
    super.stop();
  }

  protected void dispatch(final String dispatchId, final ObjectState<Object> state, final List<Entry<?>> entries) {
    dispatchers.forEach(d -> d.dispatch(new GeodeDispatchable<>(originatorId, LocalDateTime.now(), dispatchId, state, entries)));
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
              new EntryReader.Advice(null, GeodeStateStoreEntryReaderActor.class,  null, null, null, null, null, null);
      reader = childActorFor(StateStoreEntryReader.class, Definition.has(advice.entryReaderClass, new GeodeStateStoreEntryReaderInstantiator<>(advice, name)));
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

  @Override
  public void readAll(final Collection<TypedStateBundle> bundles, final ReadResultInterest interest, final Object object) {
    readAllResultCollector.prepare();

    for (final TypedStateBundle bundle : bundles) {
      readFor(bundle.id, bundle.type, readAllResultCollector, null);
    }

    final Outcome<StorageException, Result> outcome = readAllResultCollector.readResultOutcome(bundles.size());

    interest.readResultedIn(outcome, readAllResultCollector.readResultBundles(), object);
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

      final Region<Object, ObjectState<Object>> typeStore = cache().getRegion(storeName);
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

  @Override
  public Completes<Stream> streamAllOf(final Class<?> stateType) {
    // TODO: Implement
    return null;
  }

  @Override
  public Completes<Stream> streamSomeUsing(final QueryExpression query) {
    // TODO: Implement
    return null;
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

    Region<Object, State<Object>> typeStore = cache().getRegion(storeName);
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
    final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, stateVersion, metadata);// final List<Entry<?>> entries =

    try {
      final State<Object> persistedState = typeStore.putIfAbsent(id, raw);
      if (persistedState != null) {
        if (persistedState.dataVersion >= raw.dataVersion) {
          interest.writeResultedIn(
            Failure.of(new StorageException(Result.ConcurrencyViolation, "Version conflict.")),
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
        cache().getRegion(GeodeQueries.DISPATCHABLES_REGION_PATH);
      dispatchablesRegion.put(dispatchId, new GeodeDispatchable<>(originatorId, LocalDateTime.now(), dispatchId, raw, entries));

      dispatch(dispatchId, raw, entries);

      interest.writeResultedIn(Success.of(Result.Success), id, state, stateVersion, sources, object);
    }
    catch (Exception e) {
      logger().error(getClass().getSimpleName() + " writeWith() error because: " + e.getMessage(), e);
      interest.writeResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), id, state, stateVersion, sources, object);
    }
  }

  private GemFireCache cache() {
    Optional<GemFireCache> cacheOrNull = GemFireCacheProvider.getAnyInstance();
    if (cacheOrNull.isPresent()) {
      return cacheOrNull.get();
    }
    else {
      throw new RuntimeException("no GemFireCache has been created in this JVM");
    }
  }

  public static class GeodeStateStoreInstantiator implements ActorInstantiator<GeodeStateStoreActor> {
    private static final long serialVersionUID = 950661758247574005L;

    private final String originatorId;
    private final List<Dispatcher<GeodeDispatchable<ObjectState<Object>>>> dispatchers;
    private final long checkConfirmationExpirationInterval;
    private final long confirmationExpiration;

    public GeodeStateStoreInstantiator(
            final String originatorId,
            final List<Dispatcher<GeodeDispatchable<ObjectState<Object>>>> dispatchers,
            final long checkConfirmationExpirationInterval,
            final long confirmationExpiration) {
      this.originatorId = originatorId;
      this.dispatchers = dispatchers;
      this.checkConfirmationExpirationInterval = checkConfirmationExpirationInterval;
      this.confirmationExpiration = confirmationExpiration;
    }

    @Override
    public GeodeStateStoreActor instantiate() {
      return new GeodeStateStoreActor(originatorId, dispatchers, checkConfirmationExpirationInterval, confirmationExpiration);
    }

    @Override
    public Class<GeodeStateStoreActor> type() {
      return GeodeStateStoreActor.class;
    }
  }
}
