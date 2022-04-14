// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.object.geode;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.common.Failure;
import io.vlingo.xoom.common.Success;
import io.vlingo.xoom.common.serialization.JsonSerialization;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.Source;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.EntryReader;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.common.geode.dispatch.GeodeDispatchable;
import io.vlingo.xoom.symbio.store.common.geode.dispatch.GeodeDispatcherControlDelegate;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl.DispatcherControlInstantiator;
import io.vlingo.xoom.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.xoom.symbio.store.object.ObjectStore;
import io.vlingo.xoom.symbio.store.object.ObjectStoreDelegate;
import io.vlingo.xoom.symbio.store.object.StateObject;
import io.vlingo.xoom.symbio.store.object.StateSources;
/**
 * GeodeObjectStoreActor is an {@link ObjectStore} that knows how to
 * read/write {@link StateObject} from/to Apache Geode.
 */
public class GeodeObjectStoreActor extends Actor implements ObjectStore {

  private static final long CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT = 1000L;
  private static final long CONFIRMATION_EXPIRATION_DEFAULT = 1000L;

  private boolean closed;
  private final String originatorId;

  private final DispatcherControl dispatcherControl;
  private final List<Dispatcher<GeodeDispatchable<State<?>>>> dispatchers;
  private final EntryAdapterProvider entryAdapterProvider;
  private final ObjectStoreDelegate<Entry<?>, State<?>> storeDelegate;

  public GeodeObjectStoreActor(final String originatorId, final GeodeObjectStoreDelegate storeDelegate,
                               final Dispatcher<GeodeDispatchable<State<?>>> dispatcher) {
    this(originatorId,
      storeDelegate,
      dispatcher,
      CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT,
      CONFIRMATION_EXPIRATION_DEFAULT);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public GeodeObjectStoreActor(final String originatorId,
                               final GeodeObjectStoreDelegate storeDelegate,
                               final List<Dispatcher<GeodeDispatchable<State<?>>>> dispatchers,
                               long checkConfirmationExpirationInterval,
                               final long confirmationExpiration) {
    this.originatorId = originatorId;
    this.storeDelegate = storeDelegate;

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());

    final GeodeDispatcherControlDelegate controlDelegate = new GeodeDispatcherControlDelegate(originatorId);
    this.dispatchers = dispatchers;
    this.dispatcherControl = stage().actorFor(
      DispatcherControl.class,
      Definition.has(
        DispatcherControlActor.class,
        new DispatcherControlInstantiator(dispatchers, controlDelegate, checkConfirmationExpirationInterval, confirmationExpiration))
    );
  }

  public GeodeObjectStoreActor(final String originatorId,
                               final GeodeObjectStoreDelegate storeDelegate,
                               final Dispatcher<GeodeDispatchable<State<?>>> dispatcher,
                               long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this(originatorId, storeDelegate, Arrays.asList(dispatcher), checkConfirmationExpirationInterval, confirmationExpiration);
  }

  @Override
  public void close() {
    if (dispatcherControl != null) {
      dispatcherControl.stop();
    }
    if (!closed) {
      closed = true;
    }
  }

  @Override
  public Completes<EntryReader<? extends Entry<?>>> entryReader(final String name) {
    // TODO: Dave, please see io.vlingo.xoom.symbio.store.object.jdbc.jpa.JPAObjectStoreActor#entryReader()
    return null;
  }

  @Override
  public <T extends StateObject, E> void persist(StateSources<T, E> stateSources, Metadata metadata, long updateId, PersistResultInterest interest, Object object) {
    final List<Source<E>> sources = stateSources.sources();
    final T objectToPersist = stateSources.stateObject();
    try {
      storeDelegate.beginTransaction();

      /* persist the entity */
      final State<?> raw = storeDelegate.persist(objectToPersist, updateId, metadata);

      /* persist the journal entries */
      final int entryVersion = (int) stateSources.stateObject().version();
      final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, entryVersion, metadata);
      storeDelegate.persistEntries(entries);

      /* persist the dispatchables */
      final GeodeDispatchable<State<?>> dispatchable = buildDispatchable(raw, entries);
      storeDelegate.persistDispatchable(dispatchable);

      storeDelegate.completeTransaction();

      dispatch(dispatchable);
      interest.persistResultedIn(Success.of(Result.Success), objectToPersist, 1, 1, object);
    }
    catch (final StorageException ex) {
      logger().error("error persisting " + JsonSerialization.serialized(objectToPersist), ex);
      storeDelegate.failTransaction();
      interest.persistResultedIn(Failure.of(ex), objectToPersist, 1, 0, object);
    }
    catch (Exception ex) {
      logger().error("error persisting " + JsonSerialization.serialized(objectToPersist), ex);
      storeDelegate.failTransaction();
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), objectToPersist, 1, 0, object);
    }
  }

  @Override
  public <T extends StateObject, E> void persistAll(Collection<StateSources<T, E>> allStateSources, Metadata metadata, long updateId, PersistResultInterest interest, Object object) {
    final Collection<T> allObjectsToPersist = new ArrayList<>();
    final List<GeodeDispatchable<State<?>>> allDispatchables = new ArrayList<>(allStateSources.size());
    try {
      storeDelegate.beginTransaction();
      for (StateSources<T,E> stateSources : allStateSources) {
        final T objectToPersist = stateSources.stateObject();
        final List<Source<E>> sources = stateSources.sources();

        final int entryVersion = (int) stateSources.stateObject().version();
        final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, entryVersion, metadata);
        storeDelegate.persistEntries(entries);

        final State<?> state = storeDelegate.persist(objectToPersist, updateId, metadata);
        allObjectsToPersist.add(objectToPersist);

        final GeodeDispatchable<State<?>> dispatchable = buildDispatchable(state, entries);
        storeDelegate.persistDispatchable(dispatchable);
        allDispatchables.add(dispatchable);
      }
      storeDelegate.completeTransaction();

      allDispatchables.forEach(this::dispatch);
      interest.persistResultedIn(Success.of(Result.Success), allObjectsToPersist, allObjectsToPersist.size(), allObjectsToPersist.size(), object);
    }
    catch (final StorageException ex) {
      logger().error("error persisting " + JsonSerialization.serialized(allObjectsToPersist), ex);
      storeDelegate.failTransaction();
      interest.persistResultedIn(Failure.of(ex), allObjectsToPersist, 1, 0, object);
    }
    catch (Exception ex) {
      logger().error("error persisting " + JsonSerialization.serialized(allObjectsToPersist), ex);
      storeDelegate.failTransaction();
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), allObjectsToPersist, allObjectsToPersist.size(), 0, object);
    }
  }

  /* @see io.vlingo.xoom.symbio.store.object.ObjectStore#queryAll(io.vlingo.xoom.symbio.store.object.QueryExpression, io.vlingo.xoom.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object) */
  @Override
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    if (expression.isMapQueryExpression()) {
      throw new UnsupportedOperationException("MapQueryExpression is not supported by this object store");
    }

    try {
      final QueryMultiResults results = storeDelegate.queryAll(expression);
      interest.queryAllResultedIn(Success.of(Result.Success), results, object);
    } catch (final StorageException e) {
      logger().error("Query all failed because: " + e.getMessage(), e);
      interest.queryAllResultedIn(Failure.of(e), QueryMultiResults.of(null), object);
    } catch (Exception ex) {
      interest.queryAllResultedIn(
        Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)),
        QueryMultiResults.of(Collections.EMPTY_LIST),
        object);
    }
  }

  /* @see io.vlingo.xoom.symbio.store.object.ObjectStore#queryObject(io.vlingo.xoom.symbio.store.object.QueryExpression, io.vlingo.xoom.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object) */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {

    if (expression.isMapQueryExpression()) {
      throw new UnsupportedOperationException("MapQueryExpression is not supported by this object store");
    }

    try {
      final QuerySingleResult singleResult = storeDelegate.queryObject(expression);
      interest.queryObjectResultedIn(Success.of(Result.Success), singleResult, object);
    } catch (final StorageException e) {
      logger().error("Query failed because: " + e.getMessage(), e);
      interest.queryAllResultedIn(Failure.of(e), QueryMultiResults.of(null), object);
    } catch (Exception ex) {
      interest.queryObjectResultedIn(
        Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)),
        QuerySingleResult.of(null),
        object);
    }
  }

  private void dispatch(final GeodeDispatchable<State<?>> geodeDispatchable) {
    this.dispatchers.forEach(d -> d.dispatch(geodeDispatchable));
  }

  private GeodeDispatchable<State<?>> buildDispatchable(final State<?> state, final List<Entry<?>> entries) {
    final String id = getDispatchId(state, entries);
    return new GeodeDispatchable<>(originatorId, LocalDateTime.now(), id, state, entries);
  }

  private static String getDispatchId(final State<?> raw, final List<Entry<?>> entries) {
    return raw.id + ":" + entries.stream().map(Entry::id).collect(Collectors.joining(":"));
  }

  public static class GeodeObjectStoreInstantiator implements ActorInstantiator<GeodeObjectStoreActor> {
    private static final long serialVersionUID = -3188258223382398545L;

    private final String originatorId;
    private final GeodeObjectStoreDelegate delegate;
    private final Dispatcher<GeodeDispatchable<State<?>>> dispatcher;

    public GeodeObjectStoreInstantiator(
            final String originatorId,
            final GeodeObjectStoreDelegate delegate,
            final Dispatcher<GeodeDispatchable<State<?>>> dispatcher) {
      this.originatorId = originatorId;
      this.dispatcher = dispatcher;
      this.delegate = delegate;
    }

    @Override
    public GeodeObjectStoreActor instantiate() {
      return new GeodeObjectStoreActor(originatorId, delegate, dispatcher);
    }

    @Override
    public Class<GeodeObjectStoreActor> type() {
      return GeodeObjectStoreActor.class;
    }
  }
}
