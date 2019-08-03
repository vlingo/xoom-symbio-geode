// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.actors.Logger;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.*;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatchable;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatcherControlDelegate;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.ObjectStoreDelegate;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.QueryExpression;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * GeodeObjectStoreActor is an {@link ObjectStore} that knows how to
 * read/write {@link PersistentObject} from/to Apache Geode.
 */
public class GeodeObjectStoreActor extends Actor implements ObjectStore {

  private static final Logger LOG = Logger.basicLogger();
  private static final long CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT = 1000L;
  private static final long CONFIRMATION_EXPIRATION_DEFAULT = 1000L;

  private boolean closed;
  private final String originatorId;

  private final DispatcherControl dispatcherControl;
  private final Dispatcher<GeodeDispatchable<State<?>>> dispatcher;
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

  public GeodeObjectStoreActor(final String originatorId,
                               final GeodeObjectStoreDelegate storeDelegate,
                               final Dispatcher<GeodeDispatchable<State<?>>> dispatcher,
                               long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.originatorId = originatorId;
    this.storeDelegate = storeDelegate;

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());

    final GeodeDispatcherControlDelegate controlDelegate = new GeodeDispatcherControlDelegate(originatorId);
    this.dispatcher = dispatcher;
    this.dispatcherControl = stage().actorFor(
      DispatcherControl.class,
      Definition.has(
        DispatcherControlActor.class,
        Definition.parameters(dispatcher, controlDelegate, checkConfirmationExpirationInterval, confirmationExpiration))
    );
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
  public <T extends PersistentObject, E> void persist(final T objectToPersist, final List<Source<E>> sources, final Metadata metadata, final long updateId,
                                                      final PersistResultInterest interest, final Object object) {
    try {
      storeDelegate.beginTransaction();

      /* persist the entity */
      final State<?> raw = storeDelegate.persist(objectToPersist, updateId, metadata);

      /* persist the journal entries */
      final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, metadata);
      storeDelegate.persistEntries(entries);

      /* persist the dispatchables */
      final GeodeDispatchable<State<?>> dispatchable = buildDispatchable(raw, entries);
      storeDelegate.persistDispatchable(dispatchable);

      storeDelegate.completeTransaction();

      dispatch(dispatchable);
      interest.persistResultedIn(Success.of(Result.Success), objectToPersist, 1, 1, object);
    }
    catch (final StorageException ex) {
      LOG.error("error persisting " + JsonSerialization.serialized(objectToPersist), ex);
      storeDelegate.failTransaction();
      interest.persistResultedIn(Failure.of(ex), objectToPersist, 1, 0, object);
    }
    catch (Exception ex) {
      LOG.error("error persisting " + JsonSerialization.serialized(objectToPersist), ex);
      storeDelegate.failTransaction();
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), objectToPersist, 1, 0, object);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> objectsToPersist, final List<Source<E>> sources, final Metadata metadata,
                                                         final long updateId, final PersistResultInterest interest, final Object object) {
    try {
      final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, metadata);
      final List<GeodeDispatchable<State<?>>> dispatchables = new ArrayList<>(objectsToPersist.size());

      storeDelegate.beginTransaction();

      final Collection<State<?>> states = storeDelegate.persistAll(objectsToPersist, updateId, metadata);
      states.forEach(state -> {
        dispatchables.add(buildDispatchable(state, entries));
      });

      storeDelegate.persistEntries(entries);
      dispatchables.forEach(storeDelegate::persistDispatchable);

      storeDelegate.completeTransaction();

      dispatchables.forEach(this::dispatch);
      interest.persistResultedIn(Success.of(Result.Success), objectsToPersist, objectsToPersist.size(), objectsToPersist.size(), object);
    }
    catch (final StorageException ex) {
      LOG.error("error persisting " + JsonSerialization.serialized(objectsToPersist), ex);
      storeDelegate.failTransaction();
      interest.persistResultedIn(Failure.of(ex), objectsToPersist, 1, 0, object);
    }
    catch (Exception ex) {
      LOG.error("error persisting " + JsonSerialization.serialized(objectsToPersist), ex);
      storeDelegate.failTransaction();
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), objectsToPersist, objectsToPersist.size(), 0, object);
    }
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object) */
  @Override
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    if (expression.isMapQueryExpression()) {
      throw new UnsupportedOperationException("MapQueryExpression is not supported by this object store");
    }

    try {
      final QueryMultiResults results = storeDelegate.queryAll(expression);
      interest.queryAllResultedIn(Success.of(Result.Success), results, object);
    } catch (final StorageException e) {
      LOG.error("Query all failed because: " + e.getMessage(), e);
      interest.queryAllResultedIn(Failure.of(e), QueryMultiResults.of(null), object);
    } catch (Exception ex) {
      interest.queryAllResultedIn(
        Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)),
        QueryMultiResults.of(Collections.EMPTY_LIST),
        object);
    }
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object) */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {

    if (expression.isMapQueryExpression()) {
      throw new UnsupportedOperationException("MapQueryExpression is not supported by this object store");
    }

    try {
      final QuerySingleResult singleResult = storeDelegate.queryObject(expression);
      interest.queryObjectResultedIn(Success.of(Result.Success), singleResult, object);
    } catch (final StorageException e) {
      LOG.error("Query failed because: " + e.getMessage(), e);
      interest.queryAllResultedIn(Failure.of(e), QueryMultiResults.of(null), object);
    } catch (Exception ex) {
      interest.queryObjectResultedIn(
        Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)),
        QuerySingleResult.of(null),
        object);
    }
  }

  private void dispatch(final GeodeDispatchable<State<?>> geodeDispatchable) {
    this.dispatcher.dispatch(geodeDispatchable);
  }

  private GeodeDispatchable<State<?>> buildDispatchable(final State<?> state, final List<Entry<?>> entries) {
    final String id = getDispatchId(state, entries);
    return new GeodeDispatchable<>(originatorId, LocalDateTime.now(), id, state, entries);
  }

  private static String getDispatchId(final State<?> raw, final List<Entry<?>> entries) {
    return raw.id + ":" + entries.stream().map(Entry::id).collect(Collectors.joining(":"));
  }
}
