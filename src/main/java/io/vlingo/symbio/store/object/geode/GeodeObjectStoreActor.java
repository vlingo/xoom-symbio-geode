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
import io.vlingo.common.Outcome;
import io.vlingo.common.Success;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.*;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatchable;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatcherControlDelegate;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * GeodeObjectStoreActor is an {@link ObjectStore} that knows how to
 * read/write {@link PersistentObject} from/to Apache Geode.
 */
public class GeodeObjectStoreActor extends Actor implements ObjectStore {

  private static final Logger LOG = Logger.basicLogger();
  public static final long CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT = 1000L;
  public static final long CONFIRMATION_EXPIRATION_DEFAULT = 1000L;

  private boolean closed;
  private final String originatorId;

  private final Map<Class<?>,PersistentObjectMapper> mappers;
  private final DispatcherControl dispatcherControl;
  private final Dispatcher<GeodeDispatchable<State<?>>> dispatcher;
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;

  public GeodeObjectStoreActor(final String originatorId, final Dispatcher<GeodeDispatchable<State<?>>> dispatcher) {
    this(originatorId,
         dispatcher,
         CHECK_CONFIRMATION_EXPIRATION_INTERVAL_DEFAULT,
         CONFIRMATION_EXPIRATION_DEFAULT);
  }

  public GeodeObjectStoreActor(final String originatorId, final Dispatcher<GeodeDispatchable<State<?>>> dispatcher,
          long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.mappers = new HashMap<>();
    this.originatorId = originatorId;

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    
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
    final PersistentObjectMapper mapper = mappers.get(objectToPersist.getClass());
    GeodePersistentObjectMapping mapping = mapper.persistMapper();

    Region<Long, T> aggregateRegion = cache().getRegion(mapping.regionName);
    if (aggregateRegion == null) {
      interest.persistResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "Region not configured: " + mapping.regionName)), objectToPersist, 1, 0, object);
      return;
    }

    try {
      final T mutatedAggregate = objectToPersist;
      final T persistedAggregate = aggregateRegion.get(mutatedAggregate.persistenceId());
      if (persistedAggregate != null) {
        final long persistedAggregateVersion = persistedAggregate.version();
        final long mutatedAggregateVersion = mutatedAggregate.version();
        if (persistedAggregateVersion > mutatedAggregateVersion) {
          interest.persistResultedIn(Failure.of(new StorageException(Result.ConcurrencyViolation, "Version conflict.")), objectToPersist, 1, 0, object);
        }
      }
      mutatedAggregate.incrementVersion();
      LOG.info("persist - put: " + mutatedAggregate);
      aggregateRegion.put(mutatedAggregate.persistenceId(), mutatedAggregate);

      //TODO: persist sources
      final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, metadata);
      final State<?> raw = stateAdapterProvider.asRaw(String.valueOf(objectToPersist.persistenceId()), objectToPersist, 1, metadata);
      dispatch(raw, entries, cache());
      
      interest.persistResultedIn(Success.of(Result.Success), objectToPersist, 1, 1, object);
    }
    catch (Exception ex) {
      LOG.error("error persisting " + JsonSerialization.serialized(objectToPersist), ex);
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), objectToPersist, 1, 0, object);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> objectsToPersist, final List<Source<E>> sources, final Metadata metadata,
          final long updateId, final PersistResultInterest interest, final Object object) {
    try {
      String regionName = null;
      Region<Long, T> region = null;
      Map<Long, T> newEntries = new HashMap<>();
      final List<State<?>> states = new ArrayList<>(objectsToPersist.size());
      for (T objectToPersist : objectsToPersist) {

        final PersistentObjectMapper mapper = mappers.get(objectToPersist.getClass());
        GeodePersistentObjectMapping mapping = mapper.persistMapper();

        if (region == null) {
          regionName = mapping.regionName;
          region = cache().getRegion(regionName);
        }
        else if (!regionName.equals(mapping.regionName)) {
          /*
           * This ObjectStore implementation currently requires that all elements of
           * the argument objectsToPersist must be stored in the same Region (though
           * the only supertype they must share is VersionedPeristentObject). This
           * requirement allows the entire collection of objects to be stored in one
           * atomic operation using Region.putAll without resorting to using
           * transactions (which impose many additional constraints as documented here:
           * https://geode.apache.org/docs/guide/18/developing/transactions/design_considerations.html)
           */
          interest.persistResultedIn(
                  Failure.of(new StorageException(Result.Error, "persistAll requires that the collection of objects to be persisted must share the same single Geode Region")),
                  objectsToPersist,
                  objectsToPersist.size(),
                  0,
                  object);
          return;
        }

        final T mutatedObject = objectToPersist;
        final T persistedObject = region.get(mutatedObject.persistenceId());
        if (persistedObject == null) {
          newEntries.put(mutatedObject.persistenceId(), mutatedObject);
        }
        else {
          final long persistedObjectVersion = persistedObject.version();
          final long mutatedObjectVersion = mutatedObject.version();
          if (persistedObjectVersion > mutatedObjectVersion) {
            interest.persistResultedIn(
                    Failure.of(new StorageException(
                            Result.ConcurrencyViolation,
                            "Version conflict for object with persistenceId " + mutatedObject.persistenceId() +
                                    "; attempted to overwrite current entry with version " + persistedObjectVersion +
                                    " with version " + mutatedObjectVersion)),
                    objectsToPersist,
                    objectsToPersist.size(),
                    0,
                    object);
            return;
          }
          else {
            newEntries.put(mutatedObject.persistenceId(), mutatedObject);
          }
        }
        
        final State<?> raw = stateAdapterProvider.asRaw(String.valueOf(objectToPersist.persistenceId()), objectToPersist, 1, metadata);
        states.add(raw);
      }

      newEntries.forEach((k,v) -> v.incrementVersion());
      LOG.info("persist - putAll: " + Arrays.toString(newEntries.values().toArray()));
      region.putAll(newEntries);

      //TODO: persist sources
      final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, metadata);
      states.forEach(state -> {
        //dispatch each persistent object
        dispatch(state, entries, cache());
      });

      interest.persistResultedIn(Success.of(Result.Success), objectsToPersist, objectsToPersist.size(), objectsToPersist.size(), object);
    }
    catch (Exception ex) {
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), objectsToPersist, objectsToPersist.size(), 0, object);
    }
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object) */
  @Override
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    
    if (expression.isMapQueryExpression()) {
      throw new UnsupportedOperationException("MapQueryExpression is not supported by this object store");
    }
    
    String queryString = expression.query;
    QueryService queryService = cache().getQueryService();
    Query query = queryService.newQuery(queryString);
    
    List<?> queryParms = expression.isListQueryExpression()
      ? expression.asListQueryExpression().parameters
      : Collections.EMPTY_LIST;
    
    try {
      SelectResults<?> results = (SelectResults<?>) query.execute(queryParms.toArray());
      interest.queryAllResultedIn(
        Success.of(Result.Success),
        QueryMultiResults.of(results.asList()),
        object);
    }
    catch (Exception ex) {
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
    
    String queryString = expression.query;
    QueryService queryService = cache().getQueryService();
    Query query = queryService.newQuery(queryString);
    
    List<?> queryParms = expression.isListQueryExpression()
      ? expression.asListQueryExpression().parameters
      : Collections.EMPTY_LIST;
    
    try {
      SelectResults<?> results = (SelectResults<?>) query.execute(queryParms.toArray());
      final Object presistentObject = results.isEmpty() ? null : results.asList().get(0);
      interest.queryObjectResultedIn(
        Success.of(Result.Success),
        QuerySingleResult.of(presistentObject),
        object);
    }
    catch (Exception ex) {
      interest.queryObjectResultedIn(
        Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)),
        QuerySingleResult.of(null),
        object);
    }
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.store.object.PersistentObjectMapper) */
  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    mappers.put(mapper.type(), mapper);
  }

  protected Outcome<StorageException, Result> persistEach(final Object objectToPersist) {
    final PersistentObjectMapper mapper = mappers.get(objectToPersist.getClass());
    GeodePersistentObjectMapping mapping = mapper.persistMapper();

    Region<Long, PersistentObject> region = cache().getRegion(mapping.regionName);
    if (region == null) {
      return Failure.of(new StorageException(Result.NoTypeStore, "Region not configured: " + mapping.regionName));
    }
    
    final PersistentObject mutatedObject = PersistentObject.from(objectToPersist);
    try {
      PersistentObject persistedObject = region.putIfAbsent(mutatedObject.persistenceId(), mutatedObject);
      if (persistedObject != null) {
        final long persistedObjectVersion = persistedObject.version();
        final long mutatedObjectVersion = mutatedObject.version();
        if (persistedObjectVersion > mutatedObjectVersion) {
          return Failure.of(new StorageException(Result.ConcurrencyViolation, "Version conflict."));
        }
        else {
          mutatedObject.incrementVersion();
          region.put(mutatedObject.persistenceId(), mutatedObject);
        }
      }
      return Success.of(Result.Success);
    }
    catch (Exception ex) {
      return Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex));
    }
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

  private void dispatch(final State<?> state, final List<Entry<?>> entries, final GemFireCache cache){
    final String id = getDispatchId(state, entries);
    final GeodeDispatchable<State<?>> geodeDispatchable = new GeodeDispatchable<>(originatorId, LocalDateTime.now(), id, state, entries);
    Region<String, GeodeDispatchable<State<?>>> dispatchablesRegion = cache.getRegion(GeodeQueries.DISPATCHABLES_REGION_PATH);
    dispatchablesRegion.put(id, geodeDispatchable);
    this.dispatcher.dispatch(geodeDispatchable);
  }

  private static String getDispatchId(final State<?> raw, final List<Entry<?>> entries) {
    return raw.id + ":" + entries.stream().map(Entry::id).collect(Collectors.joining(":"));
  }
}
