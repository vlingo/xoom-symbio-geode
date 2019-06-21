// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.Actor;
import io.vlingo.common.Failure;
import io.vlingo.common.Outcome;
import io.vlingo.common.Success;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import java.util.*;
/**
 * GeodeObjectStoreActor is an {@link ObjectStore} that knows how to
 * read/write {@link PersistentObject} from/to Apache Geode.
 */
public class GeodeObjectStoreActor extends Actor implements ObjectStore {

  private boolean closed;
  private final Map<Class<?>,PersistentObjectMapper> mappers;

  public GeodeObjectStoreActor() {
    this.mappers = new HashMap<>();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
    }
  }

  @Override
  public <T extends PersistentObject, E> void persist(final T objectToPersist, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    
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
          interest.persistResultedIn(Failure.of(new StorageException(Result.ConcurrentyViolation, "Version conflict.")), objectToPersist, 1, 0, object);
        }
      }
      mutatedAggregate.incrementVersion();
      aggregateRegion.put(mutatedAggregate.persistenceId(), mutatedAggregate);
      
      //TODO: persist sources
      
      interest.persistResultedIn(Success.of(Result.Success), objectToPersist, 1, 1, object);
    }
    catch (Exception ex) {
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), objectToPersist, 1, 0, object);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> objectsToPersist, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    
    try {
      String regionName = null;
      Region<Long, T> region = null;
      Map<Long, T> newEntries = new HashMap<>();
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
                Result.ConcurrentyViolation,
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
      }
      
      newEntries.forEach((k,v) -> v.incrementVersion());
      region.putAll(newEntries);
      
      //TODO: persist sources
      
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
          return Failure.of(new StorageException(Result.ConcurrentyViolation, "Version conflict."));
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

}
