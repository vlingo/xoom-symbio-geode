// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import io.vlingo.actors.Actor;
import io.vlingo.common.Failure;
import io.vlingo.common.Outcome;
import io.vlingo.common.Success;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.Configuration;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.VersionedPersistentObject;
/**
 * GeodeObjectStoreActor is an {@link ObjectStore} that knows how to
 * read/write {@link VersionedPersistentObject} from/to Apache Geode.
 */
public class GeodeObjectStoreActor extends Actor implements ObjectStore {
  
  private boolean closed;
  private final GemFireCache cache;
  private final Map<Class<?>,PersistentObjectMapper> mappers;

  public GeodeObjectStoreActor(final Configuration config) {
    this.cache = GemFireCacheProvider.getAnyInstance(config);
    this.mappers = new HashMap<>();
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#close() */
  @Override
  public void close() {
    if (!closed) {
      closed = true;
    }
  }
  
  /* @see io.vlingo.symbio.store.object.ObjectStore#persist(java.lang.Object, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object) */
  @Override
  public void persist(final Object objectToPersist, final long updateId, final PersistResultInterest interest, final Object object) {
    final PersistentObjectMapper mapper = mappers.get(objectToPersist.getClass());
    GeodePersistentObjectMapping mapping = mapper.persistMapper();
    
    Region<Long, VersionedPersistentObject> region = cache.getRegion(mapping.regionName);
    if (region == null) {
      interest.persistResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "Region not configured: " + mapping.regionName)), objectToPersist, 1, 0, object);
      return;
    }
    
    try {
      final VersionedPersistentObject mutatedObject = VersionedPersistentObject.from(objectToPersist);
      final VersionedPersistentObject persistedObject = region.get(mutatedObject.persistenceId());
      if (persistedObject != null) {
        final int persistedObjectVersion = persistedObject.version();
        final int mutatedObjectVersion = mutatedObject.version();
        if (persistedObjectVersion > mutatedObjectVersion) {
          interest.persistResultedIn(Failure.of(new StorageException(Result.ConcurrentyViolation, "Version conflict.")), objectToPersist, 1, 0, object);
        }
      }
      mutatedObject.incrementVersion();
      region.put(mutatedObject.persistenceId(), mutatedObject);
      interest.persistResultedIn(Success.of(Result.Success), objectToPersist, 1, 1, object);
    }
    catch (Exception ex) {
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), objectToPersist, 1, 0, object);
    }
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#persistAll(java.util.Collection, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object) */
  @Override
  public void persistAll(final Collection<Object> objectsToPersist, final long updateId, final PersistResultInterest interest, final Object object) {
    
    try {
      String regionName = null;
      Region<Long, VersionedPersistentObject> region = null;
      Map<Long, VersionedPersistentObject> newEntries = new HashMap<>();
      for (Object objectToPersist : objectsToPersist) {
        
        final PersistentObjectMapper mapper = mappers.get(objectToPersist.getClass());
        GeodePersistentObjectMapping mapping = mapper.persistMapper();
        
        if (region == null) {
          regionName = mapping.regionName;
          region = cache.getRegion(regionName);
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
        
        final VersionedPersistentObject mutatedObject = VersionedPersistentObject.from(objectToPersist);
        final VersionedPersistentObject persistedObject = region.get(mutatedObject.persistenceId());
        if (persistedObject == null) {
          newEntries.put(mutatedObject.persistenceId(), mutatedObject);
        }
        else {
          final int persistedObjectVersion = persistedObject.version();
          final int mutatedObjectVersion = mutatedObject.version();
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
      
      interest.persistResultedIn(Success.of(Result.Success), objectsToPersist, objectsToPersist.size(), objectsToPersist.size(), object);
    }
    catch (Exception ex) {
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), objectsToPersist, objectsToPersist.size(), 0, object);
    }
  }
  
  /* @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object) */
  @Override
  public void queryAll(QueryExpression expression, QueryResultInterest interest, Object object) {
    
    if (expression.isMapQueryExpression()) {
      throw new UnsupportedOperationException("MapQueryExpression is not supported by this object store");
    }
    
    String queryString = expression.query;
    QueryService queryService = cache.getQueryService();
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
  public void queryObject(QueryExpression expression, QueryResultInterest interest, Object object) {
    
    if (expression.isMapQueryExpression()) {
      throw new UnsupportedOperationException("MapQueryExpression is not supported by this object store");
    }
    
    String queryString = expression.query;
    QueryService queryService = cache.getQueryService();
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
  public void registerMapper(PersistentObjectMapper mapper) {
    mappers.put(mapper.type(), mapper);
  }

  protected Outcome<StorageException, Result> persistEach(Object objectToPersist) {
    final PersistentObjectMapper mapper = mappers.get(objectToPersist.getClass());
    GeodePersistentObjectMapping mapping = mapper.persistMapper();

    Region<Long, VersionedPersistentObject> region = cache.getRegion(mapping.regionName);
    if (region == null) {
      return Failure.of(new StorageException(Result.NoTypeStore, "Region not configured: " + mapping.regionName));
    }
    
    final VersionedPersistentObject mutatedObject = VersionedPersistentObject.from(objectToPersist);
    try {
      VersionedPersistentObject persistedObject = region.putIfAbsent(mutatedObject.persistenceId(), mutatedObject);
      if (persistedObject != null) {
        final int persistedObjectVersion = persistedObject.version();
        final int mutatedObjectVersion = mutatedObject.version();
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
}
