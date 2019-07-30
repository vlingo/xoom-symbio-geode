// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatcherControlDelegate;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.object.ObjectStoreDelegate;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

public class GeodeObjectStoreDelegate extends GeodeDispatcherControlDelegate implements ObjectStoreDelegate<Entry<?>, State<?>> {
  private final Map<Class<?>, PersistentObjectMapper> mappers;
  private final Logger logger;
  private final StateAdapterProvider stateAdapterProvider;

  public GeodeObjectStoreDelegate(final String originatorId, final StateAdapterProvider stateAdapterProvider, final Logger logger) {
    super(originatorId);
    this.logger = logger;
    this.stateAdapterProvider = stateAdapterProvider;
    this.mappers = new HashMap<>();
  }

  private GeodeObjectStoreDelegate(final String originatorId, final Map<Class<?>, PersistentObjectMapper> mappers,
          final StateAdapterProvider stateAdapterProvider, final Logger logger) {
    super(originatorId);
    this.logger = logger;
    this.stateAdapterProvider = stateAdapterProvider;
    this.mappers = mappers;
  }

  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    mappers.put(mapper.type(), mapper);
  }

  @Override
  public void close() {
    //nothing to close
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ObjectStoreDelegate copy() {
    return new GeodeObjectStoreDelegate(getOriginatorId(), this.stateAdapterProvider, this.logger);
  }

  @Override
  public void beginTransaction() {
    //Geode store does not require transactions
  }

  @Override
  public void completeTransaction() {
    //Geode store does not require transactions
  }

  @Override
  public void failTransaction() {
    //Geode store does not require transactions
  }

  @Override
  public <T extends PersistentObject> Collection<State<?>> persistAll(final Collection<T> objectsToPersist, final long updateId, final Metadata metadata)
          throws StorageException {
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
      } else if (!regionName.equals(mapping.regionName)) {
        /*
         * This ObjectStore implementation currently requires that all elements of
         * the argument objectsToPersist must be stored in the same Region (though
         * the only supertype they must share is VersionedPeristentObject). This
         * requirement allows the entire collection of objects to be stored in one
         * atomic operation using Region.putAll without resorting to using
         * transactions (which impose many additional constraints as documented here:
         * https://geode.apache.org/docs/guide/18/developing/transactions/design_considerations.html)
         */
        throw new StorageException(Result.Error, "persistAll requires that the collection of objects to be persisted must share the same single Geode Region");
      }

      final T mutatedObject = objectToPersist;
      final T persistedObject = region.get(mutatedObject.persistenceId());
      if (persistedObject == null) {
        newEntries.put(mutatedObject.persistenceId(), mutatedObject);
      } else {
        final long persistedObjectVersion = persistedObject.version();
        final long mutatedObjectVersion = mutatedObject.version();
        if (persistedObjectVersion > mutatedObjectVersion) {
          throw new StorageException(Result.ConcurrencyViolation,
                  "Version conflict for object with persistenceId " + mutatedObject.persistenceId() + "; attempted to overwrite current entry with version "
                          + persistedObjectVersion + " with version " + mutatedObjectVersion);
        } else {
          newEntries.put(mutatedObject.persistenceId(), mutatedObject);
        }
      }

      final State<?> raw = stateAdapterProvider.asRaw(String.valueOf(objectToPersist.persistenceId()), objectToPersist, 1, metadata);
      states.add(raw);
    }

    newEntries.forEach((k, v) -> v.incrementVersion());
    logger.info("persist - putAll: " + Arrays.toString(newEntries.values().toArray()));
    region.putAll(newEntries);

    return states;
  }

  @Override
  public <T extends PersistentObject> State<?> persist(final T persistentObject, final long updateId, final Metadata metadata) throws StorageException {
    final PersistentObjectMapper mapper = mappers.get(persistentObject.getClass());
    GeodePersistentObjectMapping mapping = mapper.persistMapper();

    Region<Long, T> aggregateRegion = cache().getRegion(mapping.regionName);
    if (aggregateRegion == null) {
      throw new StorageException(Result.NoTypeStore, "Region not configured: " + mapping.regionName);
    }

    final T mutatedAggregate = persistentObject;
    final T persistedAggregate = aggregateRegion.get(mutatedAggregate.persistenceId());
    if (persistedAggregate != null) {
      final long persistedAggregateVersion = persistedAggregate.version();
      final long mutatedAggregateVersion = mutatedAggregate.version();
      if (persistedAggregateVersion > mutatedAggregateVersion) {
        throw new StorageException(Result.ConcurrencyViolation, "Version conflict");
      }
    }
    mutatedAggregate.incrementVersion();
    logger.info("persist - put: " + mutatedAggregate);
    aggregateRegion.put(mutatedAggregate.persistenceId(), mutatedAggregate);

    return stateAdapterProvider.asRaw(String.valueOf(persistentObject.persistenceId()), persistentObject, 1, metadata);
  }

  @Override
  public void persistEntries(final Collection<Entry<?>> entries) throws StorageException {
    //TODO: persist sources
  }

  @Override
  public void persistDispatchable(final Dispatchable<Entry<?>, State<?>> dispatchable) throws StorageException {
    Region<String, Dispatchable<Entry<?>, State<?>>> dispatchablesRegion = cache().getRegion(GeodeQueries.DISPATCHABLES_REGION_PATH);
    dispatchablesRegion.put(dispatchable.id(), dispatchable);
  }

  @Override
  public QueryMultiResults queryAll(final QueryExpression expression) throws StorageException {
    final SelectResults<?> results = executeQuery(expression);
    return QueryMultiResults.of(results.asList());
  }

  @Override
  public QuerySingleResult queryObject(final QueryExpression expression) throws StorageException {
    final SelectResults<?> results = executeQuery(expression);
    final Object presistentObject = results.isEmpty() ? null : results.asList().get(0);

    return QuerySingleResult.of(presistentObject);
  }

  private SelectResults<?> executeQuery(final QueryExpression expression) throws StorageException {
    String queryString = expression.query;
    QueryService queryService = cache().getQueryService();
    Query query = queryService.newQuery(queryString);

    List<?> queryParms = expression.isListQueryExpression() ? expression.asListQueryExpression().parameters : Collections.EMPTY_LIST;

    final SelectResults<?> results;
    try {
      results = (SelectResults<?>) query.execute(queryParms.toArray());
    } catch (Exception e) {
      throw new StorageException(Result.Error, e.getMessage(), e);
    }
    return results;
  }

  private GemFireCache cache() {
    Optional<GemFireCache> cacheOrNull = GemFireCacheProvider.getAnyInstance();
    if (cacheOrNull.isPresent()) {
      return cacheOrNull.get();
    } else {
      throw new StorageException(Result.NoTypeStore, "No GemFireCache has been created in this JVM");
    }
  }
}
