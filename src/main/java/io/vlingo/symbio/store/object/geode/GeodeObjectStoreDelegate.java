// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.Logger;
import io.vlingo.actors.World;
import io.vlingo.common.Completes;
import io.vlingo.symbio.*;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatcherControlDelegate;
import io.vlingo.symbio.store.common.geode.uow.GeodeUnitOfWork;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.object.ObjectStoreDelegate;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.StateObject;
import io.vlingo.symbio.store.object.StateObjectMapper;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import java.util.*;
/**
 * GeodeObjectStoreDelegate and its subclasses are responsible for adapting
 * {@link io.vlingo.symbio.store.object.ObjectStore} to Apache Geode.
 */
public abstract class GeodeObjectStoreDelegate extends GeodeDispatcherControlDelegate implements ObjectStoreDelegate<Entry<?>, State<?>> {

  private final World world;
  private final Logger logger;
  private final Map<Class<?>, StateObjectMapper> mappers;
  private final StateAdapterProvider stateAdapterProvider;
  private GeodeUnitOfWork unitOfWork;

  GeodeObjectStoreDelegate(
    final World world,
    final String originatorId,
    final StateAdapterProvider stateAdapterProvider)
  {
    super(originatorId);
    this.world = world;
    this.logger = world.defaultLogger();
    this.mappers = new HashMap<>();
    this.stateAdapterProvider = stateAdapterProvider;
  }

  protected World world() { return world; }

  protected Logger logger() {
    return logger;
  }

  protected Map<Class<?>, StateObjectMapper> mappers() { return mappers; }

  protected StateAdapterProvider stateAdapterProvider() { return stateAdapterProvider; }

  protected GeodeUnitOfWork unitOfWork() { return unitOfWork; }

  @Override
  public void registerMapper(final StateObjectMapper mapper) {
    mappers().put(mapper.type(), mapper);
  }

  @Override
  public void beginTransaction() {
    this.unitOfWork = new GeodeUnitOfWork();
  }

  @Override
  public void completeTransaction() {
    this.unitOfWork = null;
  }

  @Override
  public void failTransaction() {
    this.unitOfWork = null;
  }

  @Override
  public void close() {
    //nothing to close
  }

  @Override
  public <T extends StateObject> State<?> persist(final T objectToPersist, final long updateId, final Metadata metadata) throws StorageException {
    final Class<?> typeToPersist = objectToPersist.getClass();
    final GeodePersistentObjectMapping mapping = mappingFor(typeToPersist);
    final Region<Long, T> region = regionFor(mapping.regionPath);

    final T persistedObject = region.get(objectToPersist.persistenceId());
    if (persistedObject != null) {
      final long persistedObjectVersion = persistedObject.version();
      final long mutatedObjectVersion = objectToPersist.version();
      if (persistedObjectVersion > mutatedObjectVersion) {
        throw new StorageException(Result.ConcurrencyViolation,
          "Version conflict for object with persistenceId " +
            objectToPersist.persistenceId() +
            "; attempted to overwrite current entry of version " +
            persistedObjectVersion + " with version " +
            mutatedObjectVersion);
      }
    }
    objectToPersist.incrementVersion();
    unitOfWork().register(objectToPersist.persistenceId(), objectToPersist, region.getFullPath());

    return stateAdapterProvider().asRaw(String.valueOf(objectToPersist.persistenceId()), objectToPersist, 1, metadata);
  }

  @Override
  public <T extends StateObject> Collection<State<?>> persistAll(final Collection<T> objectsToPersist, final long updateId, final Metadata metadata) throws StorageException {
    final List<State<?>> states = new ArrayList<>(objectsToPersist.size());
    for (T objectToPersist : objectsToPersist) {
      final Class<?> typeToPersist = objectToPersist.getClass();
      final GeodePersistentObjectMapping mapping = mappingFor(typeToPersist);
      final Region<Long, T> region = regionFor(mapping.regionPath);

      final T persistedObject = region.get(objectToPersist.persistenceId());
      if (persistedObject != null) {
        final long persistedObjectVersion = persistedObject.version();
        final long mutatedObjectVersion = objectToPersist.version();
        if (persistedObjectVersion > mutatedObjectVersion) {
          throw new StorageException(Result.ConcurrencyViolation,
            "Version conflict for object with persistenceId " +
              objectToPersist.persistenceId() +
              "; attempted to overwrite current entry of version "
              + persistedObjectVersion +
              " with version " + mutatedObjectVersion);
        }
      }
      objectToPersist.incrementVersion();
      unitOfWork().register(objectToPersist.persistenceId(), objectToPersist, region.getFullPath());

      final State<?> raw = stateAdapterProvider().asRaw(String.valueOf(objectToPersist.persistenceId()), objectToPersist, 1, metadata);
      states.add(raw);
    }
    return states;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void persistEntries(final Collection<Entry<?>> entries) throws StorageException {
    for (final Entry<?> entry : entries) {
      final Long id = nextEntryId(entry);
      ((BaseEntry)entry).__internal__setId(String.valueOf(id));
      unitOfWork().register(id, entry, eventJournalRegionPath());
    }
  }

  protected abstract Long nextEntryId(final Entry<?> entry);

  @Override
  public void persistDispatchable(final Dispatchable<Entry<?>, State<?>> dispatchable) throws StorageException {
    unitOfWork().register(dispatchable.id(), dispatchable, dispatchablesRegionPath());
  }

  @Override
  public QueryMultiResults queryAll(final QueryExpression expression) throws StorageException {
    final SelectResults<?> results = executeQuery(expression);
    return QueryMultiResults.of(results.asList());
  }

  @Override
  public QuerySingleResult queryObject(final QueryExpression expression) throws StorageException {
    final SelectResults<?> results = executeQuery(expression);
    final Object persistentObject = results.isEmpty() ? null : results.asList().get(0);

    return QuerySingleResult.of(persistentObject);
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

  protected GemFireCache cache() {
    Optional<GemFireCache> cacheOrNull = GemFireCacheProvider.getAnyInstance();
    if (cacheOrNull.isPresent()) {
      return cacheOrNull.get();
    } else {
      throw new StorageException(Result.NoTypeStore, "No GemFireCache has been created in this JVM");
    }
  }

  protected <K, V> Region<K, V> regionFor(final String path) throws StorageException {
    Region<K, V> region = cache().getRegion(path);
    if (region == null) {
      throw new StorageException(Result.NoTypeStore, "Region is not configured for path: " + path);
    }
    return region;
  }

  protected GeodePersistentObjectMapping mappingFor(final Class<?> type) throws StorageException {
    final StateObjectMapper mapper = mappers().get(type);
    if (mapper == null) {
      throw new StorageException(Result.Error, "StateObjectMapper is not configured for type: " + type.getName());
    }
    return mapper.persistMapper();
  }

  protected String eventJournalRegionPath() {
    return GeodeQueries.EVENT_JOURNAL_REGION_PATH;
  }

  protected String dispatchablesRegionPath() {
    return GeodeQueries.DISPATCHABLES_REGION_PATH;
  }
}