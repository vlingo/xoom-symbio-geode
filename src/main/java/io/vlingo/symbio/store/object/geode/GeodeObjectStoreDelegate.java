// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.Definition;
import io.vlingo.actors.Logger;
import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatcherControlDelegate;
import io.vlingo.symbio.store.common.geode.identity.IDGenerator;
import io.vlingo.symbio.store.common.geode.identity.LongIDGeneratorActor;
import io.vlingo.symbio.store.object.geode.uow.GeodeUnitOfWork;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.object.ObjectStoreDelegate;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
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
 * GeodeObjectStoreDelegate is responsible for adapting {@link io.vlingo.symbio.store.object.ObjectStore}
 * to Apache Geode.
 */
public class GeodeObjectStoreDelegate extends GeodeDispatcherControlDelegate implements ObjectStoreDelegate<Entry<?>, State<?>> {
  
  public static final String ENTRY_SEQUENCE_NAME = "Entries";
  public static final String UOW_SEQUENCE_NAME = "UnitsOfWork";
  
  private final World world;
  private final Logger logger;
  private final ConsistencyMode consistencyMode;
  private final Map<Class<?>, PersistentObjectMapper> mappers;
  private final StateAdapterProvider stateAdapterProvider;
  private GeodeUnitOfWork unitOfWork;
  private IDGenerator<Long> idGenerator;

  public GeodeObjectStoreDelegate(
    final World world,
    final ConsistencyMode mode,
    final String originatorId,
    final StateAdapterProvider stateAdapterProvider)
  {
    super(originatorId);
    this.world = world;
    this.logger = world.defaultLogger();
    this.consistencyMode = mode;
    this.mappers = new HashMap<>();
    this.stateAdapterProvider = stateAdapterProvider;
  }

  private GeodeObjectStoreDelegate(
    final World world,
    final ConsistencyMode mode,
    final String originatorId,
    final Map<Class<?>, PersistentObjectMapper> mappers,
    final StateAdapterProvider stateAdapterProvider)
  {
    super(originatorId);
    this.world = world;
    this.logger = world.defaultLogger();
    this.consistencyMode = mode;
    this.mappers = mappers;
    this.stateAdapterProvider = stateAdapterProvider;
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
    return new GeodeObjectStoreDelegate(
      this.world,
      this.consistencyMode,
      getOriginatorId(),
      this.stateAdapterProvider);
  }

  @Override
  public void beginTransaction() {
    logger.debug("beginTransaction - entered");
    try {
      if (consistencyMode.isTransactional()) {
        cache().getCacheTransactionManager().begin();
      }
      this.unitOfWork = new GeodeUnitOfWork();
    }
    finally {
      logger.debug("beginTransaction - exited");
    }
  }

  @Override
  public void completeTransaction() {
    logger.debug("completeTransaction - entered");
    try {
      idGenerator()
        .next(UOW_SEQUENCE_NAME)
        .andThenConsume(id -> {
          unitOfWork.withId(id);
          if (consistencyMode.isTransactional()) {
            unitOfWork.applyTo(cache());
            cache().getCacheTransactionManager().commit();
          } else {
            regionFor(GeodeQueries.OBJECTSTORE_UOW_REGION_PATH).put(id, unitOfWork);
          }
        });
    }
    finally {
      logger.debug("completeTransaction - exited");
    }
  }

  @Override
  public void failTransaction() {
    logger.debug("failTransaction - entered");
    try {
      this.unitOfWork = null;
      if (consistencyMode.isTransactional()) {
        cache().getCacheTransactionManager().rollback();
      }
    }
    finally {
      logger.debug("failTransaction - entered");
    }
  }

  @Override
  public <T extends PersistentObject> State<?> persist(final T objectToPersist, final long updateId, final Metadata metadata) throws StorageException {
    logger.debug("persist - entered with objectToPersist=" + objectToPersist);
    try {
      final Class<?> typeToPersist = objectToPersist.getClass();
      final GeodePersistentObjectMapping mapping = persistMappingFor(typeToPersist);
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
      unitOfWork.persistEntity(mapping.regionPath, objectToPersist);

      return stateAdapterProvider.asRaw(String.valueOf(objectToPersist.persistenceId()), objectToPersist, 1, metadata);
    }
    finally {
      logger.debug("persist - exited with objectToPersist=" + objectToPersist);
    }
  }

  @Override
  public <T extends PersistentObject> Collection<State<?>> persistAll(final Collection<T> objectsToPersist, final long updateId, final Metadata metadata) throws StorageException {
    logger.debug("persistAll - entered");
    try {
      final List<State<?>> states = new ArrayList<>(objectsToPersist.size());

      for (T objectToPersist : objectsToPersist) {

        final Class<?> typeToPersist = objectToPersist.getClass();
        final GeodePersistentObjectMapping mapping = persistMappingFor(typeToPersist);
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
        unitOfWork.persistEntity(mapping.regionPath, objectToPersist);

        final State<?> raw = stateAdapterProvider.asRaw(String.valueOf(objectToPersist.persistenceId()), objectToPersist, 1, metadata);
        states.add(raw);
      }
      return states;
    }
    finally {
      logger.debug("persistAll - exited");
    }
  }

  @Override
  public void persistEntries(final Collection<Entry<?>> entries) throws StorageException {
    logger.debug("persistEntries - entered");
    try {
      for (final Entry<?> entry : entries) {
        final GeodeEventJournalEntry geodeEntry;
        if (entry instanceof GeodeEventJournalEntry) {
          geodeEntry = (GeodeEventJournalEntry) entry;
        } else {
          geodeEntry = new GeodeEventJournalEntry(entry);
        }
        idGenerator()
          .next(ENTRY_SEQUENCE_NAME)
          .andThenConsume(id -> {
            geodeEntry.__internal__setId(String.valueOf(id));
            unitOfWork.persistEntry(geodeEntry);
          });
      }
    }
    finally {
      logger.debug("persistEntries - exited");
    }
  }

  @Override
  public void persistDispatchable(final Dispatchable<Entry<?>, State<?>> dispatchable) throws StorageException {
    logger.debug("persistDispatchable - entered");
    try {
      unitOfWork.persistDispatchable(dispatchable);
    }
    finally {
      logger.debug("persistDispatchable - exited");
    }
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

  @SuppressWarnings("unchecked")
  private IDGenerator<Long> idGenerator() {
    if (idGenerator == null) {
      idGenerator = world.actorFor(
        IDGenerator.class,
        Definition.has(
          LongIDGeneratorActor.class,
          Definition.parameters(1L)));
    }
    return idGenerator;
  }

  private <K, V> Region<K, V> regionFor(final String path) throws StorageException {
    Region<K, V> region = cache().getRegion(path);
    if (region == null) {
      throw new StorageException(Result.NoTypeStore, "Region is not configured: " + path);
    }
    return region;
  }

  private GeodePersistentObjectMapping persistMappingFor(final Class<?> type) throws StorageException {
    final PersistentObjectMapper mapper = mappers.get(type);
    if (mapper == null) {
      throw new StorageException(Result.Error, "PersistentObjectMapper is not configured for type: " + type.getName());
    }
    return mapper.persistMapper();
  }
}