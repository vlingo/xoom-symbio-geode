// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.common.geode.identity.LongIDGenerator;
import io.vlingo.symbio.store.object.ObjectStoreDelegate;
import org.apache.geode.cache.CacheTransactionManager;
/**
 * TransactionallyConsistentGeodeObjectStoreDelegate is a transactionally-consistent
 * implementation of {@link GeodeObjectStoreDelegate}.
 */
public class TransactionallyConsistentGeodeObjectStoreDelegate extends GeodeObjectStoreDelegate {

  private LongIDGenerator idGenerator;

  public TransactionallyConsistentGeodeObjectStoreDelegate(
    final World world,
    final String originatorId,
    final StateAdapterProvider stateAdapterProvider) {
    super(world, originatorId, stateAdapterProvider);
  }

  @Override
  public ObjectStoreDelegate copy() {
    return new TransactionallyConsistentGeodeObjectStoreDelegate(
      world(),
      getOriginatorId(),
      stateAdapterProvider());
  }

  @Override
  public void beginTransaction() {
    txManager().begin();
    super.beginTransaction();
  }

  @Override
  public void completeTransaction() {
    unitOfWork().applyTo(cache());
    txManager().commit();
    super.completeTransaction();
  }

  @Override
  public void failTransaction() {
    txManager().rollback();
    super.failTransaction();
  }

  private CacheTransactionManager txManager() {
    return cache().getCacheTransactionManager();
  }

  private LongIDGenerator idGenerator() {
    if (idGenerator == null) {
      idGenerator = new LongIDGenerator(GeodeQueries.SEQUENCE_REGION_PATH, 1L);
    }
    return idGenerator;
  }

  @Override
  protected Long nextEntryId(Entry<?> entry) {
    return idGenerator().next(GeodeQueries.ENTRY_SEQUENCE_NAME);
  }
}