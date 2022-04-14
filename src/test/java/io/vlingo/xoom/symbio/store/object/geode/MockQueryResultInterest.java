// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.geode;

import java.util.concurrent.atomic.AtomicReference;

import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QueryResultInterest;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QuerySingleResult;
/**
 * MockQueryResultInterest
 */
public class MockQueryResultInterest implements QueryResultInterest {

  private AccessSafely access;
  public AtomicReference<QueryMultiResults> multiResults = new AtomicReference<>();
  public AtomicReference<QuerySingleResult> singleResult = new AtomicReference<>();
  
  /* @see io.vlingo.xoom.symbio.store.object.ObjectStore.QueryResultInterest#queryAllResultedIn(io.vlingo.xoom.common.Outcome, io.vlingo.xoom.symbio.store.object.ObjectStore.QueryMultiResults, java.lang.Object) */
  @Override
  public void queryAllResultedIn(Outcome<StorageException, Result> outcome, QueryMultiResults results, Object object) {
    access.writeUsing("multiResults", results);
  }

  /* @see io.vlingo.xoom.symbio.store.object.ObjectStore.QueryResultInterest#queryObjectResultedIn(io.vlingo.xoom.common.Outcome, io.vlingo.xoom.symbio.store.object.ObjectStore.QuerySingleResult, java.lang.Object) */
  @Override
  public void queryObjectResultedIn(Outcome<StorageException, Result> outcome, QuerySingleResult result, Object object) {
    access.writeUsing("singleResult", result);
  }

  public AccessSafely afterCompleting(final int times) {
    access = AccessSafely
      .afterCompleting(times)
      .writingWith("multiResults", (QueryMultiResults r) -> multiResults.set(r))
      .readingWith("multiResults", () -> multiResults.get())
      .writingWith("singleResult", (QuerySingleResult r) -> singleResult.set(r))
      .readingWith("singleResult", () -> singleResult.get());
    return access;
  }
}
