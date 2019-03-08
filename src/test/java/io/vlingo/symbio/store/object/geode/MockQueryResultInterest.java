// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.geode;

import java.util.concurrent.atomic.AtomicReference;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.ObjectStore.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest;
import io.vlingo.symbio.store.object.ObjectStore.QuerySingleResult;
/**
 * MockQueryResultInterest
 */
public class MockQueryResultInterest implements QueryResultInterest {

  private AccessSafely access;
  public AtomicReference<QueryMultiResults> multiResults = new AtomicReference<>();
  public AtomicReference<QuerySingleResult> singleResult = new AtomicReference<>();
  
  /* @see io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest#queryAllResultedIn(io.vlingo.common.Outcome, io.vlingo.symbio.store.object.ObjectStore.QueryMultiResults, java.lang.Object) */
  @Override
  public void queryAllResultedIn(Outcome<StorageException, Result> outcome, QueryMultiResults results, Object object) {
    access.writeUsing("multiResults", results);
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest#queryObjectResultedIn(io.vlingo.common.Outcome, io.vlingo.symbio.store.object.ObjectStore.QuerySingleResult, java.lang.Object) */
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
