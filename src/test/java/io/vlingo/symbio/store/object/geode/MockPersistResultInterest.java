// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest;
/**
 * MockPersistResultInterest
 */
public class MockPersistResultInterest implements PersistResultInterest {

  private AccessSafely access;

  public AtomicReference<Exception> persistCause = new AtomicReference<>();
  public AtomicReference<Result> persistResult = new AtomicReference<>();
  public AtomicReference<Object> persistedObject = new AtomicReference<>();
  public AtomicInteger expectedPersistCount = new AtomicInteger(0);
  public AtomicInteger actualPersistCount = new AtomicInteger(0);

  /* @see io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest#persistResultedIn(io.vlingo.common.Outcome, io.vlingo.symbio.store.object.PersistentObject, int, int, java.lang.Object) */
  @Override
  public void persistResultedIn(
          final Outcome<StorageException, Result> outcome,
          final Object persistentObject,
          final int expected,
          final int actual,
          final Object object) {

    outcome
      .andThen(result -> {
        access.writeUsing("persistObjectData", new StoreData(null, result, persistentObject, expected, actual));
        return result;
      })
      .otherwise(cause -> {
        access.writeUsing("persistObjectData", new StoreData(cause, cause.result, persistentObject, expected, actual));
        System.out.println("MockPersistResultInterest::persistResultedIn FAILURE cause=\"" + cause + "\", persistentObject=" + persistentObject);
        return cause.result;
      });
  }

  public AccessSafely afterCompleting(final int times) {
    access = AccessSafely
      .afterCompleting(times)
      .writingWith("persistObjectData", (StoreData data) -> {
        persistCause.set(data.errorCauses);
        persistResult.set(data.result);
        persistedObject.set(data.persistedObject);
        expectedPersistCount.set(data.expected);
        actualPersistCount.set(data.actual);
      })
      .readingWith("persistCause", () -> persistCause.get())
      .readingWith("persistResult", () -> persistResult.get())
      .readingWith("persistedObject",() -> persistedObject.get())
      .readingWith("expectedPersistCount",() -> expectedPersistCount.get())
      .readingWith("actualPersistCount",() -> actualPersistCount.get());
    return access;
  }

  public class StoreData {
    public final Exception errorCauses;
    public final Result result;
    public final Object persistedObject;
    public final int expected;
    public final int actual;

    public StoreData(Exception errorCauses, Result result, Object persistedObject, int expected, int actual) {
      super();
      this.errorCauses = errorCauses;
      this.result = result;
      this.persistedObject = persistedObject;
      this.expected = expected;
      this.actual = actual;
    }
  }
}
