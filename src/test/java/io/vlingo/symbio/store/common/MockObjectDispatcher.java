// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.dispatch.ConfirmDispatchedResultInterest;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;

public class MockObjectDispatcher implements Dispatcher<Dispatchable<Entry<?>, State<?>>> {
  private AccessSafely access;

  private final ConfirmDispatchedResultInterest confirmDispatchedResultInterest;
  private DispatcherControl control;
  private final Map<String,Dispatchable<Entry<?>, State<?>>> dispatched = new HashMap<>();
  private final AtomicBoolean processDispatch = new AtomicBoolean(true);
  private final AtomicInteger dispatchAttemptCount = new AtomicInteger(0);

  public MockObjectDispatcher(final ConfirmDispatchedResultInterest confirmDispatchedResultInterest) {
    this.confirmDispatchedResultInterest = confirmDispatchedResultInterest;
    this.access = AccessSafely.afterCompleting(0);
  }

  @Override
  public void controlWith(final DispatcherControl control) {
    this.control = control;
  }

  @Override
  public void dispatch(final Dispatchable<Entry<?>, State<?>> dispatchable) {
    dispatchAttemptCount.getAndIncrement();
    if (processDispatch.get()) {
      access.writeUsing("dispatched", dispatchable.id(), dispatchable);
      control.confirmDispatched(dispatchable.id(), confirmDispatchedResultInterest);
    }
  }

  public AccessSafely afterCompleting(final int times) {
    this.access = AccessSafely
      .afterCompleting(times)

      .writingWith("dispatched", dispatched::put)

      .readingWith("dispatchedState", (String id) -> dispatched.get(id).typedState())
      .readingWith("dispatchedStateCount", dispatched::size)

      .writingWith("processDispatch", processDispatch::set)
      .readingWith("processDispatch", processDispatch::get)

      .readingWith("dispatchAttemptCount", dispatchAttemptCount::get)

      .readingWith("dispatched", () -> dispatched);

    return access;
  }

  public void dispatchUnconfirmed() {
    control.dispatchUnconfirmed();
  }

  public Map<String, Dispatchable<Entry<?>, State<?>>> getDispatched() {
    return this.access.readFrom("dispatched");
  }

  @SuppressWarnings("unused")
  private static class Dispatch<S extends State<?>,E extends Entry<?>> {
    final Collection<E> entries;
    final S state;

    Dispatch(final S state, final Collection<E> entries) {
      this.state = state;
      this.entries = entries;
    }
  }
}
