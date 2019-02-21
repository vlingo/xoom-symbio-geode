// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.state.StateStore.ConfirmDispatchedResultInterest;
import io.vlingo.symbio.store.state.StateStore.Dispatcher;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;

public class MockObjectDispatcher implements Dispatcher {
  private AccessSafely access;
  
  public final ConfirmDispatchedResultInterest confirmDispatchedResultInterest;
  public DispatcherControl control;
  public final Map<String,Object> dispatched = new HashMap<>();
  public final AtomicBoolean processDispatch = new AtomicBoolean(true);
  public final AtomicInteger dispatchAttemptCount = new AtomicInteger(0);

  public MockObjectDispatcher(final ConfirmDispatchedResultInterest confirmDispatchedResultInterest) {
    this.confirmDispatchedResultInterest = confirmDispatchedResultInterest;
    this.access = AccessSafely.afterCompleting(0);
  }

  @Override
  public void controlWith(final DispatcherControl control) {
    this.control = control;
  }

  @Override
  public <S extends State<?>> void dispatch(final String dispatchId, final S state) {
    dispatchAttemptCount.getAndAdd(1);
    if (processDispatch.get()) {
      access.writeUsing("dispatchedState", dispatchId, (State<?>) state);
      //System.out.println("MockObjectDispatcher::dispatch - wrote dispatchedState");
      control.confirmDispatched(dispatchId, confirmDispatchedResultInterest);
    }
  }

  public AccessSafely afterCompleting(final int times) {
    access = AccessSafely
      .afterCompleting(times)
      .writingWith("dispatchedState", (String id, Object state) -> dispatched.put(id, state))
      .readingWith("dispatchedState", (String id) -> dispatched.get(id))
      .readingWith("dispatchedStateCount", () -> dispatched.size())

      .writingWith("processDispatch", (Boolean flag) -> processDispatch.set(flag))
      .readingWith("processDispatch", () -> processDispatch.get())
      
      .readingWith("dispatchAttemptCount", () -> dispatchAttemptCount.get())

      .readingWith("dispatched", () -> dispatched);

    return access;
  }

  public State<?> dispatched(final String id) {
    final State<?> dispatched = access.readFrom("dispatchedState", id);
    return dispatched;
  }

  public int dispatchedCount() {
    Map<String,State<?>> dispatched = access.readFrom("dispatched");
    return dispatched.size();
  }

  public void dispatchUnconfirmed() {
    control.dispatchUnconfirmed();
  }
}
