// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.state.geode;

import java.util.List;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.reactivestreams.Stream;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.store.EntryReader;
import io.vlingo.xoom.symbio.store.state.StateStoreEntryReader;

public class GeodeStateStoreEntryReaderActor<T extends Entry<?>> extends Actor implements StateStoreEntryReader<T> {

  public GeodeStateStoreEntryReaderActor(final Advice advice, final String name) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public Completes<String> name() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Completes<T> readNext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Completes<T> readNext(String fromId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Completes<List<T>> readNext(int maximumEntries) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Completes<List<T>> readNext(String fromId, int maximumEntries) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void rewind() {
    // TODO Auto-generated method stub
  }

  @Override
  public Completes<String> seekTo(String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Completes<Long> size() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Completes<Stream> streamAll() {
    return null;
  }

  public static class GeodeStateStoreEntryReaderInstantiator<T extends Entry<?>> implements ActorInstantiator<GeodeStateStoreEntryReaderActor<T>> {
    private static final long serialVersionUID = 266954179434375620L;

    private final EntryReader.Advice advice;
    private final String name;

    public GeodeStateStoreEntryReaderInstantiator(final EntryReader.Advice advice, final String name) {
      this.advice = advice;
      this.name = name;
    }

    @Override
    public GeodeStateStoreEntryReaderActor<T> instantiate() {
      return new GeodeStateStoreEntryReaderActor<>(advice, name);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Class<GeodeStateStoreEntryReaderActor<T>> type() {
      return (Class) GeodeStateStoreEntryReaderActor.class;
    }
  }
}
