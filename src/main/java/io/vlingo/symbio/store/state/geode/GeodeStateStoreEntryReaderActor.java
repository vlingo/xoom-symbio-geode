// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.geode;

import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.state.StateStoreEntryReader;

public class GeodeStateStoreEntryReaderActor<T extends Entry<?>> extends Actor implements StateStoreEntryReader<T> {

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
  public Completes<List<T>> readNext(int maximumEntries) {
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
    return null;
  }
}
