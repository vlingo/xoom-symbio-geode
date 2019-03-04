// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state;

import java.time.LocalDateTime;

import org.apache.geode.cache.Declarable;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;

import io.vlingo.symbio.State;
import io.vlingo.symbio.store.state.geode.GeodeDispatchable;
/**
 * GeodeDispatchableSerializer is responsible for serializing instances of
 * {@link GeodeDispatchable}.
 */
public class GeodeDispatchableSerializer implements PdxSerializer, Declarable {

  public GeodeDispatchableSerializer() {
    super();
  }

  /* @see org.apache.geode.pdx.PdxSerializer#fromData(java.lang.Class, org.apache.geode.pdx.PdxReader) */
  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    String originatorId = in.readString("originatorId");
    LocalDateTime createdAt = (LocalDateTime) in.readObject("createdAt");
    String id = in.readString("id");
    State<?> state = (State<?>) in.readObject("state");
    return new GeodeDispatchable<State<?>>(originatorId, createdAt, id, state);
  }

  /* @see org.apache.geode.pdx.PdxSerializer#toData(java.lang.Object, org.apache.geode.pdx.PdxWriter) */
  @SuppressWarnings("rawtypes")
  @Override
  public boolean toData(Object o, PdxWriter out) {
    boolean result = false;
    if (o instanceof GeodeDispatchable) {
      GeodeDispatchable instance = (GeodeDispatchable) o;
      out
        .writeString("originatorId", instance.originatorId)
        .writeObject("createdAt", instance.createdAt)
        .writeString("id", instance.id)
        .writeObject("state", instance.state);
      result = true;
    }
    return result;
  }

}
