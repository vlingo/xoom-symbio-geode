// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.geode.pdx;

import com.google.gson.reflect.TypeToken;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatchable;
import org.apache.geode.cache.Declarable;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;

import java.time.LocalDateTime;
import java.util.List;

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
    final String originatorId = in.readString("originatorId");
    final LocalDateTime createdAt = (LocalDateTime) in.readObject("createdAt");
    final String id = in.readString("id");
    final State<?> state = JsonSerialization.deserialized(in.readString("state"), new TypeToken<State.ObjectState<?>>(){}.getType());
    final List<Entry<?>> entryList = JsonSerialization.deserialized(in.readString("entries"), new TypeToken<List<BaseEntry.ObjectEntry>>(){}.getType());
    return new GeodeDispatchable<State<?>>(originatorId, createdAt, id, state, entryList);
  }

  /* @see org.apache.geode.pdx.PdxSerializer#toData(java.lang.Object, org.apache.geode.pdx.PdxWriter) */
  @SuppressWarnings("rawtypes")
  @Override
  public boolean toData(Object o, PdxWriter out) {
    boolean result = false;
    if (o instanceof GeodeDispatchable) {
      GeodeDispatchable<State<?>> instance = (GeodeDispatchable<State<?>>) o;
      out
        .writeString("originatorId", instance.originatorId)
        .writeObject("createdAt", instance.createdOn())
        .writeString("id", instance.id())
        .markIdentityField("id")
        .writeString("state", JsonSerialization.serialized(instance.state()))
        .writeString("entries", JsonSerialization.serialized(instance.entries()));
      result = true;
    }
    return result;
  }

}
