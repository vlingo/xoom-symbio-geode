// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode.pdx;

import io.vlingo.xoom.symbio.*;
import io.vlingo.xoom.symbio.store.common.geode.dispatch.GeodeDispatchable;
import io.vlingo.xoom.symbio.store.common.geode.dispatch.GeodeDispatchableSerializer;
import io.vlingo.xoom.symbio.store.object.geode.Person;
import io.vlingo.xoom.symbio.store.object.geode.PersonSerializer;

public class TestPdxSerializerProvider extends PdxSerializerProviderAdapter {

  public TestPdxSerializerProvider() {
    super();
  }

  @Override
  protected void registerSerializers() {
    registerSerializer(BaseEntry.TextEntry.class, TextEntrySerializer.class);
    registerSerializer(GeodeDispatchable.class, GeodeDispatchableSerializer.class);
    registerSerializer(Metadata.class, MetadataPdxSerializer.class);
    registerSerializer(Person.class, PersonSerializer.class);
    registerSerializer(State.BinaryState.class, StateSerializer.class);
    registerSerializer(State.ObjectState.class, StateSerializer.class);
    registerSerializer(State.TextState.class, StateSerializer.class);
  }
}
