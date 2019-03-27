// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state;

import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State.ObjectState;
import io.vlingo.symbio.StateAdapter;

public class Entity1 {
  public String id;
  public int value;
  
  public Entity1() {
    super();
  }

  public Entity1(final String id, final int value) {
    this.id = id;
    this.value = value;
  }

  /* @see java.lang.Object#hashCode() */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + value;
    return result;
  }

  /* @see java.lang.Object#equals(java.lang.Object) */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Entity1 other = (Entity1) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (value != other.value)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Entity1[id=" + id + " value=" + value + "]";
  }

  public static class Entity1StateAdapter implements StateAdapter<Entity1,ObjectState<Object>> {

    @Override
    public int typeVersion() {
      return 1;
    }

    @Override
    public Entity1 fromRawState(final ObjectState<Object> raw) {
      return (Entity1) raw.data;
    }

    @Override
    public ObjectState<Object> toRawState(final Entity1 state, final int stateVersion) {
      return toRawState(state, stateVersion, Metadata.with("value", "op"));
    }

    @Override
    public ObjectState<Object> toRawState(final Entity1 state, final int stateVersion, final Metadata metadata) {
      return new ObjectState<Object>(state.id, Entity1.class, typeVersion(), state, stateVersion, metadata);
    }

    @Override
    public <ST> ST fromRawState(ObjectState<Object> raw, Class<ST> stateType) {
      return (ST) raw.data;
    }
  }
}
