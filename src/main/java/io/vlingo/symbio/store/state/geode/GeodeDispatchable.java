// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.geode;

import io.vlingo.symbio.State;
import io.vlingo.symbio.store.state.StateStore.Dispatchable;
/**
 * GeodeDispatchable
 */
public class GeodeDispatchable<R extends State<?>> extends Dispatchable<R> {
  
  public final long writtenAt;
  public final String originatorId;
  
  public GeodeDispatchable(final String originatorId, final long writtenAt, final String id, final R state) {
    super(id, state);
    this.originatorId = originatorId;
    this.writtenAt = writtenAt;
  }

  /* @see java.lang.Object#toString() */
  @Override
  public String toString() {
    return new StringBuilder()
      .append("GeodeDispatchable(")
      .append("originatorId=").append(originatorId)
      .append(", writtenAt=").append(writtenAt)
      .append(", id=").append(id)
      .append(", state=").append(state)
      .append(")")
      .toString();
  }

}
