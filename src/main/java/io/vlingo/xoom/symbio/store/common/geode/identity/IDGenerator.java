// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode.identity;

import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.common.Completes;
/**
 * IDGenerator is responsible for vending unique identifiers of type {@code T}
 * from a named sequence.
 *
 * @param T the type of the identifiers vended by this generator
 */
public interface IDGenerator<T> {
  /**
   * Returns the next identifier of type {@code T} from the
   * sequence named {@code sequenceName}.
   *
   * @param sequenceName the name of the sequence from which
   * to allocate the identifier
   *
   * @return the next identifier of type {@code T} from the
   * sequence named {@code sequenceName}
   */
  Completes<T> next(final String sequenceName);

  static class LongIDGeneratorInstantiator implements ActorInstantiator<LongIDGeneratorActor> {
    private static final long serialVersionUID = 7401987097615842523L;

    private final long startingWithId;

    public LongIDGeneratorInstantiator(final long startingWithId) {
      this.startingWithId = startingWithId;
    }

    @Override
    public LongIDGeneratorActor instantiate() {
      return new LongIDGeneratorActor(startingWithId);
    }

    @Override
    public Class<LongIDGeneratorActor> type() {
      return LongIDGeneratorActor.class;
    }
  }
}
