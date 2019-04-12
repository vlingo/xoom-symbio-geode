// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode;

import io.vlingo.common.Completes;
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
}
