// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;
/**
 * GeodePersistentObjectMapping
 */
public class GeodePersistentObjectMapping {
  
  public final String regionPath;

  public GeodePersistentObjectMapping(final String regionPath) {
    if (regionPath == null || regionPath.isEmpty())
      throw new IllegalArgumentException("regionPath must be a non-empty String");
    this.regionPath = regionPath;
  }
}
