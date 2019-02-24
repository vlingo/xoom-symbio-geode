// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;
/**
 * GeodeQueries
 */
public interface GeodeQueries {
  
  static final String DISPATCHABLES_REGION_NAME = "vlingo-dispatchables";
  
  static final String OQL_DISPATCHABLES_SELECT =
    "SELECT DISTINCT * FROM /" +
    DISPATCHABLES_REGION_NAME +
    " ORDER BY writtenAt ASC";
}
