// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode;
/**
 * GeodeQueries
 */
public interface GeodeQueries {
  
  String DISPATCHABLES_REGION_PATH = "/vlingo-dispatchables";
  
  String OQL_DISPATCHABLES_SELECT =
    //"<trace> " +
    "SELECT DISTINCT * FROM " +
    DISPATCHABLES_REGION_PATH +
    " WHERE originatorId = $1" +
    " ORDER BY createdOn ASC";

  String ALL_UNCONFIRMED_DISPATCHABLES_SELECT =
    "SELECT DISTINCT * FROM " +
    DISPATCHABLES_REGION_PATH  +
    " ORDER BY createdOn ASC";
}
