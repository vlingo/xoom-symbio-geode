// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode;
/**
 * GeodeQueries
 */
public interface GeodeQueries {

  String DISPATCHABLES_REGION_PATH = "Dispatchables";
  String OBJECTSTORE_EVENT_JOURNAL_REGION_PATH = "ObjectStoreEventJournal";
  String OBJECTSTORE_UOW_REGION_PATH = "ObjectStoreUnitsOfWork";
  String STATESTORE_EVENT_JOURNAL_REGION_PATH = "StateStoreEventJournal";
  String EVENT_JOURNAL_SEQUENCE_KEY = "IdSequence";

  String OQL_DISPATCHABLES_SELECT =
    //"<trace> " +
    "SELECT DISTINCT * FROM /" +
    DISPATCHABLES_REGION_PATH +
    " WHERE originatorId = $1" +
    " ORDER BY createdOn ASC";

  String ALL_UNCONFIRMED_DISPATCHABLES_SELECT =
    "SELECT DISTINCT * FROM /" +
    DISPATCHABLES_REGION_PATH  +
    " ORDER BY createdOn ASC";
}
