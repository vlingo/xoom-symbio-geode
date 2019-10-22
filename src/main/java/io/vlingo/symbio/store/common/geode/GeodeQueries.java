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

  /* GemFire Region Paths */
  String DISPATCHABLES_REGION_PATH = "Dispatchables";
  String EVENT_JOURNAL_REGION_PATH = "EventJournal";
  String UOW_REGION_PATH = "UnitsOfWork";
  String SEQUENCE_REGION_PATH = "IdSequences";

  /* ID Sequence Names */
  String ENTRY_SEQUENCE_NAME = "Entries";
  String UOW_SEQUENCE_NAME = "UnitsOfWork";

  /* Queries */
  String ALL_UNCONFIRMED_DISPATCHABLES_SELECT = "SELECT DISTINCT * FROM /" + DISPATCHABLES_REGION_PATH  + " ORDER BY createdOn ASC";
}
