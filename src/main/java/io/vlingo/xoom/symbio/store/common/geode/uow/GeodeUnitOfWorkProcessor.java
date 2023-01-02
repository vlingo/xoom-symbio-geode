// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode.uow;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.GemFireCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
/**
 * GeodeUnitOfWorkProcessor is responsible for executing the
 * {@link GeodeUnitOfWork#applyTo(GemFireCache)} operation in
 * a separate thread.
 */
public class GeodeUnitOfWorkProcessor implements Callable<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(GeodeUnitOfWorkProcessor.class);

  private final Cache cache;
  private final GeodeUnitOfWork uow;

  public GeodeUnitOfWorkProcessor(final Cache cache, final GeodeUnitOfWork uow) {
    super();
    this.cache = cache;
    this.uow = uow;
  }

  @Override
  public Boolean call() throws Exception {
    LOG.debug("call - entered");
    boolean result = false;
    try {
      try {
        uow.applyTo(cache);
        result = true;
      } catch (Throwable t) {
        LOG.error("call - error applying " + uow, t);
      }
      return result;
    } finally {
      LOG.debug("call - exited with result=" + result);
    }
  }
}
