// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode.uow;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * GeodeUnitOfWorkListener is responsible for listening for events
 * on
 */
public class GeodeUnitOfWorkListener implements AsyncEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(GeodeUnitOfWorkListener.class);

  private Cache cache;

  @Override
  public void initialize(Cache cache, Properties properties) {
    this.cache = cache;
  }

  @Override
  public boolean processEvents(final List<AsyncEvent> events) {
    LOG.info("processEvents - entered with " + events.size() + " events");
    boolean result = false;
    try {
      List<GeodeUnitOfWorkProcessor> eventProcessors = new ArrayList<>();
      int i = 0;
      for (AsyncEvent event : events) {
        LOG.info("processEvents - event[" + (i++) + "] = " + event);
        final Operation op = event.getOperation();
        if (op.equals(Operation.CREATE)) {
          final GeodeUnitOfWork uow = (GeodeUnitOfWork) event.getDeserializedValue();
          eventProcessors.add(new GeodeUnitOfWorkProcessor(cache, uow));
        }
      }

      try {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Optional<Boolean> booleanOrNull = executorService
          .invokeAll(eventProcessors)
          .stream()
          .map(future -> {
            try {
              return future.get();
            } catch (Throwable t) {
              return false;
            }
          })
          .reduce((result1, result2) -> result1 && result2);
        result = booleanOrNull.isPresent() && booleanOrNull.get();
      } catch (Throwable t) {
        LOG.error("error processing events, t");
      }

      return result;
    }
    finally {
      LOG.info("processEvents - exited with result=" + result);
    }
  }
}
