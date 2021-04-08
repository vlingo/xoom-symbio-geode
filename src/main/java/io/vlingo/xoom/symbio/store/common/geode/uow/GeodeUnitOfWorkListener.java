// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode.uow;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * GeodeUnitOfWorkListener is responsible for listening for create events
 * on the region which stores {@link GeodeUnitOfWork} objects.
 * <p>
 * Note that {@link #processEvents(List)} method receives a batch of
 * {@link AsyncEvent}, each of which represents one {@link GeodeUnitOfWork} that
 * needs to be processed, and returns a boolean which indicates whether all
 * the events in the batch were processed correctly.  As long as {@link #processEvents(List)}
 * returns false, Geode continues to re-try processing the events.
 * <p>
 * It is important for the async event queue to be configured as serial, so that
 * events get processed in order. It is also recommended that the batch size be small.
 * To protect against loss of events, it is recommended that the async queue be
 * configured for persistence to a disk store. You may wish to experiment with
 * configuring multiple dispatcher threads but, if so, ensure that you use the
 * key ordering policy (since {@link GeodeUnitOfWork} keys are long value that
 * is allocated out of a single, monotonically-increasing ID sequence.
 * <p>
 * Here is a basic example of how to configure this listener (not all options shown):
 * <pre>
 *   &lt;async-event-queue id="object-store-uow-queue" persistent="true" disk-store-name="uowStore" parallel="false"&gt;
 *     &lt;async-event-listener&gt;
 *       &lt;class-name&gt;io.vlingo.xoom.symbio.store.common.geode.uow.GeodeUnitOfWorkListener&lt;/class-name&gt;
 *     &lt;/async-event-listener&gt;
 *   &lt;/async-event-queue&gt;
 * </pre>
 * For more information on configuring {@link AsyncEventListener}s see the
 * <a href="https://geode.apache.org/docs/guide/19/developing/events/chapter_overview.html">Events and Event Handling</a>
 * section of the Apache Geode documentation.
 */
public class GeodeUnitOfWorkListener implements AsyncEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(GeodeUnitOfWorkListener.class);
  private static final Long INVOKEALL_TIMEOUT_VALUE = 3000L;
  private static final TimeUnit INVOKEALL_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

  private Cache cache;

  @Override
  public void initialize(Cache cache, Properties properties) {
    this.cache = cache;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public boolean processEvents(final List<AsyncEvent> events) {
    LOG.debug("processEvents - entered with " + events.size() + " events");
    boolean result = false;
    try {
      List<GeodeUnitOfWorkProcessor> eventProcessors = new ArrayList<>();
      int i = 0;
      for (AsyncEvent event : events) {
        LOG.debug("processEvents - event[" + (i++) + "] = " + event);
        final Operation op = event.getOperation();
        if (op.equals(Operation.CREATE)) {
          final GeodeUnitOfWork uow = (GeodeUnitOfWork) event.getDeserializedValue();
          eventProcessors.add(new GeodeUnitOfWorkProcessor(cache, uow));
        }
      }

      try {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Optional<Boolean> booleanOrNull = executorService
          .invokeAll(eventProcessors, INVOKEALL_TIMEOUT_VALUE, INVOKEALL_TIMEOUT_UNIT)
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
      LOG.debug("processEvents - exited with result=" + result);
    }
  }
}
