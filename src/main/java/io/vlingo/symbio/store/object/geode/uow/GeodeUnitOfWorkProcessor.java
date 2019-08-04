package io.vlingo.symbio.store.object.geode.uow;

import org.apache.geode.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

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
    LOG.info("call - entered");
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
      LOG.info("call - exited with result=" + result);
    }
  }
}
