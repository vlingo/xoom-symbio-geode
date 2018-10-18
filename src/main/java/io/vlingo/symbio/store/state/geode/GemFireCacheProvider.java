// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.client.ClientCacheFactory;
/**
 * GemFireCacheProvider is responsible for vending an appropriate
 * instance of {@link GemFireCache} based on a specified
 * {@link Configuration}.
 *
 * @author davem
 * @since Oct 14, 2018
 */
public class GemFireCacheProvider {

  public static GemFireCache getAnyInstance(Configuration config) throws CouldNotAccessCacheException {
    return config.role.isPeer() ? serverCache() : clientCache();
  }

  protected static GemFireCache clientCache() throws CouldNotAccessCacheException {
    GemFireCache clientCache = null;
    try {
      clientCache = ClientCacheFactory.getAnyInstance();
    } catch (CacheClosedException ex) {
      clientCache = new ClientCacheFactory().create();
    }

    if (clientCache == null) {
      throw new CouldNotAccessCacheException("Unable to create or access existing client GemFireCache.");
    }

    return clientCache;
  }

  protected static GemFireCache serverCache() throws CouldNotAccessCacheException {
    GemFireCache serverCache = null;
    try {
      serverCache = CacheFactory.getAnyInstance();
    } catch (CacheClosedException ex) {
      serverCache = new CacheFactory().create();
    }

    if (serverCache == null) {
      throw new CouldNotAccessCacheException("Unable to create or access existing server GemFireCache.");
    }

    return serverCache;
  }

  public static class CouldNotAccessCacheException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CouldNotAccessCacheException() {
      super();
    }

    public CouldNotAccessCacheException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
    }

    public CouldNotAccessCacheException(String message, Throwable cause) {
      super(message, cause);
    }

    public CouldNotAccessCacheException(String message) {
      super(message);
    }

    public CouldNotAccessCacheException(Throwable cause) {
      super(cause);
    }
  }
}
