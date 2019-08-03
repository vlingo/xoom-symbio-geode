// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode;

import java.util.Optional;
import java.util.Properties;

import io.vlingo.actors.Logger;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
/**
 * GemFireCacheProvider is responsible for vending an appropriate
 * instance of {@link GemFireCache}.
 */
public class GemFireCacheProvider {

  private static final Properties DEFAULT_PROPERTIES = new Properties(System.getProperties());
  private static final Logger LOG = Logger.basicLogger();

  /**
   * Returns an {@link Optional} referencing the singleton
   * {@link GemFireCache} existing in this JVM or null if
   * no cache has been created yet.
   *
   * @return a {@link GemFireCache} or null
   */
  public static Optional<GemFireCache> getAnyInstance() {
    return getAnyInstance(DEFAULT_PROPERTIES);
  }

  /**
   * Returns an {@link Optional} referencing the singleton
   * {@link GemFireCache} existing in this JVM or null if
   * no cache has been created yet.
   *
   * @param properties the possibly empty {@link Properties} with which
   *                   to configure the cache
   *
   * @return a {@link GemFireCache} or null
   */
  public static Optional<GemFireCache> getAnyInstance(final Properties properties) {
    GemFireCache cache;
    try {
      cache = forClient(properties);
    }
    catch (Throwable t) {
      try {
        cache = forPeer(properties);
      }
      catch (Throwable t2) {
        cache = null;
      }
    }
    return Optional.ofNullable(cache);
  }

  /**
   * Returns the singleton {@link ClientCache} in this JVM or
   * attempts to create a new one. Throws {@link CouldNotAccessCacheException}
   * if a server {@link Cache} was already created in this JVM
   * or if the attempt to create a {@link ClientCache} fails
   * for any reason.
   *
   * @return a {@link ClientCache}
   */
  public static ClientCache forClient() throws CouldNotAccessCacheException {
    return forClient(DEFAULT_PROPERTIES);
  }
  /**
   * Returns the singleton {@link ClientCache} in this JVM or
   * attempts to create a new one. Throws {@link CouldNotAccessCacheException}
   * if a server {@link Cache} was already created in this JVM
   * or if the attempt to create a {@link ClientCache} fails
   * for any reason.
   *
   * @param properties the possibly empty {@link Properties} with which
   *                   to configure the cache
   *
   * @return a {@link ClientCache}
   */
  public static ClientCache forClient(final Properties properties) throws CouldNotAccessCacheException {
    ClientCache clientCache;
    try {
      clientCache = ClientCacheFactory.getAnyInstance();
      LOG.trace("returning EXISTING ClientCache");
    }
    catch (Throwable t) {
      try {
        clientCache = new ClientCacheFactory(properties).create();
        LOG.trace("returning NEW ClientCache");
      }
      catch (Throwable t2) {
        throw new CouldNotAccessCacheException("Unable to create or access existing ClientCache.", t2);
      }
    }
    return clientCache;
  }

  /**
   * Returns the singleton {@link Cache} in this JVM or attempts to
   * create a new one. Throws {@link CouldNotAccessCacheException}
   * if a {@link ClientCache} was already created in this JVM
   * or if the attempt to create a {@link Cache} fails
   * for any reason.
   *
   * @return a {@link Cache}
   */
  public static Cache forPeer() throws CouldNotAccessCacheException {
    return forPeer(DEFAULT_PROPERTIES);
  }

  /**
   * Returns the singleton {@link Cache} in this JVM or attempts to
   * create a new one. Throws {@link CouldNotAccessCacheException}
   * if a {@link ClientCache} was already created in this JVM
   * or if the attempt to create a {@link Cache} fails
   * for any reason.
   *
   * @param properties the possibly empty {@link Properties} with which
   *                   to configure the cache
   *
   * @return a {@link Cache}
   */
  public static Cache forPeer(Properties properties) throws CouldNotAccessCacheException {
    Cache serverCache;
    try {
      serverCache = CacheFactory.getAnyInstance();
      LOG.trace("returning EXISTING server Cache");
    }
    catch (Throwable t) {
      try {
        serverCache = new CacheFactory(properties).create();
        LOG.trace("returning NEW server Cache");
      }
      catch (Throwable t2) {
        throw new CouldNotAccessCacheException("Unable to create or access existing Cache.", t2);
      }
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
