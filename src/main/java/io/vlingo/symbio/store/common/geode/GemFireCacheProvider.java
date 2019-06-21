// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode;

import java.util.Optional;

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

  /**
   * Returns an {@link Optional} referencing the singleton
   * {@link GemFireCache} existing in this JVM or null if
   * no cache has been created yet.
   *
   * @return a {@link GemFireCache} or null
   */
  public static Optional<GemFireCache> getAnyInstance() {
    GemFireCache cache = null;
    try {
      cache = ClientCacheFactory.getAnyInstance();
    } catch (Throwable t) {
      try {
        cache = CacheFactory.getAnyInstance();
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
    ClientCache clientCache = null;
    try {
      clientCache = ClientCacheFactory.getAnyInstance();
    } catch (Throwable t) {
      try {
        clientCache = new ClientCacheFactory().create();
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
    Cache serverCache = null;
    try {
      serverCache = CacheFactory.getAnyInstance();
    } catch (Throwable t) {
      try {
        serverCache = new CacheFactory().create();
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
