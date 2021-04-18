// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode.identity;

import io.vlingo.xoom.symbio.store.common.geode.GemFireCacheProvider;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;

import java.util.*;
/**
 * LongIDGenerator is responsible for generating identifiers of
 * type {@link Long} from a named sequence.
 */
public class LongIDGenerator {

  public static final String DEFAULT_SEQUENCE_REGION_PATH = "/IdSequences";
  public static final Long DEFAULT_ALLOCATION_SIZE = 100L;

  private final String sequenceRegionPath;
  private final Long allocationSize;
  private final Map<String, LongIDAllocation> allocationsByName;

  public static Long nextID(final String sequenceName) {
    return new LongIDGenerator(DEFAULT_SEQUENCE_REGION_PATH, 1L).next(sequenceName);
  }
  
  public static Long nextID(final String path, final String sequenceName) {
    return new LongIDGenerator(DEFAULT_SEQUENCE_REGION_PATH, 1L).next(sequenceName);
  }
  
  public LongIDGenerator() {
    this(DEFAULT_SEQUENCE_REGION_PATH, DEFAULT_ALLOCATION_SIZE);
  }
  
  public LongIDGenerator(final String sequenceRegionPath) {
    this(sequenceRegionPath, DEFAULT_ALLOCATION_SIZE);
  }
  
  public LongIDGenerator(final Long allocationSize) {
    this(DEFAULT_SEQUENCE_REGION_PATH, allocationSize);
  }
  
  public LongIDGenerator(final String sequenceRegionPath, final Long allocationSize) {
    super();
    if (isBlank(sequenceRegionPath))
      throw new IllegalArgumentException("sequenceRegionPath is required");
    this.sequenceRegionPath = sequenceRegionPath;
    if (allocationSize == null)
      this.allocationSize = DEFAULT_ALLOCATION_SIZE;
    else
      this.allocationSize = allocationSize;
    this.allocationsByName = new HashMap<>();
  }

  boolean isBlank(String string) {
    return string == null || string.isEmpty();
  }
  
  /**
   * Return the number of identifiers that will be returned by
   * each allocation operation.
   * 
   * @return the number of identifiers that will be returned by
   * each allocation operation
   */
  public Long allocationSize() {
    return allocationSize;
  }

  /**
   * Return the next Long-valued identifier for the sequence
   * named {@code sequenceName}
   * 
   * @param sequenceName the name of the sequence from which
   * an identifier is requested
   * 
   * @return the next Long-valued identifier for the sequence
   * named {@code sequenceName}
   */
  public Long next(String sequenceName) {
    return allocationFor(sequenceName).next();
  }
  
  /**
   * Returns the {@link LongIDAllocation} for the sequence named
   * {@code sequenceName}.
   * 
   * @param sequenceName the name of the sequence for which an
   * allocation is requested
   * 
   * @return the {@link LongIDAllocation} for the sequence named
   * {@code sequenceName}
   */
  private LongIDAllocation allocationFor(final String sequenceName) {
    LongIDAllocation allocation = allocationsByName.get(sequenceName);
    if (allocation == null || !allocation.hasNext()) {
      allocation = computeAllocation(sequenceName);
      allocationsByName.put(sequenceName, allocation);
    }
    return allocation;
  }
  
  /**
   * Obtain a new {@link LongIDAllocation} from the sequence named
   * {@code sequenceName}
   * 
   * @param sequenceName the name of the sequence for which an
   * allocation is requested
   * 
   * @return a new {@link LongIDAllocation} from the sequence named
   * {@code sequenceName}
   */
  @SuppressWarnings("unchecked")
  private LongIDAllocation computeAllocation(String sequenceName) {
    
    Set<String> filter = new HashSet<>();
    filter.add(sequenceName);

    Region<String, LongSequence> region = cache().getRegion(sequenceRegionPath);
    ResultCollector<LongIDAllocation, List<LongIDAllocation>> rc = FunctionService
      .onRegion(region)
      .withFilter(filter)
      .setArguments(allocationSize)
      .execute(LongIDAllocator.class.getName());

    List<LongIDAllocation> result = rc.getResult();
    return result.get(0);
  }
  
  private GemFireCache cache() {
    Optional<GemFireCache> cacheOrNull = GemFireCacheProvider.getAnyInstance();
    if (cacheOrNull.isPresent()) {
      return cacheOrNull.get();
    }
    else {
      throw new RuntimeException("no GemFireCache has been created in this JVM");
    }
  }
}
