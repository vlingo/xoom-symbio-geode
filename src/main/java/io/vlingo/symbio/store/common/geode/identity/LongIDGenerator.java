// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode.identity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.logging.log4j.util.Strings;

import io.vlingo.symbio.store.common.geode.Configuration;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
/**
 * LongIDGenerator is responsible for generating identifiers of
 * type {@link Long} from a named sequence.
 */
public class LongIDGenerator {
  
  private static final Long DEFAULT_ALLOCATION_SIZE = 100L;

  private final GemFireCache cache;
  private final String sequenceRegionPath;
  private final Long allocationSize;
  private final Map<String, LongIDAllocation> allocationsByName;

  public LongIDGenerator(final Configuration config, final String sequenceRegionPath) {
    this(config, sequenceRegionPath, DEFAULT_ALLOCATION_SIZE);
  }
  
  public LongIDGenerator(final Configuration config, final String sequenceRegionPath, final Long allocationSize) {
    super();
    if (config == null)
      throw new IllegalArgumentException("config is required");
    this.cache = GemFireCacheProvider.getAnyInstance(config);
    if (Strings.isBlank(sequenceRegionPath))
      throw new IllegalArgumentException("sequenceRegionPath is required");
    this.sequenceRegionPath = sequenceRegionPath;
    if (allocationSize == null)
      this.allocationSize = DEFAULT_ALLOCATION_SIZE;
    else
      this.allocationSize = allocationSize;
    this.allocationsByName = new HashMap<String, LongIDAllocation>();
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
    
    Set<String> filter = new HashSet<String>();
    filter.add(sequenceName);

    Region<String, LongSequence> region = cache.getRegion(sequenceRegionPath);
    ResultCollector<LongIDAllocation, List<LongIDAllocation>> rc = FunctionService
      .onRegion(region)
      .withFilter(filter)
      .setArguments(allocationSize)
      .execute(LongIDAllocator.class.getName());

    List<LongIDAllocation> result = (List<LongIDAllocation>) rc.getResult();
    return result.get(0);
  }
}
