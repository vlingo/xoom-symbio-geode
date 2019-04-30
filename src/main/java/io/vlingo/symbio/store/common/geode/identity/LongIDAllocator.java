// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.geode.identity;

import java.util.Set;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.execute.ResultSender;
/**
 * LongIDAllocator is a {@link Function} that computes and returns
 * a {@link LongIDAllocation} from a named {@link LongSequence}.
 */
public class LongIDAllocator implements Function<Long>, Declarable {

  private static final long serialVersionUID = 1L;

  /*
   * @see org.apache.geode.cache.execute.Function#execute(org.apache.geode.cache.
   * execute.FunctionContext)
   */
  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext<Long> fc) {
    
    assert fc instanceof RegionFunctionContext : "expected to be RegionFunctionContext";
    RegionFunctionContext rfc = (RegionFunctionContext) fc;

    Region<String, LongSequence> region = rfc.getDataSet();
    Long size = (Long) rfc.getArguments();
    
    ResultSender<LongIDAllocation> rs = rfc.getResultSender();

    Set<String> keys = (Set<String>) rfc.getFilter();
    for (String key : keys) {
      LongSequence sequence = region.get(key);
      if (sequence == null) {
        sequence = new LongSequence(key);
      }

      LongIDAllocation allocation = sequence.allocate(size);
      region.put(key, sequence);

      rs.lastResult(allocation);
    }
  }
}
