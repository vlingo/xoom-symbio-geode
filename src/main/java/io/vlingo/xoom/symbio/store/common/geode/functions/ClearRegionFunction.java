// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common.geode.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ClearRegionFunction implements Function, Declarable {
    private static final long serialVersionUID = 1L;

    private static final int BATCH_SIZE = 30000;

    @Override
    public void execute(FunctionContext ctx) {
        if (ctx instanceof RegionFunctionContext) {
            final RegionFunctionContext rfc = (RegionFunctionContext) ctx;
            try {
                final Region<Object, Object> region = rfc.getDataSet();
                if (PartitionRegionHelper.isPartitionedRegion(region)) {
                    clear(PartitionRegionHelper.getLocalDataForContext(rfc));
                } else {
                    clear(region);
                }
                ctx.getResultSender().lastResult("Success");
            } catch (final Throwable t) {
                rfc.getResultSender().sendException(t);
            }
        } else {
            ctx.getResultSender().lastResult("ERROR: The function must be executed on region!");
        }
    }

    private void clear(final Region<Object, Object> localRegion) {
        int numLocalEntries = localRegion.keySet().size();
        if (numLocalEntries <= BATCH_SIZE) {
            localRegion.removeAll(localRegion.keySet());
        } else {
            final List<Object> buffer = new ArrayList<>(BATCH_SIZE);
            int count = 0;
            for (final Object k : localRegion.keySet()) {
                buffer.add(k);
                count++;
                if (count == BATCH_SIZE) {
                    localRegion.removeAll(buffer);
                    buffer.clear();
                    count = 0;
                }
            }
            localRegion.removeAll(buffer);
        }
    }

    @Override
    public boolean hasResult() {
        return true;
    }

    @Override
    public String getId() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean optimizeForWrite() {
        return true;
    }

    @Override
    public boolean isHA() {
        return true;
    }
}
