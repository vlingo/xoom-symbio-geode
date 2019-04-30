// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode.pdx;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.Declarable;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;
/**
 * DynaPdxSerializer is a {@link PdxSerializer} that delegates to an
 * appropriate kind of {@link PdxSerializer}, as determined by the
 * configuration held in {@link PdxSerializerRegistry}.
 */
public class DynaPdxSerializer implements PdxSerializer, Declarable {
  
  private Map<String, PdxSerializer> serializersByType = new ConcurrentHashMap<>();
  
  public DynaPdxSerializer() {
    super();
  }

  /* @see org.apache.geode.pdx.PdxSerializer#toData(java.lang.Object, org.apache.geode.pdx.PdxWriter) */
  @Override
  public boolean toData(Object o, PdxWriter out) {
    return serializerFor(o).toData(o, out);
  }

  /* @see org.apache.geode.pdx.PdxSerializer#fromData(java.lang.Class, org.apache.geode.pdx.PdxReader) */
  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    return serializerFor(clazz).fromData(clazz, in);
  }

  private PdxSerializer serializerFor(final Object o) {
    return serializerFor(o.getClass());
  }
  
  private PdxSerializer serializerFor(Class<?> c) {
    String fqcn = c.getName().replace("$", ".");
    PdxSerializer serializer = serializersByType.get(fqcn);
    if (serializer == null) {
      serializer = PdxSerializerRegistry.serializerForType(fqcn);
      serializersByType.put(fqcn, serializer);
      System.out.println(fqcn + " will be serialized by " + serializer);
    }
    return serializer;
  }
}
