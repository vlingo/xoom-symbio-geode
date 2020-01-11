// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio;

import org.apache.geode.cache.Declarable;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;
/**
 * MetadataPdxSerializer is responsible for serializing instances of
 * {@link Metadata}. It is necessary to customize the PDX serialization
 * of {@link Metadata} to avoid serializing {@link Metadata#EmptyObject}.
 */
public class MetadataPdxSerializer implements PdxSerializer, Declarable {

  /**
   * Constructs a MetadataPdxSerializer.
   */
  public MetadataPdxSerializer() {
    super();
  }

  /* @see org.apache.geode.pdx.PdxSerializer#toData(java.lang.Object, org.apache.geode.pdx.PdxWriter) */
  @Override
  public boolean toData(Object o, PdxWriter out) {
    boolean result = false;
    if (o instanceof Metadata) {
      Metadata instance = (Metadata) o;
      if (instance.object != Metadata.EmptyObject) {
        out.writeObject("object", instance.object);
      }
      out
        .writeString("operation", instance.operation)
        .writeString("value", instance.value);
      result = true;
    }
    return result;
  }

  /* @see org.apache.geode.pdx.PdxSerializer#fromData(java.lang.Class, org.apache.geode.pdx.PdxReader) */
  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    Object object = in.readObject("object");
    String operation = in.readString("operation");
    String value = in.readString("value");
    return (object == null)
      ? new Metadata(value, operation)
      : new Metadata(object, value, operation);
  }
}
