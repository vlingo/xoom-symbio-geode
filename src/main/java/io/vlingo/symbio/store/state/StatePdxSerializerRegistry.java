// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
/**
 * StatePdxSerializerMap is responsible for maintaining a registry of
 * {@link PdxSerializer} on a per domain-type basis, and for vending
 * an appropriate instance of {@link PdxSerializer} for a given
 * fully-qualified class name.
 *
 * @author davem
 * @since Oct 13, 2018
 */
public class StatePdxSerializerRegistry {
  
  private static final Map<String, String> SerializerClassByType = new ConcurrentHashMap<>();

  public static void serializeTypeWith(String typeFQCN, String serializerFQCN) {
    SerializerClassByType.putIfAbsent(typeFQCN, serializerFQCN);
  }

  public static void serializeTypeWith(Class<?> type, Class<?> pdxSerializerClass) {
    serializeTypeWith(type.getName(), pdxSerializerClass.getName());
  }
  
  public static PdxSerializer serializerForType(Class<?> clazz) {
    return serializerForType(clazz.getName());
  }

  public static PdxSerializer serializerForType(String typeFQCN) {
    PdxSerializer serializer = null;
    String serializerFQCN = SerializerClassByType.get(typeFQCN);
    if (serializerFQCN != null) {
      try {
        serializer = (PdxSerializer) Class.forName(serializerFQCN).newInstance();
      } catch (Exception ex) {
        throw new PdxSerializationException("error instantiating serializer of class " + serializerFQCN);
      }
    }
    else {
      serializer = new ReflectionBasedAutoSerializer(typeFQCN);
    }
    return serializer;
  }
}
