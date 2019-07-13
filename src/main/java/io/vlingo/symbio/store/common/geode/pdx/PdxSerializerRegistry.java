// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode.pdx;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PdxSerializerRegistry is responsible for maintaining a registry of
 * {@link PdxSerializer} on a domain-type basis, and for vending
 * an appropriate instance of {@link PdxSerializer} for a given
 * fully-qualified class name.
 */
public class PdxSerializerRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(PdxSerializerRegistry.class);
  private static final Map<String, String> SerializerClassByType = new ConcurrentHashMap<>();

  public static void serializeTypeWith(final String typeFQCN, final String serializerFQCN) {
    SerializerClassByType.putIfAbsent(typeFQCN, serializerFQCN);
    LOG.info(typeFQCN + " will be serialized by " + serializerFQCN);
  }

  public static void serializeTypeWith(final Class<?> type, final Class<?> pdxSerializerClass) {
    serializeTypeWith(type.getName(), pdxSerializerClass.getName());
  }
  
  public static PdxSerializer serializerForType(final Class<?> clazz) {
    return serializerForType(clazz.getName());
  }

  public static PdxSerializer serializerForType(final String typeFQCN) {
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
