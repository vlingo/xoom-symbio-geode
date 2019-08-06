// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode.pdx;

import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class PdxSerializerProviderAdapter implements PdxSerializerProvider {

  private Map<String, PdxSerializer> serializersByType = new HashMap<>();
  private Logger logger;

  public PdxSerializerProviderAdapter() {
    super();
    registerSerializers();
  }

  protected abstract void registerSerializers() throws PdxSerializationException;

  @Override
  public PdxSerializer serializerForType(final Class<?> clazz) {
    return serializerForType(clazz.getName());
  }

  @Override
  public PdxSerializer serializerForType(final String typeFQCN) {
    PdxSerializer serializer = serializersByType.get(typeFQCN);
    if (serializer == null) {
      serializer = defaultSerializerForType(typeFQCN);
      serializersByType.put(typeFQCN, serializer);
      logger().debug(typeFQCN + " will be serialized by " + serializer.getClass().getName());
    }
    return serializer;
  }

  protected PdxSerializer defaultSerializerForType(final String typeFQCN) {
    return new ReflectionBasedAutoSerializer(typeFQCN);
  }

  public void registerSerializer(final Class<?> dataClass, final Class<?> serializerClass) throws PdxSerializationException {
    registerSerializer(dataClass.getName(), serializerClass);
  }

  public void registerSerializer(final String dataClassName, final Class<?> serializerClass) throws PdxSerializationException {
    try {
      serializersByType.put(dataClassName, (PdxSerializer) serializerClass.newInstance());
      logger().debug(dataClassName + " will be serialized by " + serializerClass.getName());
    } catch (Exception ex) {
      throw new PdxSerializationException("error instantiating serializer of class " + serializerClass.getName());
    }
  }

  public void registerSerializer(final String dataClassName, final String serializerClassName) throws PdxSerializationException {
    try {
      Class<?> serializerClass = Class.forName(serializerClassName);
      registerSerializer(dataClassName, serializerClass);
    }
    catch (Throwable t) {
      throw new PdxSerializationException("error loading serializer class " + serializerClassName, t);
    }
  }

  protected Logger logger() {
    if (logger == null) {
      logger = LoggerFactory.getLogger(getClass());
    }
    return logger;
  }
}
