// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.common.geode.pdx;

import org.apache.geode.pdx.PdxSerializationException;

import java.io.FileInputStream;
import java.util.Properties;

public class PropertyPdxSerializerProvider extends PdxSerializerProviderAdapter {

  private static final String PROPERTY_FILENAME = "pdx.properties";

  public PropertyPdxSerializerProvider() {
    super();
  }

  @Override
  protected void registerSerializers() {
    try {
      Properties properties = new Properties();
      properties.load(new FileInputStream(PROPERTY_FILENAME));
      for (final String dataClassName : properties.stringPropertyNames()) {
        final String serializerClassName = properties.getProperty(dataClassName);
        registerSerializer(dataClassName, serializerClassName);
      }
    }
    catch (Throwable t) {
      throw new PdxSerializationException("error loading pdx serializer mappings from " + PROPERTY_FILENAME, t);
    }
  }
}
