package io.vlingo.symbio.store.common.geode.pdx;

import org.apache.geode.pdx.PdxSerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
