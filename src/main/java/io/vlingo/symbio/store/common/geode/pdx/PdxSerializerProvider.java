package io.vlingo.symbio.store.common.geode.pdx;

import org.apache.geode.pdx.PdxSerializer;

public interface PdxSerializerProvider {
  PdxSerializer serializerForType(final Class<?> clazz);
  PdxSerializer serializerForType(final String typeFQCN);

  class NoPdxSerializerProvider implements PdxSerializerProvider {

    @Override
    public PdxSerializer serializerForType(Class<?> clazz) {
      throw new RuntimeException("No 'provider' has been configured for DynaPdxSerializer");
    }

    @Override
    public PdxSerializer serializerForType(String typeFQCN) {
      throw new RuntimeException("No 'provider' has been configured for DynaPdxSerializer");
    }
  }
}
