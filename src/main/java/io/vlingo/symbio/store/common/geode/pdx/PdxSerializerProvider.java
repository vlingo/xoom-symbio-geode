// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
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
