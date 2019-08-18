// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode.pdx;

import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DynaPdxSerializer is a {@link PdxSerializer} that delegates to an
 * appropriate kind of {@link PdxSerializer}, as determined by the
 * configured {@link PdxSerializerProvider}.
 * &nbsp;
 * Configure DynaPdxSerializer by adding the following stanza to
 * the client and server cache.xml files:
 * <pre>
 *   &lt;pdx&gt;
 *     &lt;pdx-serializer&gt;
 *       &lt;class-name&gt;io.vlingo.symbio.store.common.geode.pdx.DynaPdxSerializer&lt;/class-name&gt;
 *       &lt;parameter name="provider"&gt;
 *         &lt;string&gt;io.myco.PdxSerializerProviderImpl&lt;/string&gt;
 *         &lt;/parameter&gt;
 *     &lt;/pdx-serializer&gt;
 *  &lt;/pdx&gt;
 * </pre>
 * Subclass {@link PdxSerializerProviderAdapter} to easily implement a provider.
 */
@SuppressWarnings("unused")
public class DynaPdxSerializer implements PdxSerializer, Declarable {

  private static final Logger LOG = LoggerFactory.getLogger(DynaPdxSerializer.class);
  private static final String PROVIDER_PROPERTY_NAME = "provider";

  private PdxSerializerProvider provider;

  public DynaPdxSerializer() {
    super();
  }

  @Override
  public void initialize(Cache cache, Properties properties) {
    final String providerFQCN = properties.getProperty(PROVIDER_PROPERTY_NAME);
    try {
      provider = (PdxSerializerProvider) Class.forName(providerFQCN).newInstance();
    }
    catch (Throwable t) {
      LOG.error("unable to load configured PdxSerializerProvider: " + providerFQCN);
      provider = new PdxSerializerProvider.NoPdxSerializerProvider();
    }
  }

  /* @see org.apache.geode.pdx.PdxSerializer#toData(java.lang.Object, org.apache.geode.pdx.PdxWriter) */
  @Override
  public boolean toData(Object o, PdxWriter out) {
    try {
      return serializerFor(o).toData(o, out);
    }
    catch (PdxSerializationException ex) {
      LOG.error("error serializing instance of " + o.getClass().getName(), ex);
      throw ex;
    }
    catch (Throwable t) {
      LOG.error("error serializing instance of " + o.getClass(), t);
      throw new PdxSerializationException("error serializing instance of " + o.getClass().getName());
    }
  }

  /* @see org.apache.geode.pdx.PdxSerializer#fromData(java.lang.Class, org.apache.geode.pdx.PdxReader) */
  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    return serializerFor(clazz).fromData(clazz, in);
  }

  private PdxSerializer serializerFor(final Object o) {
    return serializerFor(o.getClass());
  }

  private PdxSerializer serializerFor(final Class<?> c) {
    return provider.serializerForType(c);
  }

  private PdxSerializer serializerFor(final String fqcn) {
    return provider.serializerForType(fqcn);
  }
}
