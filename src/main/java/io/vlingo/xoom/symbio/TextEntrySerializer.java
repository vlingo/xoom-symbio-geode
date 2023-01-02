// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio;

import io.vlingo.xoom.symbio.store.StoredTypes;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;

public class TextEntrySerializer implements PdxSerializer {

  @Override
  public boolean toData(Object o, PdxWriter out) {
    boolean result = false;
    if (o instanceof BaseEntry.TextEntry) {
      final BaseEntry.TextEntry entry = (BaseEntry.TextEntry) o;
      out
        .writeString("id", entry.id())
        .markIdentityField("id")
        .writeString("entryData", entry.entryData())
        .writeObject("metadata", entry.metadata())
        .writeString("typeName", entry.type())
        .writeInt("typeVersion", entry.typeVersion());
      result = true;
    }
    return result;
  }

  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    final String id = in.readString("id");
    final String entryData = in.readString("entryData");
    final Metadata metadata = (Metadata) in.readObject("metadata");
    final Class<?> type = computeType(in.readString("typeName"));
    final int typeVersion = in.readInt("typeVersion");
    return new BaseEntry.TextEntry(id, type, typeVersion, entryData, metadata);
  }

  private Class<?> computeType(final String typeFQCN) {
    if (typeFQCN == null || typeFQCN.isEmpty())
      throw new PdxSerializationException(getClass().getName() + ".computeType - cannot compute type because typeFQCN is null or empty");
    try {
      return StoredTypes.forName(typeFQCN);
    }
    catch (Throwable t) {
      throw new PdxSerializationException(getClass().getName() + ".computeType - error executing StoredTypes.forName(" + typeFQCN + ")", t);
    }
  }
}
