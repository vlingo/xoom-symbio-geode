// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.symbio.Metadata;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;

import java.time.LocalDate;

public class GeodeEventJournalEntrySerializer implements PdxSerializer {

  @Override
  public boolean toData(Object o, PdxWriter out) {
    boolean result = false;
    if (o instanceof GeodeEventJournalEntry) {
      final GeodeEventJournalEntry entry = (GeodeEventJournalEntry) o;
      out
        .writeString("id", entry.id())
        .markIdentityField("id")
        .writeObject("entryTimestamp", entry.entryTimestamp())
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
    final LocalDate entryTimestamp = (LocalDate) in.readObject("entryTimestamp");
    final String entryData = in.readString("entryData");
    final Metadata metadata = (Metadata) in.readObject("metadata");
    final Class<?> type = computeType(in.readString("type"));
    final int typeVersion = in.readInt("typeVersion");
    return new GeodeEventJournalEntry(id, entryTimestamp, type, typeVersion, entryData, metadata);
  }

  private Class<?> computeType(final String typeFQCN) {
    try {
      return Class.forName(typeFQCN);
    }
    catch (Throwable t) {
      throw new PdxSerializationException("error loading class " + typeFQCN, t);
    }
  }
}
