// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

import java.time.LocalDate;
import java.util.Comparator;
/**
 *
 */
public class GeodeEventJournalEntry implements Entry<String>, PdxSerializable {

  private String id;
  private LocalDate entryTimestamp;
  private String entryData;
  private Metadata metadata;
  private String type;
  private int typeVersion;

  public GeodeEventJournalEntry() {
    super();
  }

  public GeodeEventJournalEntry(final Entry<?> entry) {
    this.entryTimestamp = LocalDate.now();
    this.entryData = JsonSerialization.serialized(entry.entryData());
    this.metadata = entry.metadata();
    this.type = entry.type();
    this.typeVersion = entry.typeVersion();
  }

  public GeodeEventJournalEntry(final String id, final LocalDate entryTimestamp, final Class<?> type, final int typeVersion, final Object entryData, final Metadata metadata) {
    this(id, entryTimestamp, type, typeVersion, JsonSerialization.serialized(entryData), metadata);
  }

  public GeodeEventJournalEntry(final String id, final LocalDate entryTimestamp, final Class<?> type, final int typeVersion, final String entryData, final Metadata metadata) {
    if (id == null) throw new IllegalArgumentException("Entry id must not be null.");
    this.id = id;
    if (entryTimestamp == null) throw new IllegalArgumentException("Entry timestamp must not be null.");
    this.entryTimestamp = entryTimestamp;
    if (type == null) throw new IllegalArgumentException("Entry type must not be null.");
    this.type = type.getName();
    if (typeVersion <= 0) throw new IllegalArgumentException("Entry typeVersion must be greater than 0.");
    this.typeVersion = typeVersion;
    if (entryData == null) throw new IllegalArgumentException("Entry entryData must not be null.");
    this.entryData = entryData;
    if (metadata == null) throw new IllegalArgumentException("Entry metadata must not be null.");
    this.metadata = metadata;
  }

  @Override
  public String id() {
    return id;
  }

  public LocalDate entryTimestamp() {
    return entryTimestamp;
  }

  @Override
  public String entryData() { return entryData; }

  @Override
  public Metadata metadata() {
    return metadata;
  }

  @Override
  public String typeName() {
    return type;
  }

  @Override
  public int typeVersion() {
    return typeVersion;
  }

  @Override
  public boolean hasMetadata() {
    return !metadata.isEmpty();
  }

  @Override
  public boolean isEmpty() {
    return entryData.isEmpty();
  }

  @Override
  public boolean isNull() {
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <C> Class<C> typed() {
    try {
      return (Class<C>) Class.forName(type);
    } catch (final Exception e) {
      throw new IllegalStateException("Cannot get class for type: " + type);
    }
  }

  @Override
  public int compareTo(final Entry<String> other) {
    final GeodeEventJournalEntry that = (GeodeEventJournalEntry) other;
    return Comparator
      .comparing((GeodeEventJournalEntry e) -> e.id)
      .thenComparing(e -> e.entryTimestamp)
      .thenComparing(e -> e.entryData)
      .thenComparing(e -> e.type)
      .thenComparingInt(e -> e.typeVersion)
      .thenComparing(e -> e.metadata)
      .compare(this, that);
  }

  @Override
  public String toString() {
    return "GeodeEventJournalEntry{" +
      "id='" + id + '\'' +
      ", entryTimestamp=" + entryTimestamp +
      ", entryData='" + entryData + '\'' +
      ", metadata=" + metadata +
      ", type='" + type + '\'' +
      ", typeVersion=" + typeVersion +
      '}';
  }

  @Override
  public Entry<String> withId(final String id) {
    return new GeodeEventJournalEntry(id, LocalDate.now(), typed(), typeVersion, entryData, metadata);
  }

  public void __internal__setId(final String id) {
    this.id = id;
  }

  @Override
  public void toData(PdxWriter out) {
    out
      .writeString("id", id())
      .markIdentityField("id")
      .writeObject("entryTimestamp", entryTimestamp())
      .writeString("entryData", entryData())
      .writeObject("metadata", metadata())
      .writeString("type", type())
      .writeInt("typeVersion", typeVersion());
  }

  @Override
  public void fromData(PdxReader in) {
    this.id = in.readString("id");
    this.entryTimestamp = (LocalDate) in.readObject("entryTimestamp");
    this.entryData = in.readString("entryData");
    this.metadata = (Metadata) in.readObject("metadata");
    this.type = in.readString("type");
    this.typeVersion = in.readInt("typeVersion");
  }
}
