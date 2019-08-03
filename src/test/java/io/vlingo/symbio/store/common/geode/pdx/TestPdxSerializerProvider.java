package io.vlingo.symbio.store.common.geode.pdx;

import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.MetadataPdxSerializer;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateSerializer;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatchable;
import io.vlingo.symbio.store.common.geode.dispatch.GeodeDispatchableSerializer;
import io.vlingo.symbio.store.object.geode.GeodeEventJournalEntry;
import io.vlingo.symbio.store.object.geode.GeodeEventJournalEntrySerializer;
import io.vlingo.symbio.store.object.geode.Person;
import io.vlingo.symbio.store.object.geode.PersonSerializer;

public class TestPdxSerializerProvider extends PdxSerializerProviderAdapter {

  public TestPdxSerializerProvider() {
    super();
  }

  @Override
  protected void registerSerializers() {
    registerSerializer(GeodeDispatchable.class, GeodeDispatchableSerializer.class);
    registerSerializer(GeodeEventJournalEntry.class, GeodeEventJournalEntrySerializer.class);
    registerSerializer(Metadata.class, MetadataPdxSerializer.class);
    registerSerializer(Person.class, PersonSerializer.class);
    registerSerializer(State.BinaryState.class, StateSerializer.class);
    registerSerializer(State.ObjectState.class, StateSerializer.class);
    registerSerializer(State.TextState.class, StateSerializer.class);
  }
}
