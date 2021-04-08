package io.vlingo.xoom.symbio;

import io.vlingo.xoom.symbio.store.StoredTypes;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;

public class StateSerializer implements PdxSerializer {

  @Override
  @SuppressWarnings("rawtypes")
  public boolean toData(Object o, PdxWriter out) {
    boolean result = false;
    if (o instanceof State) {
      final State state = (State) o;
      out
        .writeString("id", state.id)
        .markIdentityField("id")
        .writeInt("dataVersion", state.dataVersion)
        .writeString("type", state.type)
        .writeInt("typeVersion", state.typeVersion)
        .writeObject("metadata", state.metadata);
      if (state.isBinary()) {
        final State.BinaryState binaryState = (State.BinaryState) state;
        out
          .writeString("kind", "binary")
          .writeByteArray("data", binaryState.data);
      }
      else if (state.isObject()) {
        final State.ObjectState objectState = (State.ObjectState) state;
        out
          .writeString("kind", "object")
          .writeObject("data", objectState.data);
      }
      else {
        final State.TextState textState = (State.TextState) state;
        out
          .writeString("kind", "text")
          .writeString("data", textState.data);
      }
      result = true;
    }
    return result;
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Object fromData(Class<?> clazz, PdxReader in) {
    Object result;
    final String id = in.readString("id");
    final int dataVersion = in.readInt("dataVersion");
    final Class<?> type = computeType(in.readString("type"));
    final int typeVersion = in.readInt("typeVersion");
    final Metadata metadata = (Metadata) in.readObject("metadata");
    final String kind = in.readString("kind");
    if ("binary".equals(kind)) {
      final byte[] data = in.readByteArray("data");
      result = new State.BinaryState(id, type, typeVersion, data, dataVersion, metadata);
    }
    else if ("text".equals(kind)) {
      final String data = in.readString("data");
      result = new State.TextState(id, type, typeVersion, data, dataVersion, metadata);
    }
    else {
      final Object data = in.readObject("data");
      result = new State.ObjectState(id, type, typeVersion, data, dataVersion, metadata);
    }
    return result;
  }

  private Class<?> computeType(final String typeFQCN) {
    try {
      return StoredTypes.forName(typeFQCN);
    }
    catch (Throwable t) {
      throw new PdxSerializationException("error loading class " + typeFQCN, t);
    }
  }
}
