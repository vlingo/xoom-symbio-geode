package io.vlingo.symbio.store.object.geode;

import io.vlingo.symbio.store.object.geode.Person;
import org.apache.geode.cache.Declarable;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;

public class PersonSerializer implements PdxSerializer, Declarable {

  @Override
  public boolean toData(Object o, PdxWriter out) {
    boolean result = false;
    if (o instanceof Person) {
      final Person p = (Person) o;
      out
        .writeLong("persistenceId", p.persistenceId())
        .markIdentityField("persistenceId")
        .writeLong("version", p.version())
        .writeInt("age", p.age)
        .writeString("name", p.name);
      result = true;
    }
    return result;
  }

  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    long persistenceId = in.readLong("persistenceId");
    long version = in.readLong("version");
    int age = in.readInt("age");
    String name = in.readString("name");
    return new Person(name, age, persistenceId, version);
  }
}
