package io.vlingo.xoom.symbio.store.common.geode.uow;

import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxWriter;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
/**
 * GeodeUnitOfWork represents a single persistence unit of work
 * against the Apache Geode Object Store.
 */
public class GeodeUnitOfWork {

  protected Long id;
  protected LocalDate timestamp;
  protected Map<String, Map<Object,Object>> changedObjectsByPath;

  public GeodeUnitOfWork() {
    super();
    this.timestamp = LocalDate.now();
    this.changedObjectsByPath = new HashMap<>();
  }

  public Long id() {
    return id;
  }

  public void withId(final Long id) {
    this.id = id;
  }

  public LocalDate timestamp() { return timestamp; }

  public <K,V> void register(final K key, final V value, String path) {
    changedObjectsByPath
      .computeIfAbsent(path, s -> new HashMap<>())
      .put(key, value);
  }

  public void applyTo(final GemFireCache cache) throws StorageException {
    changedObjectsByPath.keySet().forEach(path -> {
      regionFor(cache, path).putAll(changedObjectsByPath.get(path));
    });
  }

  protected <K, V> Region<K, V> regionFor(final GemFireCache cache, final String path) throws StorageException {
    Region<K, V> region = cache.getRegion(path);
    if (region == null) {
      throw new StorageException(Result.NoTypeStore, "Region is not configured: " + path);
    }
    return region;
  }

  public void toData(final PdxWriter out) {
    out
      .writeLong("id", id)
      .markIdentityField("id")
      .writeObject("timestamp", timestamp)
      .writeObject("changedObjectsByPath", changedObjectsByPath);
  }

  @SuppressWarnings("unchecked")
  public void fromData(final PdxReader in) {
    this.id = in.readLong("id");
    this.timestamp = (LocalDate) in.readObject("timestamp");
    this.changedObjectsByPath = (Map<String, Map<Object,Object>>) in.readObject("changedObjectsByPath");
  }
}
