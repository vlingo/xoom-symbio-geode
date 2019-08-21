package io.vlingo.symbio.store.common.geode.uow;

import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class GeodeUnitOfWork {

  protected Long id;
  protected LocalDate timestamp;
  protected Map<String, Entry<?>> entriesById;
  protected Map<String, Dispatchable<Entry<?>, State<?>>> dispatchablesById;
  /**
   * GeodeUnitOfWork represents a single persistence unit of work
   * against the Apache Geode Object Store, and is designed to allow atomic
   * storage of three (sets of) objects (entities, Entries and Dispatchables)
   * in a consistent way without the use of Geode transactions using the
   * <code>Region.put</code> and <code>Region.putAll</code> operations.
   * The objects contained in the unit of work are subsequently
   * distributed to their respective regions by an AsyncEventListener.
   */
  public GeodeUnitOfWork() {
    super();
    this.timestamp = LocalDate.now();
    this.entriesById = new HashMap<>();
    this.dispatchablesById = new HashMap<>();
  }

  public Long id() {
    return id;
  }

  public void withId(final Long id) {
    this.id = id;
  }

  public LocalDate timestamp() { return timestamp; }

  public Map<String, Entry<?>> entries() {
    return entriesById;
  }

  public void persistEntry(final Entry<?> entryToPersist) {
    entriesById.put(entryToPersist.id(), entryToPersist);
  }

  public void persistEntries(final Collection<Entry<?>> entriesToPersist) {
    final Map<String, Entry<?>> all =
      entriesToPersist.stream().collect(Collectors.toMap(Entry::id, entry -> entry));
    entriesById.putAll(all);
  }

  public Map<String, Dispatchable<Entry<?>, State<?>>> dispatchables() {
    return dispatchablesById;
  }

  public void persistDispatchable(final Dispatchable<Entry<?>, State<?>> dispatchableToPersist) {
    dispatchablesById.put(dispatchableToPersist.id(), dispatchableToPersist);
  }

  public void applyTo(final GemFireCache cache) throws StorageException {
    applyEntriesTo(cache);
    applyDispatchablesTo(cache);
  }

  private void applyEntriesTo(final GemFireCache cache) throws StorageException {
    regionFor(cache, GeodeQueries.OBJECTSTORE_EVENT_JOURNAL_REGION_PATH).putAll(entriesById);
  }

  private void applyDispatchablesTo(final GemFireCache cache) throws StorageException {
    regionFor(cache, GeodeQueries.DISPATCHABLES_REGION_PATH).putAll(dispatchablesById);
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
      .writeObject("entriesById", entriesById)
      .writeObject("dispatchablesById", dispatchablesById);
  }

  @SuppressWarnings("unchecked")
  public void fromData(final PdxReader in) {
    this.id = in.readLong("id");
    this.timestamp = (LocalDate) in.readObject("timestamp");
    this.entriesById = (Map<String, Entry<?>>) in.readObject("entriesById");
    this.dispatchablesById = (Map<String, Dispatchable<Entry<?>, State<?>>>) in.readObject("dispatchablesById");
  }
}
