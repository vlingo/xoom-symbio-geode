package io.vlingo.xoom.symbio.store.object.geode;

import org.apache.geode.cache.Region;

import java.util.Map;
/**
 * ConsistencyPolicy represents the strategy used by the <code>persist</code>
 * and <code>persistAll</code> operations of {@link GeodeObjectStoreActor}
 * in storing the entities, Entries and Dispatchables associated with one
 * call tho these operations to their separate regions in a consistent way.
 * &nbsp;
 * The <code>TRANSACTION</code> mode uses a Geode transaction to ensure that
 * all three region updates occur with ACID transactional semantics. In this
 * mode, the objects will immediately be present in their regions at the
 * commit of the transaction and prior to completion of the persistence operation.
 * While this policy seems the obvious choice, it is not yet implemented
 * because Geode transactions have significant constraints and requirements
 * that are complicated to meet in a general way.
 * &nbsp;
 * The <code>EVENTUAL</code> mode uses an alternative strategy that relies
 * on the atomicity of the {@link Region#put(Object, Object)}
 * and {@link Region#putAll(Map)} methods. Under this strategy, the entities,
 * Entries and Dispatchables are stored together in one atomic operation into
 * a unit of work region, then processed by an <code>AsyncEventListener</code>
 * into their respective regions. This means that the persisted objects will
 * not be available from their respective regions for a very short while until
 * the <code>AsyncEventListener</code> runs.
 */
public enum ConsistencyPolicy {

  /* not yet supported
  TRANSACTIONAL,
  */
  EVENTUAL;

  public boolean isEventual() {
    return this == EVENTUAL;
  }

  public boolean isTransactional() {
    return !isEventual();
  }
}
