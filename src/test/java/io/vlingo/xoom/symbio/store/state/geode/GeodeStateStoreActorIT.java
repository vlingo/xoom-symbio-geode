// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.state.geode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.actors.testkit.TestWorld;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.State.ObjectState;
import io.vlingo.xoom.symbio.StateAdapterProvider;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.common.MockObjectDispatcher;
import io.vlingo.xoom.symbio.store.common.event.TestEvent;
import io.vlingo.xoom.symbio.store.common.event.TestEventAdapter;
import io.vlingo.xoom.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.xoom.symbio.store.common.geode.GeodeQueries;
import io.vlingo.xoom.symbio.store.common.geode.dispatch.GeodeDispatchable;
import io.vlingo.xoom.symbio.store.common.geode.functions.ClearRegionFunction;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.state.Entity1;
import io.vlingo.xoom.symbio.store.state.Entity1.Entity1StateAdapter;
import io.vlingo.xoom.symbio.store.state.MockObjectResultInterest;
import io.vlingo.xoom.symbio.store.state.MockObjectResultInterest.StoreData;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.StateStore.TypedStateBundle;
import io.vlingo.xoom.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.xoom.symbio.store.state.geode.GeodeStateStoreActor.GeodeStateStoreInstantiator;
/**
 * GemFireStateStoreTest is responsible for testing {@link GeodeStateStoreActor}.
 */
public class GeodeStateStoreActorIT {
  private static final Logger LOG = LoggerFactory.getLogger(GeodeStateStoreActorIT.class);
  private final static String StoreName = Entity1.class.getSimpleName();

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private MockObjectDispatcher dispatcher;
  private MockObjectResultInterest interest;
  private StateStore store;
  private World world;

  @Test
  public void testThatStateStoreWritesText() {
    final AccessSafely access1 = interest.afterCompleting(1);
    dispatcher.afterCompleting(1);

    final Entity1 entity = new Entity1("123", 5);

    store.write(entity.id, entity, 1, interest);

    assertEquals(0, (int) access1.readFrom("readObjectResultedIn"));
    assertEquals(1, (int) access1.readFrom("writeObjectResultedIn"));
    assertEquals(Result.Success, access1.readFrom("objectWriteResult"));
    assertEquals(entity, access1.readFrom("objectState"));
  }

  @Test
  public void testThatStateStoreWritesAndReadsObject() {

    final AccessSafely access1 = interest.afterCompleting(2);
    dispatcher.afterCompleting(2);

    final Entity1 entity = new Entity1("123", 5);

    store.write(entity.id, entity, 1, interest);
    store.read(entity.id, Entity1.class, interest);

    assertEquals(1, (int) access1.readFrom("readObjectResultedIn"));
    assertEquals(1, (int) access1.readFrom("writeObjectResultedIn"));
    assertEquals(Result.Success, access1.readFrom("objectReadResult"));
    assertEquals(entity, access1.readFrom("objectState"));

    final Entity1 readEntity = (Entity1) access1.readFrom("objectState");

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatStateStoreWritesAndReadsMetadataValue() {
    final AccessSafely access1 = interest.afterCompleting(2);
    dispatcher.afterCompleting(2);

    final Entity1 entity = new Entity1("123", 5);

    store.write(entity.id, entity, 1, Metadata.with("value", "op"), interest);
    store.read(entity.id, Entity1.class, interest);

    assertEquals(1, (int) access1.readFrom("readObjectResultedIn"));
    assertEquals(1, (int) access1.readFrom("writeObjectResultedIn"));
    assertEquals(Result.Success, access1.readFrom("objectReadResult"));
    assertEquals(entity, access1.readFrom("objectState"));
    assertNotNull(access1.readFrom("metadataHolder"));
    final Metadata metadata = access1.readFrom("metadataHolder");
    assertTrue(metadata.hasValue());
    assertEquals("value", metadata.value);

    final Entity1 readEntity = (Entity1) access1.readFrom("objectState");

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatStateStoreWritesAndReadsMetadataOperation() {
    final AccessSafely access1 = interest.afterCompleting(2);
    dispatcher.afterCompleting(2);

    final Entity1 entity = new Entity1("123", 5);

    store.write(entity.id, entity, 1, Metadata.with("value", "op"), interest);
    store.read(entity.id, Entity1.class, interest);

    assertEquals(1, (int) access1.readFrom("readObjectResultedIn"));
    assertEquals(1, (int) access1.readFrom("writeObjectResultedIn"));
    assertEquals(Result.Success, access1.readFrom("objectReadResult"));
    assertEquals(entity, access1.readFrom("objectState"));
    final Metadata metadata = access1.readFrom("metadataHolder");
    assertNotNull(metadata);
    assertTrue(metadata.hasOperation());
    assertEquals("op", metadata.operation);

    final Entity1 readEntity = (Entity1) access1.readFrom("objectState");

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatConcurrencyViolationsDetected() {
    final AccessSafely access1 = interest.afterCompleting(2);
    dispatcher.afterCompleting(2);

    final Entity1 entity = new Entity1("123", 5);

    store.write(entity.id, entity, 1, interest);
    store.write(entity.id, entity, 2, interest);

    assertEquals(2, (int) access1.readFrom("objectWriteAccumulatedResultsCount"));
    assertEquals(Result.Success, access1.readFrom("objectWriteAccumulatedResults"));
    assertEquals(Result.Success, access1.readFrom("objectWriteAccumulatedResults"));
    assertEquals(0, (int) access1.readFrom("objectWriteAccumulatedResultsCount"));

    final AccessSafely access2 = interest.afterCompleting(3);
    dispatcher.afterCompleting(3);

    store.write(entity.id, entity, 1, interest);
    store.write(entity.id, entity, 2, interest);
    store.write(entity.id, entity, 3, interest);

    assertEquals(3, (int) access2.readFrom("objectWriteAccumulatedResultsCount"));
    assertEquals(Result.ConcurrencyViolation, access2.readFrom("objectWriteAccumulatedResults"));
    assertEquals(Result.ConcurrencyViolation, access2.readFrom("objectWriteAccumulatedResults"));
    assertEquals(Result.Success, access2.readFrom("objectWriteAccumulatedResults"));
  }

  @Test
  public void testThatStateStoreDispatches() {
    interest.afterCompleting(3);
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(3);

    final Entity1 entity1 = new Entity1("123", 1);
    store.write(entity1.id, entity1, 1, interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(entity2.id, entity2, 1, interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(entity3.id, entity3, 1, interest);

    assertEquals(3, (int) accessDispatcher.readFrom("dispatchedStateCount"));
    final State<?> state123 = accessDispatcher.readFrom("dispatchedState", dispatchId("123"));
    assertEquals("123", state123.id);
    final State<?> state234 = accessDispatcher.readFrom("dispatchedState", dispatchId("234"));
    assertEquals("234", state234.id);
    final State<?> state345 = accessDispatcher.readFrom("dispatchedState", dispatchId("345"));
    assertEquals("345", state345.id);

    interest.afterCompleting(4);
    final AccessSafely accessDispatcher1 = dispatcher.afterCompleting(4);

    accessDispatcher1.writeUsing("processDispatch", false);
    final Entity1 entity4 = new Entity1("456", 4);
    store.write(entity4.id, entity4, 1, interest);
    final Entity1 entity5 = new Entity1("567", 5);
    store.write(entity5.id, entity5, 1, interest);

    accessDispatcher1.writeUsing("processDispatch", true);
    dispatcher.dispatchUnconfirmed();
    accessDispatcher1.readFrom("dispatchedStateCount");

    assertEquals(5, (int) accessDispatcher1.readFrom("dispatchedStateCount"));

    final State<?> state456 = accessDispatcher1.readFrom("dispatchedState", dispatchId("456"));
    assertEquals("456", state456.id);
    final State<?> state567 = accessDispatcher1.readFrom("dispatchedState", dispatchId("567"));
    assertEquals("567", state567.id);
  }

  @Test
  public void testThatReadAllReadsAll() {
    dispatcher.afterCompleting(0); // prevent write failure

    final AccessSafely accessWrites = interest.afterCompleting(3);

    final Entity1 entity1 = new Entity1("123", 1);
    store.write(entity1.id, entity1, 1, interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(entity2.id, entity2, 1, interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(entity3.id, entity3, 1, interest);

    final int totalWrites = accessWrites.readFrom("totalWrites");

    assertEquals(3, totalWrites);

    final AccessSafely accessReads = interest.afterCompleting(3);

    final List<TypedStateBundle> bundles =
            Arrays.asList(
                    new TypedStateBundle(entity1.id, Entity1.class),
                    new TypedStateBundle(entity2.id, Entity1.class),
                    new TypedStateBundle(entity3.id, Entity1.class));

    store.readAll(bundles, interest, null);

    final List<StoreData> allStates = accessReads.readFrom("readAllStates");

    assertEquals(3, allStates.size());
    final Entity1 state123 = allStates.get(0).typedState();
    assertEquals("123", state123.id);
    assertEquals(1, state123.value);
    final Entity1 state234 = allStates.get(1).typedState();
    assertEquals("234", state234.id);
    assertEquals(2, state234.value);
    final Entity1 state345 = allStates.get(2).typedState();
    assertEquals("345", state345.id);
    assertEquals(3, state345.value);
  }

  @Test
  public void testThatReadErrorIsReported() {
    final AccessSafely access1 = interest.afterCompleting(2);
    dispatcher.afterCompleting(2);

    final Entity1 entity = new Entity1("123", 1);
    store.write(entity.id, entity, 1, interest);
    store.read(null, Entity1.class, interest);

    assertEquals(1, (int) access1.readFrom("errorCausesCount"));
    final Exception cause1 = access1.readFrom("errorCauses");
    assertEquals("The id is null.", cause1.getMessage());
    Result result1 = access1.readFrom("objectReadResult");
    assertTrue(result1.isError());

    interest = new MockObjectResultInterest();
    final AccessSafely access2 = interest.afterCompleting(1);
    dispatcher.afterCompleting(1);

    store.read(entity.id, null, interest);

    final Exception cause2 = access2.readFrom("errorCauses");
    assertEquals("The type is null.", cause2.getMessage());
    Result result2 = access2.readFrom("objectReadResult");
    assertTrue(result2.isError());
    final Object objectState = access2.readFrom("objectState");
    assertNull(objectState);
  }

  @Test
  public void testThatWriteErrorIsReported() {
    final AccessSafely access1 = interest.afterCompleting(1);
    dispatcher.afterCompleting(1);

    store.write(null, null, 0, interest);

    assertEquals(1, (int) access1.readFrom("errorCausesCount"));
    final Exception cause1 = access1.readFrom("errorCauses");
    assertEquals("The state is null.", cause1.getMessage());
    final Result result1 = access1.readFrom("objectWriteAccumulatedResults");
    assertTrue(result1.isError());
    final Object objectState = access1.readFrom("objectState");
    assertNull(objectState);
  }

  @Test
  public void testRedispatch() {
    final AccessSafely access = interest.afterCompleting(3);
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(5);

    accessDispatcher.writeUsing("processDispatch", false);

    final Entity1 entity1 = new Entity1("123", 1);
    store.write(entity1.id, entity1, 1, interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(entity2.id, entity2, 1, interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(entity3.id, entity3, 1, interest);

    final int readsTimes = access.readFrom("writeObjectResultedIn");
    assertEquals(3, readsTimes);

    accessDispatcher.writeUsing("processDispatch", true);

    int dispatchedStateCount = accessDispatcher.readFrom("dispatchedStateCount");
    assertTrue("dispatchedStateCount", dispatchedStateCount == 3);

    int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
    assertTrue("dispatchAttemptCount", dispatchAttemptCount > 3);
  }

  @BeforeClass
  @SuppressWarnings("unused")
  public static void beforeAnyTest() {
    Properties serverProps = new Properties();
    serverProps.put(ConfigurationProperties.CACHE_XML_FILE, "server-cache.xml");
    serverProps.put(ConfigurationProperties.LOG_LEVEL, "error");

    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server1 = cluster.startServerVM(1, serverProps, locator.getPort());
    MemberVM server2 = cluster.startServerVM(2, serverProps, locator.getPort());

    System.setProperty("LOCATOR_IP", ipAddress());
    System.setProperty("LOCATOR_PORT", String.valueOf(locator.getPort()));
    System.setProperty("gemfire." + ConfigurationProperties.CACHE_XML_FILE, "client-cache.xml");
    System.setProperty("gemfire." + ConfigurationProperties.LOG_LEVEL, "error");
  }


  @Before
  public void beforeEachTest() {
    TestWorld testWorld = TestWorld.startWithDefaults("test-store");
    world = testWorld.world();

    interest = new MockObjectResultInterest();
    dispatcher = new MockObjectDispatcher(interest);

    final String originatorId = "TEST";
    final long checkConfirmationExpirationInterval = 1000L;
    final long confirmationExpiration = 1000L;

    StateAdapterProvider stateAdapterProvider = new StateAdapterProvider(world);
    stateAdapterProvider.registerAdapter(Entity1.class, new Entity1StateAdapter());
    EntryAdapterProvider entryAdapterProvider = EntryAdapterProvider.instance(world);
    entryAdapterProvider.registerAdapter(TestEvent.class, new TestEventAdapter());
    new EntryAdapterProvider(world); //entryAdapterProvider =

    store = world.actorFor(
      StateStore.class,
      Definition.has(
        GeodeStateStoreActor.class,
        new GeodeStateStoreInstantiator(originatorId, Arrays.asList(dispatcher(dispatcher)), checkConfirmationExpirationInterval, confirmationExpiration)));

    StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, StoreName);
  }

  @After
  public void afterEachTest() {
    destroyWorld();
    clearCache();
    GemFireCacheProvider.forClient().close();
  }

  private void destroyWorld() {
    world.terminate();
    world = null;
    store = null;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Dispatcher<GeodeDispatchable<ObjectState<Object>>> dispatcher(MockObjectDispatcher dispatcher) {
    return (Dispatcher) dispatcher;
  }

  @SuppressWarnings("rawtypes")
  private void clearCache() {
    Optional<GemFireCache> cacheOrNull = GemFireCacheProvider.getAnyInstance();
    if (cacheOrNull.isPresent()) {
      GemFireCache cache = cacheOrNull.get();
      Region<String, ObjectState> storeRegion = cache.getRegion(StoreName);
      if (storeRegion != null) {
        FunctionService
          .onRegion(storeRegion)
          .execute(ClearRegionFunction.class.getSimpleName());
      }
      Region<String, ObjectState> dispatchablesRegion = cache.getRegion(GeodeQueries.DISPATCHABLES_REGION_PATH);
      if (dispatchablesRegion != null) {
        FunctionService
            .onRegion(dispatchablesRegion)
            .execute(ClearRegionFunction.class.getSimpleName());
      }
    }
  }

  private String dispatchId(final String entityId) {
    return StoreName + ":" + entityId;
  }

  private static String ipAddress() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    }
    catch (Throwable t) {
      LOG.error("error looking up host IP address; defaulting to loopback", t);
      return InetAddress.getLoopbackAddress().getHostAddress();
    }
  }
}
