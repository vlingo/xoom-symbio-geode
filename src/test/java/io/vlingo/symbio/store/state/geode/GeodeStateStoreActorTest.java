// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.ServerLauncher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.vlingo.actors.World;
import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.actors.testkit.TestWorld;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.ObjectState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.Entity1.Entity1StateAdapter;
import io.vlingo.symbio.store.state.MetadataPdxSerializer;
import io.vlingo.symbio.store.state.MockObjectDispatcher;
import io.vlingo.symbio.store.state.MockObjectResultInterest;
import io.vlingo.symbio.store.state.StatePdxSerializerRegistry;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
/**
 * GemFireStateStoreTest is responsible for testing {@link GeodeStateStoreActor}.
 */
public class GeodeStateStoreActorTest {
  private final static String StoreName = Entity1.class.getSimpleName();
  
  private static ServerLauncher serverLauncher;
  private static Configuration configuration;
  
  private MockObjectDispatcher dispatcher;
  private MockObjectResultInterest interest;
  private StateStore store;
  private TestWorld testWorld;
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

    store.write(entity.id, entity, 1, interest);
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

    store.write(entity.id, entity, 1, interest);
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
    assertEquals(Result.ConcurrentyViolation, access2.readFrom("objectWriteAccumulatedResults"));
    assertEquals(Result.ConcurrentyViolation, access2.readFrom("objectWriteAccumulatedResults"));
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
    interest.afterCompleting(1);
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(3);

    final Entity1 entity = new Entity1("123", 5);

    accessDispatcher.writeUsing("processDispatch", false);
    store.write(entity.id, entity, 1, interest);

    try {
      Thread.sleep(2000);
    }
    catch (InterruptedException ex) {
      //ignored
    }
    
    accessDispatcher.writeUsing("processDispatch", true);

    int dispatchCount = accessDispatcher.readFrom("dispatchedStateCount");
    System.out.println("InMemoryStateStoreTest::testRedispatch - dispatchCount=" + dispatchCount);
    assertTrue("dispatchCount", dispatchCount == 1);
    
    int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
    System.out.println("InMemoryStateStoreTest::testRedispatch - dispatchAttemptCount=" + dispatchAttemptCount);
    assertTrue("dispatchCount", dispatchAttemptCount > 1);
}
  
  @BeforeClass
  public static void beforeAllTests() {
    startGeode();
  }
  
  protected static void startGeode() {
    System.out.println("startGeode - entered");
    try {
      StatePdxSerializerRegistry.serializeTypeWith(Metadata.class, MetadataPdxSerializer.class);
      serverLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(System.getProperty("user.dir") + "/target")
        .build();
      serverLauncher.start();
      System.out.println("started cache server '" + serverLauncher.getMemberName() + "'");
    } finally {
      System.out.println("startGeode - exited");
    }
  }
  
  @Before
  public void beforeEachTest() {
    testWorld = TestWorld.startWithDefaults("test-store");
    world = testWorld.world();
    
    interest = new MockObjectResultInterest();
    dispatcher = new MockObjectDispatcher(interest);
    
    configuration = Configuration.forPeer();
    
    store = world.actorFor(StateStore.class, GeodeStateStoreActor.class, dispatcher, configuration);
    store.registerAdapter(Entity1.class, new Entity1StateAdapter());
    
    StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, StoreName);
  }
  
  @AfterClass
  public static void afterAllTests() {
    stopGeode();
  }
  
  public static void stopGeode() {
    System.out.println("stopGeode - entered");
    try {
      if (serverLauncher != null) {
        serverLauncher.stop();
        serverLauncher = null;
      }
    } finally {
      System.out.println("stopGeode - exited");
    }
  }
  
  @After
  public void afterEachTest() {
    destroyWorld();
    clearCache();
  }
  
  protected void destroyWorld() {
    world.terminate();
    world = null;
    store = null;
  }
  
  @SuppressWarnings("rawtypes")
  private void clearCache() {
    GemFireCache cache = GemFireCacheProvider.getAnyInstance(configuration);
    Region<String, ObjectState> region = cache.getRegion(StoreName);
    if (region != null) {
      for (String key : region.keySet()) {
        region.remove(key);
      }
    }
  }
  
  private String dispatchId(final String entityId) {
    return StoreName + ":" + entityId;
  }
}
