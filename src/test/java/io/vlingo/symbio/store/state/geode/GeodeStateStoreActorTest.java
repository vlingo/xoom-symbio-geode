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

import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.symbio.Metadata;
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
  private World world;
  
  @Test
  public void testThatStateStoreWritesText() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(1);

    store.write(entity.id, entity, 1, interest);

    interest.until.completes();

    assertEquals(0, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectWriteResult.get());
    assertEquals(entity, interest.objectState.get());
  }
  
  @Test
  public void testThatStateStoreWritesAndReadsObject() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(3);

    store.write(entity.id, entity, 1, interest);
    store.read(entity.id, Entity1.class, interest);

    interest.until.completes();

    assertEquals(1, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectReadResult.get());
    assertEquals(entity, interest.objectState.get());
    assertNotNull(interest.metadataHolder.get());

    final Entity1 readEntity = (Entity1) interest.objectState.get();

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatStateStoreWritesAndReadsMetadataValue() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(2);

    store.write(entity.id, entity, 1, interest);
    store.read(entity.id, Entity1.class, interest);

    interest.until.completes();

    assertEquals(1, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectReadResult.get());
    assertEquals(entity, interest.objectState.get());
    assertNotNull(interest.metadataHolder.get());
    assertTrue(interest.metadataHolder.get().hasValue());
    assertEquals("value", interest.metadataHolder.get().value);

    final Entity1 readEntity = (Entity1) interest.objectState.get();

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatStateStoreWritesAndReadsMetadataOperation() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(2);

    store.write(entity.id, entity, 1, interest);
    store.read(entity.id, Entity1.class, interest);

    interest.until.completes();

    assertEquals(1, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectReadResult.get());
    assertEquals(entity, interest.objectState.get());
    assertNotNull(interest.metadataHolder.get());
    assertTrue(interest.metadataHolder.get().hasOperation());
    assertEquals("op", interest.metadataHolder.get().operation);

    final Entity1 readEntity = (Entity1) interest.objectState.get();

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatStateStoreWritesAndReadsMetadata() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(3);

    store.write(entity.id, entity, 1, interest);
    store.read(entity.id, Entity1.class, interest);

    interest.until.completes();

    assertEquals(1, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectReadResult.get());
    assertEquals(entity, interest.objectState.get());
    assertNotNull(interest.metadataHolder.get());
    assertTrue(interest.metadataHolder.get().hasValue());
    assertEquals("value", interest.metadataHolder.get().value);
    assertTrue(interest.metadataHolder.get().hasOperation());
    assertEquals("op", interest.metadataHolder.get().operation);

    final Entity1 readEntity = (Entity1) interest.objectState.get();

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatConcurrencyViolationsDetected() {
    final Entity1 entity = new Entity1("123", 5);

    interest.until = TestUntil.happenings(4);
    store.write(entity.id, entity, 1, interest);
    store.write(entity.id, entity, 2, interest);
    interest.until.completes();

    assertEquals(2, interest.objectWriteAccumulatedResults.size());
    assertEquals(Result.Success, interest.objectWriteAccumulatedResults.poll());
    assertEquals(Result.Success, interest.objectWriteAccumulatedResults.poll());

    interest.until = TestUntil.happenings(4);
    store.write(entity.id, entity, 1, interest);
    store.write(entity.id, entity, 2, interest);
    store.write(entity.id, entity, 3, interest);
    interest.until.completes();

    assertEquals(3, interest.objectWriteAccumulatedResults.size());
    assertEquals(Result.ConcurrentyViolation, interest.objectWriteAccumulatedResults.poll());
    assertEquals(Result.ConcurrentyViolation, interest.objectWriteAccumulatedResults.poll());
    assertEquals(Result.Success, interest.objectWriteAccumulatedResults.poll());
  }

  @Test
  public void testThatStateStoreDispatches() {
    interest.until = TestUntil.happenings(3);

    dispatcher.until = TestUntil.happenings(3);

    final Entity1 entity1 = new Entity1("123", 1);
    store.write(entity1.id, entity1, 1, interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(entity2.id, entity2, 1, interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(entity3.id, entity3, 1, interest);

    interest.until.completes();
    dispatcher.until.completes();

    assertEquals(3, dispatcher.dispatched.size());
    assertEquals("123", dispatcher.dispatched.get(dispatchId("123")).id);
    assertEquals("234", dispatcher.dispatched.get(dispatchId("234")).id);
    assertEquals("345", dispatcher.dispatched.get(dispatchId("345")).id);

    interest.until = TestUntil.happenings(5);
    dispatcher.until = TestUntil.happenings(2);

    dispatcher.processDispatch.set(false);
    final Entity1 entity4 = new Entity1("456", 4);
    store.write(entity4.id, entity4, 1, interest);
    final Entity1 entity5 = new Entity1("567", 5);
    store.write(entity5.id, entity5, 1, interest);
    dispatcher.processDispatch.set(true);
    dispatcher.control.dispatchUnconfirmed();

    dispatcher.until.completes();
    interest.until.completes();

    assertEquals(5, dispatcher.dispatched.size());
    assertEquals("456", dispatcher.dispatched.get(dispatchId("456")).id);
    assertEquals("567", dispatcher.dispatched.get(dispatchId("567")).id);
  }

  @Test
  public void testThatReadErrorIsReported() {
    interest.until = TestUntil.happenings(3);
    final Entity1 entity = new Entity1("123", 1);
    store.write(entity.id, entity, 1, interest);
    store.read(null, Entity1.class, interest);
    interest.until.completes();
    assertEquals(1, interest.errorCauses.size());
    assertEquals("The id is null.", interest.errorCauses.poll().getMessage());
    assertTrue(interest.objectReadResult.get().isError());
    
    interest.until = TestUntil.happenings(1);
    store.read(entity.id, null, interest);
    interest.until.completes();
    assertEquals("The type is null.", interest.errorCauses.poll().getMessage());
    assertTrue(interest.objectReadResult.get().isError());
    assertNull(interest.objectState.get());
  }

  @Test
  public void testThatWriteErrorIsReported() {
    interest.until = TestUntil.happenings(1);
    store.write(null, null, 0, interest);
    interest.until.completes();
    assertEquals(1, interest.errorCauses.size());
    assertEquals("The state is null.", interest.errorCauses.poll().getMessage());
    assertTrue(interest.objectWriteAccumulatedResults.poll().isError());
    assertNull(interest.objectState.get());
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
    createWorld();
  }
  
  protected void createWorld() {
    world = World.startWithDefaults("test-store");
    interest = new MockObjectResultInterest(0);
    dispatcher = new MockObjectDispatcher(0, interest);
    configuration = Configuration.forPeer();
    store = world.actorFor(Definition.has(GeodeStateStoreActor.class, Definition.parameters(dispatcher, configuration)), StateStore.class);
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
