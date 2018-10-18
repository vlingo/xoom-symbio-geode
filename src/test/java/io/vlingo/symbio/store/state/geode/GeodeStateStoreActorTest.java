// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.MetadataPdxSerializer;
import io.vlingo.symbio.store.state.MockObjectDispatcher;
import io.vlingo.symbio.store.state.MockObjectResultInterest;
import io.vlingo.symbio.store.state.ObjectStateStore;
import io.vlingo.symbio.store.state.StatePdxSerializerRegistry;
import io.vlingo.symbio.store.state.StateStore.Result;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
/**
 * GemFireStateStoreTest is responsible for testing {@link GeodeStateStoreActor}.
 *
 * @author davem
 * @since Oct 14, 2018
 */
public class GeodeStateStoreActorTest {
  private final static String StoreName = Entity1.class.getSimpleName();
  
  private static ServerLauncher serverLauncher;
  private static Configuration configuration;
  private MockObjectDispatcher dispatcher;
  private MockObjectResultInterest interest;
  private ObjectStateStore store;
  private World world;
  
  @Test
  public void testThatStateStoreWritesText() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(1);

    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 1), interest);

    interest.until.completes();

    assertEquals(0, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectWriteResult.get());
    assertEquals(entity, interest.objectState.get().data);
  }
  
  @Test
  public void testThatStateStoreWritesAndReadsObject() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(3);

    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 1), interest);
    store.read(entity.id, Entity1.class, interest);

    interest.until.completes();

    assertEquals(1, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectReadResult.get());
    assertEquals(entity, interest.objectState.get().data);
    assertEquals(1, interest.objectState.get().typeVersion);
    assertFalse(interest.objectState.get().hasMetadata());

    final Entity1 readEntity = (Entity1) interest.objectState.get().data;

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatStateStoreWritesAndReadsMetadataValue() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(2);

    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 1, Metadata.withValue("value")), interest);
    store.read(entity.id, Entity1.class, interest);

    interest.until.completes();

    assertEquals(1, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectReadResult.get());
    assertEquals(entity, interest.objectState.get().data);
    assertEquals(1, interest.objectState.get().typeVersion);
    assertTrue(interest.objectState.get().hasMetadata());
    assertTrue(interest.objectState.get().metadata.hasValue());
    assertEquals("value", interest.objectState.get().metadata.value);
    assertFalse(interest.objectState.get().metadata.hasOperation());

    final Entity1 readEntity = (Entity1) interest.objectState.get().data;

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatStateStoreWritesAndReadsMetadataOperation() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(2);

    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 1, Metadata.withOperation("op")), interest);
    store.read(entity.id, Entity1.class, interest);

    interest.until.completes();

    assertEquals(1, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectReadResult.get());
    assertEquals(entity, interest.objectState.get().data);
    assertEquals(1, interest.objectState.get().typeVersion);
    assertTrue(interest.objectState.get().hasMetadata());
    assertFalse(interest.objectState.get().metadata.hasValue());
    assertTrue(interest.objectState.get().metadata.hasOperation());
    assertEquals("op", interest.objectState.get().metadata.operation);

    final Entity1 readEntity = (Entity1) interest.objectState.get().data;

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatStateStoreWritesAndReadsMetadata() {
    final Entity1 entity = new Entity1("123", 5);
    interest.until = TestUntil.happenings(3);

    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 1, Metadata.withOperation("op")), interest);
    store.read(entity.id, Entity1.class, interest);

    interest.until.completes();

    assertEquals(1, interest.readObjectResultedIn.get());
    assertEquals(1, interest.writeObjectResultedIn.get());
    assertEquals(Result.Success, interest.objectReadResult.get());
    assertEquals(entity, interest.objectState.get().data);
    assertEquals(1, interest.objectState.get().typeVersion);
    assertTrue(interest.objectState.get().hasMetadata());
    assertFalse(interest.objectState.get().metadata.hasValue());
    assertTrue(interest.objectState.get().metadata.hasOperation());
    assertEquals("op", interest.objectState.get().metadata.operation);

    final Entity1 readEntity = (Entity1) interest.objectState.get().data;

    assertEquals("123", readEntity.id);
    assertEquals(5, readEntity.value);
  }

  @Test
  public void testThatConcurrencyViolationsDetected() {
    final Entity1 entity = new Entity1("123", 5);

    interest.until = TestUntil.happenings(4);
    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 1, Metadata.withOperation("op")), interest);
    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 2, Metadata.withOperation("op")), interest);
    interest.until.completes();

    assertEquals(2, interest.objectWriteAccumulatedResults.size());
    assertEquals(Result.Success, interest.objectWriteAccumulatedResults.poll());
    assertEquals(Result.Success, interest.objectWriteAccumulatedResults.poll());

    interest.until = TestUntil.happenings(4);
    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 1, Metadata.withOperation("op")), interest);
    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 2, Metadata.withOperation("op")), interest);
    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 3, Metadata.withOperation("op")), interest);
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
    store.write(new ObjectState<>(entity1.id, Entity1.class, 1, entity1, 1), interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(new ObjectState<>(entity2.id, Entity1.class, 1, entity2, 1), interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(new ObjectState<>(entity3.id, Entity1.class, 1, entity3, 1), interest);

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
    store.write(new ObjectState<>(entity4.id, Entity1.class, 1, entity4, 1), interest);
    final Entity1 entity5 = new Entity1("567", 5);
    store.write(new ObjectState<>(entity5.id, Entity1.class, 1, entity5, 1), interest);
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
    store.write(new ObjectState<>(entity.id, Entity1.class, 1, entity, 1), interest);
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
    assertTrue(interest.objectState.get().isNull());
  }

  @Test
  public void testThatWriteErrorIsReported() {
    interest.until = TestUntil.happenings(1);
    store.write(null, interest);
    interest.until.completes();
    assertEquals(1, interest.errorCauses.size());
    assertEquals("The state is null.", interest.errorCauses.poll().getMessage());
    assertTrue(interest.objectWriteAccumulatedResults.poll().isError());
    assertTrue(interest.objectState.get().isNull());
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
    store = world.actorFor(Definition.has(GeodeStateStoreActor.class, Definition.parameters(dispatcher, configuration)), ObjectStateStore.class);
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
