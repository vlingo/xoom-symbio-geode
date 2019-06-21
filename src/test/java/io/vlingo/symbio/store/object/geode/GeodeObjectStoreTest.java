// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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
import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.object.ListQueryExpression;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.state.geode.GeodeQueries;
/**
 * GeodeObjectStoreTest
 */
public class GeodeObjectStoreTest {
  
  private static ServerLauncher serverLauncher;
  private World world;
  private List<GeodePersistentObjectMapping> registeredMappings;
  private ObjectStore objectStore;

  @Test
  public void testThatObjectStoreInsertsOneAndQueries() {
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);
    
    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping("Person");
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);
    
    final long greenLanternId = 300L;
    final Person greenLantern = new Person("Green Lantern", 30, greenLanternId);
    assertEquals(0L, greenLantern.version());
    objectStore.persist(greenLantern, persistInterest);
    
    final Person storedgreenLantern = persistAccess.readFrom("persistedObject");
    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    assertEquals(greenLantern, storedgreenLantern);
    assertEquals(1, (int) persistAccess.readFrom("expectedPersistCount"));
    assertEquals(1, (int) persistAccess.readFrom("actualPersistCount"));
    assertEquals(1L, storedgreenLantern.version());

    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);
    
    objectStore.queryObject(
      ListQueryExpression.using(
        Person.class,
        "select * from /Person p where p.id = $1",
        Arrays.asList(greenLanternId)),
      queryInterest
    );
    
    final QuerySingleResult querySingleResult = queryAccess.readFrom("singleResult");
    assertNotNull(querySingleResult);
    assertEquals(greenLantern, querySingleResult.persistentObject);
  }
  
  @Test
  public void testThatObjectStoreInsertsMultipleAndQueries() {
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);
    
    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping("Person");
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);
    
    final Person greenLantern = new Person("Green Lantern", 30, 300L);
    final Person theWasp = new Person("The Wasp", 40, 400L);
    final Person ironMan = new Person("Iron Man", 50, 500L);
    objectStore.persistAll(Arrays.asList(greenLantern, theWasp, ironMan), persistInterest);
    
    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    assertEquals(3, (int) persistAccess.readFrom("expectedPersistCount"));
    assertEquals(3, (int) persistAccess.readFrom("actualPersistCount"));

    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);
    
    objectStore.queryAll(
      QueryExpression.using(
        Person.class,
        "select * from /Person p order by p.id"),
      queryInterest
    );
    
    final QueryMultiResults queryMultiResults = queryAccess.readFrom("multiResults");
    assertNotNull(queryMultiResults);
    assertEquals("persistentObject.size", 3, queryMultiResults.persistentObjects.size());
    final Iterator<?> i = queryMultiResults.persistentObjects.iterator();
    assertEquals("greenLantern", greenLantern, i.next());
    assertEquals("theWasp", theWasp, i.next());
    assertEquals("ironMan", ironMan, i.next());
  }
  
  @Test
  public void testThatSingleEntityUpdates() {
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);
    
    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping("Person");
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);
    
    final long greenLanternId = 300L;
    final Person greenLantern = new Person("Green Lantern", 30, greenLanternId);
    assertEquals(0L, greenLantern.version());
    objectStore.persist(greenLantern, persistInterest);
    
    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    
    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);
    
    objectStore.queryObject(
      ListQueryExpression.using(
        Person.class,
        "select * from /Person p where p.id = $1",
        Arrays.asList(greenLanternId)),
      queryInterest
    );
    
    final QuerySingleResult querySingleResult = queryAccess.readFrom("singleResult");
    assertNotNull(querySingleResult);
    final Person queriedPerson = (Person) querySingleResult.persistentObject;
    assertEquals(greenLantern, queriedPerson);
    
    final long queriedPersonVersion = queriedPerson.version();
    
    final MockPersistResultInterest updateInterest = new MockPersistResultInterest();
    final AccessSafely updateAccess = updateInterest.afterCompleting(1);
    
    queriedPerson.withAge(31);
    objectStore.persist(queriedPerson, queryInterest.singleResult.get().updateId, updateInterest);
    assertEquals(Result.Success, updateAccess.readFrom("persistResult"));
    
    final MockQueryResultInterest requeryInterest = new MockQueryResultInterest();
    final AccessSafely requeryAccess = requeryInterest.afterCompleting(1);
    
    objectStore.queryObject(
      ListQueryExpression.using(
        Person.class,
        "select * from /Person p where p.id = $1",
        Arrays.asList(greenLanternId)),
      requeryInterest
    );
    
    final QuerySingleResult requerySingleResult = requeryAccess.readFrom("singleResult");
    assertNotNull(requerySingleResult);
    final Person requeriedPerson = (Person) requerySingleResult.persistentObject;
    assertEquals(greenLantern, requeriedPerson);
    assertEquals(queriedPersonVersion + 1, requeriedPerson.version());
  }
  
  @Test
  public void testThatMultipleEntitiesUpdate() {
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);
    
    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping("Person");
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);
    
    /* insert */
    
    final Person greenLantern1 = new Person("Green Lantern", 30, 300L);
    final Person theWasp1 = new Person("The Wasp", 40, 400L);
    final Person ironMan1 = new Person("Iron Man", 50, 500L);
    objectStore.persistAll(Arrays.asList(greenLantern1, theWasp1, ironMan1), persistInterest);
    
    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    assertEquals(3, (int) persistAccess.readFrom("expectedPersistCount"));
    assertEquals(3, (int) persistAccess.readFrom("actualPersistCount"));
    
    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);
    
    objectStore.queryAll(
      QueryExpression.using(
        Person.class,
        "select * from /Person p order by p.id"),
      queryInterest
    );
    
    final QueryMultiResults queryMultiResults = queryAccess.readFrom("multiResults");
    assertNotNull(queryMultiResults);
    assertEquals("persistentObject.size", 3, queryMultiResults.persistentObjects.size());
    final Iterator<?> i = queryMultiResults.persistentObjects.iterator();
    final Person queriedGreenLantern1 = (Person) i.next();
    assertEquals("greenLantern", greenLantern1, queriedGreenLantern1);
    final Person queriedTheWasp1 = (Person) i.next();
    assertEquals("theWasp", theWasp1, queriedTheWasp1);
    final Person queriedIronMan1 = (Person) i.next();
    assertEquals("ironMan", ironMan1, queriedIronMan1);
    
    /* update */
    
    final long queriedGreenLantern1Version = queriedGreenLantern1.version();
    final long queriedTheWasp1Version = queriedTheWasp1.version();
    final long queriedIronMan1Version = queriedIronMan1.version();
    
    final MockPersistResultInterest updateInterest = new MockPersistResultInterest();
    final AccessSafely updateAccess = updateInterest.afterCompleting(1);
    
    Random random = new Random(System.currentTimeMillis());
    final String greenLantern2Name = "Green Lantern " + random.nextInt(2000);
    final String theWasp2Name = "The Wasp " + random.nextInt(2000);
    final String ironMan2Name = "Iron Man " + random.nextInt(2000);
    
    final Person greenLantern2 = queriedGreenLantern1.withName(greenLantern2Name);
    final Person theWasp2 = queriedTheWasp1.withName(theWasp2Name);
    final Person ironMan2 = queriedIronMan1.withName(ironMan2Name);
    
    Collection<Person> updatedPeople = Arrays.asList(greenLantern2, theWasp2, ironMan2);
    
    objectStore.persistAll(updatedPeople, updateInterest);
    
    Exception persistCause = updateAccess.readFrom("persistCause");
    assertNull(persistCause);
    assertEquals(Result.Success, updateAccess.readFrom("persistResult"));
    
    final MockQueryResultInterest requeryInterest = new MockQueryResultInterest();
    final AccessSafely requeryAccess = requeryInterest.afterCompleting(1);
    
    objectStore.queryAll(
      QueryExpression.using(
        Person.class,
        "select * from /Person p order by p.id"),
      requeryInterest
    );
    
    final QueryMultiResults requeryMultiResults = requeryAccess.readFrom("multiResults");
    assertNotNull(requeryMultiResults);
    assertEquals("persistentObject.size", 3, requeryMultiResults.persistentObjects.size());
    final Iterator<?> i3 = requeryMultiResults.persistentObjects.iterator();
    final Person queriedGreenLantern2 = (Person) i3.next();
    assertEquals("queriedGreenLantern2.version", queriedGreenLantern1Version + 1, queriedGreenLantern2.version());
    assertEquals("queriedGreenLantern2.name", queriedGreenLantern2.name, greenLantern2Name);
    final Person queriedTheWasp2 = (Person) i3.next();
    assertEquals("queriedTheWasp2.version", queriedTheWasp1Version + 1, queriedTheWasp2.version());
    assertEquals("queriedTheWasp2.name", queriedTheWasp2.name, theWasp2Name);
    final Person queriedIronMan2 = (Person) i3.next();
    assertEquals("queriedIronMan2.version", queriedIronMan1Version + 1, queriedIronMan2.version());
    assertEquals("queriedIronMan2.name", queriedIronMan2.name, ironMan2Name);
  }
  
  @BeforeClass
  public static void beforeAllTests() throws Exception {
    startGeode();
  }
  
  protected static void startGeode() throws Exception{
    System.out.println("startGeode - entered");
    try {
      System.setProperty("gemfire.Query.VERBOSE","true");
      Path tempDir = Files.createTempDirectory("geodeObjectStoreTest");
      serverLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(tempDir.toString())
        .build();
      serverLauncher.start();
      System.out.println("started cache server '" + serverLauncher.getMemberName() + "'");
    }
    finally {
      System.out.println("startGeode - exited");
    }
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
  
  @Before
  public void beforeEachTest() {
    world = World.startWithDefaults("test-world");
    objectStore = world.actorFor(
      ObjectStore.class,
      Definition.has(GeodeObjectStoreActor.class, Definition.NoParameters)
    );
    registeredMappings = new ArrayList<>();
  }
  
  @After
  public void afterEachTest() {
    destroyWorld();
    clearCache();
  }
  
  protected void destroyWorld() {
    world.terminate();
    world = null;
    objectStore = null;
  }
  
  private void clearCache() {
    Optional<GemFireCache> cacheOrNull = GemFireCacheProvider.getAnyInstance();
    if (cacheOrNull.isPresent()) {
      GemFireCache cache = cacheOrNull.get();
      clearEntities(cache);
      clearDispatchables(cache);
    }
  }
  
  private void clearEntities(GemFireCache cache) {
    for (GeodePersistentObjectMapping mapping : registeredMappings) {
      Region<Long, PersistentObject> region = cache.getRegion(mapping.regionName);
      if (region != null) {
        Set<?> keys = region.keySet();
        for (Object key : keys) {
          region.remove(key);
        }
      }
    }
  }
  
  private void clearDispatchables(GemFireCache cache) {
    Region<?, ?> region = cache.getRegion(GeodeQueries.DISPATCHABLES_REGION_PATH);
    if (region != null) {
      Set<?> keys = region.keySet();
      for (Object key : keys) {
        region.remove(key);
      }
    }
  }

}
