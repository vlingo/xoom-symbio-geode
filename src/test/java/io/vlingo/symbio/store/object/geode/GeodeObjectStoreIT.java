// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.common.MockObjectDispatcher;
import io.vlingo.symbio.store.common.event.Event;
import io.vlingo.symbio.store.common.event.TestEvent;
import io.vlingo.symbio.store.common.event.TestEventAdapter;
import io.vlingo.symbio.store.common.geode.ClearRegionFunction;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.GeodeQueries;
import io.vlingo.symbio.store.common.geode.pdx.PdxSerializerRegistry;
import io.vlingo.symbio.store.common.geode.pdx.PersonSerializer;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.object.*;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.symbio.store.state.MockObjectResultInterest;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.net.InetAddress;
import java.util.*;

import static org.junit.Assert.*;
/**
 * GeodeObjectStoreIT implements
 */
public class GeodeObjectStoreIT {

  private static final Logger LOG = LoggerFactory.getLogger(GeodeObjectStoreIT.class);
  private static final String PERSON_REGION_PATH = "/Person";

  @ClassRule
  public static DockerComposeContainer environment =
    new DockerComposeContainer(new File("docker/docker-compose.yml"))
      .withEnv("HOST_IP", hostIP())
      .withEnv("USER_DIR", System.getProperty("user.dir"))
      .withLogConsumer("locator", new Slf4jLogConsumer(LOG))
      .withExposedService("server1", 40404)
      .withLogConsumer("server1", new Slf4jLogConsumer(LOG))
      .waitingFor("server1", Wait.forLogMessage(".*is currently online.*", 1))
      .withExposedService("server2", 40405)
      .withLogConsumer("server2", new Slf4jLogConsumer(LOG))
      .waitingFor("server2", Wait.forLogMessage(".*is currently online.*", 1));

  private World world;
  private List<GeodePersistentObjectMapping> registeredMappings;
  private ObjectStore objectStore;
  private MockObjectResultInterest interest;
  private MockObjectDispatcher dispatcher;

  @Test
  public void testThatObjectStoreInsertsOneAndQueries() {
    dispatcher.afterCompleting(1);
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);

    final long greenLanternId = 300L;
    final Person greenLantern = new Person("Green Lantern", 30, greenLanternId);
    assertEquals(0L, greenLantern.version());
    objectStore.persist(greenLantern, Collections.singletonList(TestEvent.randomEvent()), persistInterest);

    final Person storedGreenLantern = persistAccess.readFrom("persistedObject");
    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    assertEquals(greenLantern, storedGreenLantern);
    assertEquals(1, (int) persistAccess.readFrom("expectedPersistCount"));
    assertEquals(1, (int) persistAccess.readFrom("actualPersistCount"));
    assertEquals(1L, storedGreenLantern.version());

    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);

    objectStore.queryObject(
            ListQueryExpression.using(
                    Person.class,
                    "select * from /Person p where p.persistenceId = $1",
                    Arrays.asList(greenLanternId)),
            queryInterest
    );

    final QuerySingleResult querySingleResult = queryAccess.readFrom("singleResult");
    assertNotNull(querySingleResult);
    assertEquals(greenLantern, querySingleResult.persistentObject);

    final Map<String, Dispatchable<Entry<?>, State<?>>> dispatched = dispatcher.getDispatched();
    assertEquals(1, dispatched.size());
  }

  @Test
  public void testThatObjectStoreInsertsMultipleAndQueries() {
    dispatcher.afterCompleting(3);
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);

    final Person greenLantern = new Person("Green Lantern", 30, 301L);
    final Person theWasp = new Person("The Wasp", 40, 401L);
    final Person ironMan = new Person("Iron Man", 50, 501L);
    objectStore.persistAll(Arrays.asList(greenLantern, theWasp, ironMan), persistInterest);

    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    assertEquals(3, (int) persistAccess.readFrom("expectedPersistCount"));
    assertEquals(3, (int) persistAccess.readFrom("actualPersistCount"));

    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);

    objectStore.queryAll(
      QueryExpression.using(
        Person.class,
        "select * from /Person p order by p.persistenceId"),
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
    dispatcher.afterCompleting(1);
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);

    final long greenLanternId = 302L;
    final Person greenLantern = new Person("Green Lantern", 30, greenLanternId);
    assertEquals(0L, greenLantern.version());
    objectStore.persist(greenLantern, persistInterest);

    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));

    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);

    objectStore.queryObject(
      ListQueryExpression.using(
        Person.class,
        "select * from /Person p where p.persistenceId = $1",
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
        "select * from /Person p where p.persistenceId = $1",
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
    clearEntities(GemFireCacheProvider.getAnyInstance().get());

    dispatcher.afterCompleting(5);
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);

    /* insert */

    final Person greenLantern1 = new Person("Green Lantern", 30, 303L);
    final Person theWasp1 = new Person("The Wasp", 40, 403L);
    final Person ironMan1 = new Person("Iron Man", 50, 503L);
    objectStore.persistAll(Arrays.asList(greenLantern1, theWasp1, ironMan1), persistInterest);

    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    assertEquals(3, (int) persistAccess.readFrom("expectedPersistCount"));
    assertEquals(3, (int) persistAccess.readFrom("actualPersistCount"));

    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);

    objectStore.queryAll(
      QueryExpression.using(
        Person.class,
        "select * from /Person p order by p.persistenceId"),
      queryInterest
    );

    final QueryMultiResults queryMultiResults = queryAccess.readFrom("multiResults");
    assertNotNull(queryMultiResults);
    assertEquals("query result set size after insert", 3, queryMultiResults.persistentObjects.size());
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
        "select * from /Person p order by p.persistenceId"),
      requeryInterest
    );

    final QueryMultiResults requeryMultiResults = requeryAccess.readFrom("multiResults");
    assertNotNull(requeryMultiResults);
    assertEquals("query result set size after update", 3, requeryMultiResults.persistentObjects.size());
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

  @Test
  public void testRedispatch() {
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(5);

    accessDispatcher.writeUsing("processDispatch", false);

    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    PersistentObjectMapper personMapper = PersistentObjectMapper.with(Person.class, personMapping, personMapping);
    objectStore.registerMapper(personMapper);

    /* insert */

    final Person greenLantern1 = new Person("Green Lantern", 30, 304L);
    final Person theWasp1 = new Person("The Wasp", 40, 404L);
    final Person ironMan1 = new Person("Iron Man", 50, 504L);

    final List<Source<Event>> sources = Arrays.asList(TestEvent.randomEvent(), TestEvent.randomEvent());
    objectStore.persistAll(Arrays.asList(greenLantern1, theWasp1, ironMan1), sources, persistInterest);

    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    assertEquals(3, (int) persistAccess.readFrom("expectedPersistCount"));
    assertEquals(3, (int) persistAccess.readFrom("actualPersistCount"));

    try {
      Thread.sleep(3000);
    } catch (InterruptedException ex) {
      //ignored
    }

    accessDispatcher.writeUsing("processDispatch", true);

    final Map<String, Dispatchable<Entry<?>, State<?>>> dispatched = dispatcher.getDispatched();
    assertEquals(3, dispatched.size());

    final int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
    assertTrue("dispatchAttemptCount", dispatchAttemptCount > 3);

    for (final Dispatchable<Entry<?>, State<?>> dispatchable : dispatched.values()) {
      Assert.assertNotNull(dispatchable.createdOn());
      Assert.assertNotNull(dispatchable.id());
      Assert.assertTrue(dispatchable.state().isPresent());
      final Collection<Entry<?>> dispatchedEntries = dispatchable.entries();
      Assert.assertEquals(sources.size(), dispatchedEntries.size());
    }
  }

  @Before
  public void beforeEachTest() {
    System.setProperty("HOST_IP", hostIP());

    PdxSerializerRegistry.serializeTypeWith(Person.class, PersonSerializer.class);

    world = World.startWithDefaults("test-world");
    final String originatorId = "TEST";

    EntryAdapterProvider.instance(world).registerAdapter(TestEvent.class, new TestEventAdapter());

    interest = new MockObjectResultInterest();
    dispatcher = new MockObjectDispatcher(interest);
    objectStore = world.actorFor(
            ObjectStore.class,
            Definition.has(GeodeObjectStoreActor.class,
                    Definition.parameters(originatorId, dispatcher))
    );
    registeredMappings = new ArrayList<>();
  }

  @After
  public void afterEachTest() {
    destroyWorld();
    clearCache();
    GemFireCacheProvider.forClient().close();
  }

  private void destroyWorld() {
    objectStore.close();
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
        FunctionService
          .onRegion(region)
          .execute(ClearRegionFunction.class.getSimpleName());
      }
    }
  }

  private void clearDispatchables(GemFireCache cache) {
    Region<?, ?> region = cache.getRegion(GeodeQueries.DISPATCHABLES_REGION_PATH);
    if (region != null) {
      FunctionService
        .onRegion(region)
        .execute(ClearRegionFunction.class.getSimpleName());
    }
  }

  private static String hostIP() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    }
    catch (Throwable t) {
      LOG.error("error looking up host IP address; defaulting to loopback", t);
      return InetAddress.getLoopbackAddress().getHostAddress();
    }
  }
}
