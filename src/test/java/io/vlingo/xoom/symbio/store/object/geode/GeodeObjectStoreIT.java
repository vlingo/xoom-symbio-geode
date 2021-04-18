// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.object.geode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

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
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.StateAdapterProvider;
import io.vlingo.xoom.symbio.store.ListQueryExpression;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.common.MockObjectDispatcher;
import io.vlingo.xoom.symbio.store.common.event.TestEvent;
import io.vlingo.xoom.symbio.store.common.event.TestEventAdapter;
import io.vlingo.xoom.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.xoom.symbio.store.common.geode.GeodeQueries;
import io.vlingo.xoom.symbio.store.common.geode.dispatch.GeodeDispatchable;
import io.vlingo.xoom.symbio.store.common.geode.functions.ClearRegionFunction;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.object.ObjectStore;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.xoom.symbio.store.object.StateObject;
import io.vlingo.xoom.symbio.store.object.StateObjectMapper;
import io.vlingo.xoom.symbio.store.object.StateSources;
import io.vlingo.xoom.symbio.store.object.geode.GeodeObjectStoreActor.GeodeObjectStoreInstantiator;
import io.vlingo.xoom.symbio.store.state.MockObjectResultInterest;
/**
 * GeodeObjectStoreIT implements
 */
public class GeodeObjectStoreIT {

  private static final Logger LOG = LoggerFactory.getLogger(GeodeObjectStoreIT.class);
  private static final String PERSON_REGION_PATH = "Person";

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private World world;
  private List<GeodePersistentObjectMapping> registeredMappings;
  private ObjectStore objectStore;
  private MockObjectDispatcher dispatcher;
  private GeodeObjectStoreDelegate storeDelegate;

  @Test
  public void testThatObjectStoreInsertsOneAndQueries() throws Exception {
    dispatcher.afterCompleting(1);
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    StateObjectMapper personMapper = StateObjectMapper.with(Person.class, personMapping, personMapping);
    storeDelegate.registerMapper(personMapper);

    final long greenLanternId = 300L;
    final Person greenLantern = new Person("Green Lantern", 30, greenLanternId);
    assertEquals(0L, greenLantern.version());
    objectStore.persist(StateSources.of(greenLantern, TestEvent.randomEvent()), persistInterest);

    /* give GeodeUnitOfWorkListener time to run */
    Thread.sleep(3000);

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
                    Collections.singletonList(greenLanternId)),
            queryInterest
    );

    final QuerySingleResult querySingleResult = queryAccess.readFrom("singleResult");
    assertNotNull(querySingleResult);
    assertEquals(greenLantern, querySingleResult.stateObject);

    final Map<String, Dispatchable<Entry<?>, State<?>>> dispatched = dispatcher.getDispatched();
    assertEquals(1, dispatched.size());
  }

  @Test
  public void testThatObjectStoreInsertsMultipleAndQueries() throws Exception {
    dispatcher.afterCompleting(3);
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    StateObjectMapper personMapper = StateObjectMapper.with(Person.class, personMapping, personMapping);
    storeDelegate.registerMapper(personMapper);

    final Person greenLantern = new Person("Green Lantern", 30, 301L);
    final Person theWasp = new Person("The Wasp", 40, 401L);
    final Person ironMan = new Person("Iron Man", 50, 501L);
    objectStore.persistAll(Arrays.asList(StateSources.of(greenLantern), StateSources.of(theWasp), StateSources.of(ironMan)), persistInterest);

    /* give GeodeUnitOfWorkListener time to run */
    Thread.sleep(3000);

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
    assertEquals("persistentObject.size", 3, queryMultiResults.stateObjects.size());
    final Iterator<?> i = queryMultiResults.stateObjects.iterator();
    assertEquals("greenLantern", greenLantern, i.next());
    assertEquals("theWasp", theWasp, i.next());
    assertEquals("ironMan", ironMan, i.next());
  }

  @Test
  public void testThatSingleEntityUpdates() throws Exception {
    dispatcher.afterCompleting(1);
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    StateObjectMapper personMapper = StateObjectMapper.with(Person.class, personMapping, personMapping);
    storeDelegate.registerMapper(personMapper);

    final long greenLanternId = 302L;
    final Person greenLantern = new Person("Green Lantern", 30, greenLanternId);
    assertEquals(0L, greenLantern.version());
    objectStore.persist(StateSources.of(greenLantern), persistInterest);

    /* give GeodeUnitOfWorkListener time to run */
    Thread.sleep(3000);

    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));

    final MockQueryResultInterest queryInterest = new MockQueryResultInterest();
    final AccessSafely queryAccess = queryInterest.afterCompleting(1);

    objectStore.queryObject(
      ListQueryExpression.using(
        Person.class,
        "select * from /Person p where p.persistenceId = $1",
        Collections.singletonList(greenLanternId)),
      queryInterest
    );

    final QuerySingleResult querySingleResult = queryAccess.readFrom("singleResult");
    assertNotNull(querySingleResult);
    final Person queriedPerson = (Person) querySingleResult.stateObject;
    assertEquals(greenLantern, queriedPerson);

    final long queriedPersonVersion = queriedPerson.version();

    final MockPersistResultInterest updateInterest = new MockPersistResultInterest();
    final AccessSafely updateAccess = updateInterest.afterCompleting(1);

    queriedPerson.withAge(31);
    objectStore.persist(StateSources.of(queriedPerson), queryInterest.singleResult.get().updateId, updateInterest);

    /* give GeodeUnitOfWorkListener time to run */
    Thread.sleep(3000);

    assertEquals(Result.Success, updateAccess.readFrom("persistResult"));

    final MockQueryResultInterest requeryInterest = new MockQueryResultInterest();
    final AccessSafely requeryAccess = requeryInterest.afterCompleting(1);

    objectStore.queryObject(
      ListQueryExpression.using(
        Person.class,
        "select * from /Person p where p.persistenceId = $1",
        Collections.singletonList(greenLanternId)),
      requeryInterest
    );

    final QuerySingleResult requerySingleResult = requeryAccess.readFrom("singleResult");
    assertNotNull(requerySingleResult);
    final Person requeriedPerson = (Person) requerySingleResult.stateObject;
    assertEquals(greenLantern, requeriedPerson);
    assertEquals(queriedPersonVersion + 1, requeriedPerson.version());
  }

  @Test
  public void testThatMultipleEntitiesUpdate() throws Exception {
    dispatcher.afterCompleting(5);
    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    StateObjectMapper personMapper = StateObjectMapper.with(Person.class, personMapping, personMapping);
    storeDelegate.registerMapper(personMapper);

    /* insert */

    final Person greenLantern1 = new Person("Green Lantern", 30, 303L);
    final Person theWasp1 = new Person("The Wasp", 40, 403L);
    final Person ironMan1 = new Person("Iron Man", 50, 503L);
    objectStore.persistAll(Arrays.asList(StateSources.of(greenLantern1), StateSources.of(theWasp1), StateSources.of(ironMan1)), persistInterest);

    /* give GeodeUnitOfWorkListener time to run */
    Thread.sleep(3000);

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
    assertEquals("query result set size after insert", 3, queryMultiResults.stateObjects.size());
    final Iterator<?> i = queryMultiResults.stateObjects.iterator();
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

    objectStore.persistAll(Arrays.asList(StateSources.of(greenLantern2), StateSources.of(theWasp2), StateSources.of(ironMan2)), updateInterest);

    /* give GeodeUnitOfWorkListener time to run */
    Thread.sleep(3000);

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
    assertEquals("query result set size after update", 3, requeryMultiResults.stateObjects.size());
    final Iterator<?> i3 = requeryMultiResults.stateObjects.iterator();
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
  public void testRedispatch() throws Exception {
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(5);

    accessDispatcher.writeUsing("processDispatch", false);

    final MockPersistResultInterest persistInterest = new MockPersistResultInterest();
    final AccessSafely persistAccess = persistInterest.afterCompleting(1);

    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping(PERSON_REGION_PATH);
    registeredMappings.add(personMapping);
    StateObjectMapper personMapper = StateObjectMapper.with(Person.class, personMapping, personMapping);
    storeDelegate.registerMapper(personMapper);

    /* insert */

    final Person greenLantern1 = new Person("Green Lantern", 30, 304L);
    final Person theWasp1 = new Person("The Wasp", 40, 404L);
    final Person ironMan1 = new Person("Iron Man", 50, 504L);

    objectStore.persistAll(
      Arrays.asList(
        StateSources.of(greenLantern1, TestEvent.randomEvent()),
        StateSources.of(theWasp1, TestEvent.randomEvent()),
        StateSources.of(ironMan1, TestEvent.randomEvent())
      ), persistInterest);

    /* give GeodeUnitOfWorkListener time to run */
    Thread.sleep(3000);

    assertEquals(Result.Success, persistAccess.readFrom("persistResult"));
    assertEquals(3, (int) persistAccess.readFrom("expectedPersistCount"));
    assertEquals(3, (int) persistAccess.readFrom("actualPersistCount"));

    accessDispatcher.writeUsing("processDispatch", true);

    final Map<String, Dispatchable<Entry<?>, State<?>>> dispatched = accessDispatcher.readFrom("dispatched"); // dispatcher.getDispatched();
    assertEquals(3, dispatched.size());

    final int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
    assertTrue("dispatchAttemptCount", dispatchAttemptCount > 3);

    for (final Dispatchable<Entry<?>, State<?>> dispatchable : dispatched.values()) {
      Assert.assertNotNull(dispatchable.createdOn());
      Assert.assertNotNull(dispatchable.id());
      Assert.assertTrue(dispatchable.state().isPresent());
      Assert.assertEquals(3, dispatched.size());
    }
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
    world = World.startWithDefaults("test-world");
    final String originatorId = "TEST";

    EntryAdapterProvider.instance(world).registerAdapter(TestEvent.class, new TestEventAdapter());
    final StateAdapterProvider stateAdapterProvider = StateAdapterProvider.instance(world);
    MockObjectResultInterest interest = new MockObjectResultInterest();
    dispatcher = new MockObjectDispatcher(interest);
    storeDelegate = new GeodeObjectStoreDelegate(world, ConsistencyPolicy.EVENTUAL, originatorId, stateAdapterProvider);
    objectStore = world.actorFor(
            ObjectStore.class,
            Definition.has(
              GeodeObjectStoreActor.class,
              new GeodeObjectStoreInstantiator(originatorId, storeDelegate, dispatcher(dispatcher)))
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
    storeDelegate.close();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Dispatcher<GeodeDispatchable<State<?>>> dispatcher(MockObjectDispatcher dispatcher) {
    return (Dispatcher) dispatcher;
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
      Region<Long, StateObject> region = cache.getRegion(mapping.regionPath);
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
