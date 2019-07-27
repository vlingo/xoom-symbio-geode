// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.symbio.store.common.geode.ClearRegionFunction;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.identity.IDGenerator;
import io.vlingo.symbio.store.common.geode.identity.LongIDGenerator;
import io.vlingo.symbio.store.common.geode.identity.LongIDGeneratorActor;
import io.vlingo.symbio.store.common.geode.identity.LongSequence;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
/**
 * LongIDGeneratorIT
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class LongIDGeneratorIT {

  private static final Logger LOG = LoggerFactory.getLogger(LongIDGeneratorIT.class);

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;

  private World world;

  @Test
  public void testLongIDGenerator() throws Exception {

    LongIDGenerator generator = new LongIDGenerator(2L);

    String customerSeq = "test.Customer";
    String productSeq = "test.Product";

    assertEquals("next customer ID is 1", new Long(1), generator.next(customerSeq));
    assertEquals("next customer ID is 2", new Long(2), generator.next(customerSeq));
    assertEquals("next product ID is 1", new Long(1), generator.next(productSeq));
    assertEquals("next customer ID is 3", new Long(3), generator.next(customerSeq));
    assertEquals("next customer ID is 4", new Long(4), generator.next(customerSeq));
    assertEquals("next product ID is 2", new Long(2), generator.next(productSeq));
    assertEquals("next customer ID is 5", new Long(5), generator.next(customerSeq));
    assertEquals("next product ID is 3", new Long(3), generator.next(productSeq));
  }

  @Test
  public void testLongIDGeneratorActor() throws Exception {
    IDGenerator<Long> generator = world.actorFor(
      IDGenerator.class,
      Definition.has(
        LongIDGeneratorActor.class,
        Definition.parameters(4L)));

    String customerSeq = "test.Customer";
    String productSeq = "test.Product";

    assertEquals("next customer ID is 1", new Long(1), generator.next(customerSeq).await());
    assertEquals("next customer ID is 2", new Long(2), generator.next(customerSeq).await());
    assertEquals("next product ID is 1", new Long(1), generator.next(productSeq).await());
    assertEquals("next customer ID is 3", new Long(3), generator.next(customerSeq).await());
    assertEquals("next customer ID is 4", new Long(4), generator.next(customerSeq).await());
    assertEquals("next product ID is 2", new Long(2), generator.next(productSeq).await());
    assertEquals("next customer ID is 5", new Long(5), generator.next(customerSeq).await());
    assertEquals("next product ID is 3", new Long(3), generator.next(productSeq).await());
  }

  @BeforeClass
  public static void beforeAnyTest() {
    Properties serverProps = new Properties();
    serverProps.put(ConfigurationProperties.CACHE_XML_FILE, "server-cache.xml");
    serverProps.put(ConfigurationProperties.LOG_LEVEL, "error");

    locator = cluster.startLocatorVM(0, serverProps);
    server1 = cluster.startServerVM(1, serverProps, locator.getPort());
    server2 = cluster.startServerVM(2, serverProps, locator.getPort());

    System.setProperty("LOCATOR_IP", ipAddress());
    System.setProperty("LOCATOR_PORT", String.valueOf(locator.getPort()));
    System.setProperty("gemfire." + ConfigurationProperties.CACHE_XML_FILE, "client-cache.xml");
    System.setProperty("gemfire." + ConfigurationProperties.LOG_LEVEL, "error");
  }

  @Before
  public void beforeEachTest() {
    world = World.startWithDefaults("test-world");
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
  }

  private void clearCache() {
    Optional<GemFireCache> cacheOrNull = GemFireCacheProvider.getAnyInstance();
    if (cacheOrNull.isPresent()) {
      GemFireCache cache = cacheOrNull.get();
      Region<String, LongSequence> region = cache.getRegion(LongIDGenerator.DEFAULT_SEQUENCE_REGION_PATH);
      if (region != null) {
        FunctionService
          .onRegion(region)
          .execute(ClearRegionFunction.class.getSimpleName());
      }
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
