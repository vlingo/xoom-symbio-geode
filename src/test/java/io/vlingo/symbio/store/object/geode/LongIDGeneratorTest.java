// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.geode;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

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
import io.vlingo.symbio.store.common.geode.Configuration;
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.identity.IDGenerator;
import io.vlingo.symbio.store.common.geode.identity.LongIDGenerator;
import io.vlingo.symbio.store.common.geode.identity.LongIDGeneratorActor;
import io.vlingo.symbio.store.common.geode.identity.LongSequence;
/**
 * LongIDGeneratorTest
 */
public class LongIDGeneratorTest {
  
  private static String SEQUENCE_REGION_NAME = "TestSequences";

  private static ServerLauncher serverLauncher;
  private World world;
  private Configuration configuration;
  
  @Test
  public void testLongIDGenerator() throws Exception {
    
    LongIDGenerator generator = new LongIDGenerator(Configuration.define().forPeer(), SEQUENCE_REGION_NAME, 2L);
    
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
  @SuppressWarnings("unchecked")
  public void testLongIDGeneratorActor() throws Exception {
    IDGenerator<Long> generator = world.actorFor(
      IDGenerator.class,
      Definition.has(
        LongIDGeneratorActor.class, 
        Definition.parameters(configuration, SEQUENCE_REGION_NAME, 4L)));
    
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
  public static void beforeAllTests() throws Exception {
    startGeode();
  }
  
  protected static void startGeode() throws Exception{
    System.setProperty("gemfire.Query.VERBOSE","true");
    Path tempDir = Files.createTempDirectory("longIDGeneratorTest");
    serverLauncher = new ServerLauncher.Builder()
      .setWorkingDirectory(tempDir.toString())
      .build();
    serverLauncher.start();
  }
  
  @AfterClass
  public static void afterAllTests() {
    stopGeode();
  }
  
  public static void stopGeode() {
    if (serverLauncher != null) {
      serverLauncher.stop();
      serverLauncher = null;
    }
  }
  
  @Before
  public void beforeEachTest() {
    world = World.startWithDefaults("test-world");
    configuration = Configuration.define().forPeer();
  }
  
  @After
  public void afterEachTest() {
    destroyWorld();
    clearCache();
  }
  
  protected void destroyWorld() {
    world.terminate();
    world = null;
  }
  
  private void clearCache() {
    GemFireCache cache = GemFireCacheProvider.getAnyInstance(configuration);
    clearSequences(cache);
  }

  private void clearSequences(GemFireCache cache) {
    Region<String, LongSequence> region = cache.getRegion(SEQUENCE_REGION_NAME);
    if (region != null) {
      Set<?> keys = region.keySet();
      for (Object key : keys) {
        region.remove(key);
      }
    }
  }
}
