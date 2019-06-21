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
import io.vlingo.symbio.store.common.geode.GemFireCacheProvider;
import io.vlingo.symbio.store.common.geode.identity.IDGenerator;
import io.vlingo.symbio.store.common.geode.identity.LongIDGenerator;
import io.vlingo.symbio.store.common.geode.identity.LongIDGeneratorActor;
import io.vlingo.symbio.store.common.geode.identity.LongSequence;
/**
 * LongIDGeneratorTest
 */
public class LongIDGeneratorTest {
  
  private static ServerLauncher serverLauncher;
  private static GemFireCache cache;
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
  @SuppressWarnings("unchecked")
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
  public static void beforeAllTests() throws Exception {
    System.setProperty("gemfire.Query.VERBOSE","true");
    Path tempDir = Files.createTempDirectory("longIDGeneratorTest");
    serverLauncher = new ServerLauncher.Builder()
      .setWorkingDirectory(tempDir.toString())
      .build();
    serverLauncher.start();
    cache = GemFireCacheProvider.forPeer();
  }
  
  @AfterClass
  public static void afterAllTests() {
    if (cache == null) {
      cache.close();
      cache = null;
    }
    if (serverLauncher != null) {
      serverLauncher.stop();
      serverLauncher = null;
    }
  }
  
  @Before
  public void beforeEachTest() {
    world = World.startWithDefaults("test-world");
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
    Region<String, LongSequence> region = cache.getRegion(LongIDGenerator.DEFAULT_SEQUENCE_REGION_PATH);
    if (region != null) {
      Set<?> keys = region.keySet();
      for (Object key : keys) {
        region.remove(key);
      }
    }
  }
}
