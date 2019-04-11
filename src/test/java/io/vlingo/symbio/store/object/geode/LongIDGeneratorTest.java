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

import org.apache.geode.distributed.ServerLauncher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.vlingo.symbio.store.common.geode.Configuration;
import io.vlingo.symbio.store.common.geode.LongIDGenerator;
/**
 * LongIDGeneratorTest
 */
public class LongIDGeneratorTest {

  private static ServerLauncher serverLauncher;
  
  @Test
  public void testAllcation() throws Exception {
    
    LongIDGenerator generator = new LongIDGenerator(Configuration.define().forPeer(), "TestSequences", 2L);
    
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
}
