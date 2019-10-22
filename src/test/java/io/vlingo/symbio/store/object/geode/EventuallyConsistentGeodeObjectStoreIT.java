package io.vlingo.symbio.store.object.geode;

import io.vlingo.actors.World;
import io.vlingo.symbio.StateAdapterProvider;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.Properties;
/**
 * EventuallyConsistentGeodeObjectStoreIT
 */
@Ignore
public class EventuallyConsistentGeodeObjectStoreIT extends GeodeObjectStoreIT {

  @BeforeClass
  @SuppressWarnings("unused")
  public static void beforeAnyTest() {
    Properties serverProps = new Properties();
    serverProps.put(ConfigurationProperties.CACHE_XML_FILE, "ec-server-cache.xml");
    serverProps.put(ConfigurationProperties.LOG_LEVEL, "error");

    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server1 = cluster.startServerVM(1, serverProps, locator.getPort());
    MemberVM server2 = cluster.startServerVM(2, serverProps, locator.getPort());

    System.setProperty("LOCATOR_IP", ipAddress());
    System.setProperty("LOCATOR_PORT", String.valueOf(locator.getPort()));
    System.setProperty("gemfire." + ConfigurationProperties.CACHE_XML_FILE, "ec-client-cache.xml");
    System.setProperty("gemfire." + ConfigurationProperties.LOG_LEVEL, "error");
  }

  @Override
  protected GeodeObjectStoreDelegate newStoreDelegate(final World world, final String originatorId, final StateAdapterProvider stateAdapterProvider) {
    return new EventuallyConsistentGeodeObjectStoreDelegate(world, originatorId, stateAdapterProvider);
  }
}
