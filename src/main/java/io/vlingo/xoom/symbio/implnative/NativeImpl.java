package io.vlingo.xoom.symbio.implnative;


import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.store.object.geode.GeodePersistentObjectMapping;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

public final class NativeImpl {
  @CEntryPoint(name = "Java_io_vlingo_xoom_symbio_geodenative_Native_start")
  public static int start(@CEntryPoint.IsolateThreadContext long isolateId, CCharPointer name) {
    final String nameString = CTypeConversion.toJavaString(name);
    World world = World.startWithDefaults(nameString);
    GeodePersistentObjectMapping personMapping = new GeodePersistentObjectMapping("");

    return 0;
  }
}
