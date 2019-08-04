package io.vlingo.symbio.store.object.geode;

public enum ConsistencyMode {
  EVENTUAL,
  TRANSACTIONAL;

  public boolean isEventual() {
    return this == EVENTUAL;
  }

  public boolean isTransactional() {
    return this == TRANSACTIONAL;
  }
}
