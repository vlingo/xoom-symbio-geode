// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.geode;
/**
 * Configuration is responsible for maintaining the configuration
 * for a {@link GeodeStateStoreActor}.
 *
 * @author davem
 * @since Oct 14, 2018
 */
public class Configuration {
  
  public final Role role;
  
  public static Configuration forPeer() {
    return new Configuration(Role.Peer);
  }
  
  public static Configuration forClient() {
    return new Configuration(Role.Client);
  }
  
  public Configuration(final Role role) {
    this.role = role;
  }
  
  public static enum Role {
    Peer {
      @Override public boolean isPeer() { return true; }
    }, 
    Client {
      @Override public boolean isClient() { return true; }
    };
    
    public boolean isPeer() { return false; }
    public boolean isClient() { return false; }
  }
}
