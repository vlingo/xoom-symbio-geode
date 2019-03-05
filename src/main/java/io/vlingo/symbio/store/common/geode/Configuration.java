// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.common.geode;

/**
 * Configuration is responsible for maintaining the configuration
 * for a Geode object or state store.
 */
public class Configuration {
  
  private Role role;
  
  private Configuration() {
    super();
    this.role = Role.Peer;
  }
  
  public static Configuration define() {
    return new Configuration();
  }
  
  public Configuration forPeer() {
    this.role = Role.Peer;
    return this;
  }
  
  public Configuration forClient() {
    this.role = Role.Client;
    return this;
  }
    
  public Role role() {
    return role;
  }
  
  public boolean isPeer() {
    return role == Role.Peer;
  }

  public boolean isClient() {
    return role == Role.Client;
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
