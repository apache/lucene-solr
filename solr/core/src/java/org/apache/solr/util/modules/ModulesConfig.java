/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.util.modules;

/**
 * Configuration for module system. Part of solr.xml
 */
public class ModulesConfig {
  boolean active;

  public ModulesConfig(boolean active) {
    this.active = active;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public static class ModulesConfigBuilder {
    boolean active = true;

    public ModulesConfigBuilder setActive(boolean active) {
      this.active = active;
      return this;
    }

    public ModulesConfig build() {
      return new ModulesConfig(active);
    }
  }
}
