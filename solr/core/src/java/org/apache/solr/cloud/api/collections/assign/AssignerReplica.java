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
package org.apache.solr.cloud.api.collections.assign;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents basic information about a replica.
 */
public class AssignerReplica {
  private final String name;
  private final String node;
  private final String collection;
  private final String shard;
  private final String core;
  private final ReplicaType type;
  private final ReplicaState state;
  private final Map<String, Object> properties = new HashMap<>();

  public AssignerReplica(String name, String node,
                         String collection, String shard, String core,
                         ReplicaType type, ReplicaState state,
                         Map<String, Object> properties) {
    this.name = name;
    this.node = node;
    this.collection = collection;
    this.shard = shard;
    this.core = core;
    this.type = type;
    this.state = state;
    if (properties != null) {
      this.properties.putAll(properties);
    }
  }

  // so-called coreNode name
  public String getName() {
    return name;
  }

  public String getNode() {
    return node;
  }

  public String getCollection() {
    return collection;
  }

  public String getShard() {
    return shard;
  }

  // SolrCore name
  public String getCore() {
    return core;
  }

  public ReplicaType getType() {
    return type;
  }

  public ReplicaState getState() {
    return state;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }
}
