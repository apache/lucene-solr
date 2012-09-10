package org.apache.solr.common.cloud;

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

import org.apache.noggit.JSONWriter;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A Slice contains immutable information about a logical shard (all replicas that share the same shard id).
 */
public class Slice extends ZkNodeProps {
  public static String REPLICAS = "replicas";
  public static String RANGE = "range";
  public static String LEADER = "leader";

  private final String name;
  private final HashPartitioner.Range range;
  // private final Integer replicationFactor;
  private final Map<String,Replica> replicas;
  private final Replica leader;

  public Slice(String name, Map<String,Replica> replicas) {
    this(name, replicas, null);
  }

  public Slice(String name, Map<String,Replica> replicas, Map<String,Object> props) {
    super( props==null ? new LinkedHashMap<String,Object>(2) : new LinkedHashMap<String,Object>(props));
    this.name = name;
    this.replicas = replicas != null ? replicas : makeReplicas((Map<String,Object>)propMap.get(REPLICAS));
    propMap.put(REPLICAS, replicas);

    String rangeStr = (String)propMap.get(RANGE);
    HashPartitioner.Range tmpRange = null;
    if (rangeStr != null) {
      HashPartitioner hp = new HashPartitioner();
      tmpRange = hp.fromString(rangeStr);
    }

    range = tmpRange;
    // replicationFactor = null;  // future
    leader = findLeader();
  }


  private Map<String,Replica> makeReplicas(Map<String,Object> genericReplicas) {
    if (genericReplicas == null) return new HashMap<String,Replica>(1);
    Map<String,Replica> result = new LinkedHashMap<String, Replica>(genericReplicas.size());
    for (Map.Entry<String,Object> entry : genericReplicas.entrySet()) {
      String name = entry.getKey();
      Object val = entry.getValue();
      Replica r;
      if (val instanceof Replica) {
        r = (Replica)val;
      } else {
        r = new Replica(name, (Map<String,Object>)val);
      }
      result.put(name, r);
    }
    return result;
  }

  private Replica findLeader() {
    for (Replica replica : replicas.values()) {
      if (replica.getStr(LEADER) != null) return replica;
    }
    return null;
  }

  /**
   * Return slice name (shard id).
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the list of replicas for this slice.
   */
  public Collection<Replica> getReplicas() {
    return replicas.values();
  }

  /**
   * Get the map of coreNodeName to replicas for this slice.
   *
   * @return map containing coreNodeName as the key, see
   *         {@link ZkStateReader#getCoreNodeName(String, String)}, Replica
   *         as the value.
   */
  public Map<String, Replica> getReplicasMap() {
    return replicas;
  }

  public Map<String,Replica> getReplicasCopy() {
    return new LinkedHashMap<String,Replica>(replicas);
  }

  public Replica getLeader() {
    return leader;
  }

  /*
  // returns a copy of this slice containing the new replica
  public Slice addReplica(Replica replica) {
    Map<String, Object> newProps = new LinkedHashMap<String,Object>(props);
    Map<String, Replica> replicas = getReplicasMap();
    Map<String, Replica> newReplicas = replicas == null ? new HashMap<String, Replica>(1) : new LinkedHashMap<String, Replica>(replicas);
//    newReplicas.put(replica.getName(), replica);
    newProps.put(REPLICAS, replicas);
    return new Slice(name, newProps); // TODO: new constructor that takes replicas as-is w/o rebuilding
  }

  public static Slice newSlice(String name) {
    Map<String, Object> props = new HashMap<String,Object>(1);
    props.put("replicas", new HashMap<String,Object>(1));
    return new Slice(name, props);
  }
   ***/

  @Override
  public String toString() {
    return "Slice [replicas=" + replicas + ", name=" + name + "]";
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(replicas);
  }
}
