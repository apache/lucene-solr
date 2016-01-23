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

import org.noggit.JSONUtil;
import org.noggit.JSONWriter;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A Slice contains immutable information about a logical shard (all replicas that share the same shard id).
 */
public class Slice extends ZkNodeProps {
  
  /** The slice's state. */
  public enum State {
    
    /** The default state of a slice. */
    ACTIVE,
    
    /**
     * A slice is put in that state after it has been successfully split. See
     * <a href="https://cwiki.apache.org/confluence/display/solr/Collections+API#CollectionsAPI-api3">
     * the reference guide</a> for more details.
     */
    INACTIVE,
    
    /**
     * When a shard is split, the new sub-shards are put in that state while the
     * split operation is in progress. A shard in that state still receives
     * update requests from the parent shard leader, however does not participate
     * in distributed search.
     */
    CONSTRUCTION,
    
    /**
     * Sub-shards of a split shard are put in that state, when they need to
     * create replicas in order to meet the collection's replication factor. A
     * shard in that state still receives update requests from the parent shard
     * leader, however does not participate in distributed search.
     */
    RECOVERY;
    
    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }
    
    /** Converts the state string to a State instance. */
    public static State getState(String stateStr) {
      return State.valueOf(stateStr.toUpperCase(Locale.ROOT));
    }
  }
  
  public static final String REPLICAS = "replicas";
  public static final String RANGE = "range";
  public static final String LEADER = "leader";       // FUTURE: do we want to record the leader as a slice property in the JSON (as opposed to isLeader as a replica property?)
  public static final String PARENT = "parent";

  private final String name;
  private final DocRouter.Range range;
  private final Integer replicationFactor;      // FUTURE: optional per-slice override of the collection replicationFactor
  private final Map<String,Replica> replicas;
  private final Replica leader;
  private final State state;
  private final String parent;
  private final Map<String, RoutingRule> routingRules;

  /**
   * @param name  The name of the slice
   * @param replicas The replicas of the slice.  This is used directly and a copy is not made.  If null, replicas will be constructed from props.
   * @param props  The properties of the slice - a shallow copy will always be made.
   */
  public Slice(String name, Map<String,Replica> replicas, Map<String,Object> props) {
    super( props==null ? new LinkedHashMap<String,Object>(2) : new LinkedHashMap<>(props));
    this.name = name;

    Object rangeObj = propMap.get(RANGE);
    if (propMap.get(ZkStateReader.STATE_PROP) != null) {
      this.state = State.getState((String) propMap.get(ZkStateReader.STATE_PROP));
    } else {
      this.state = State.ACTIVE;                         //Default to ACTIVE
      propMap.put(ZkStateReader.STATE_PROP, state.toString());
    }
    DocRouter.Range tmpRange = null;
    if (rangeObj instanceof DocRouter.Range) {
      tmpRange = (DocRouter.Range)rangeObj;
    } else if (rangeObj != null) {
      // Doesn't support custom implementations of Range, but currently not needed.
      tmpRange = DocRouter.DEFAULT.fromString(rangeObj.toString());
    }
    range = tmpRange;

    /** debugging.  this isn't an error condition for custom sharding.
    if (range == null) {
      System.out.println("###### NO RANGE for " + name + " props=" + props);
    }
    **/

    if (propMap.containsKey(PARENT) && propMap.get(PARENT) != null)
      this.parent = (String) propMap.get(PARENT);
    else
      this.parent = null;

    replicationFactor = null;  // future

    // add the replicas *after* the other properties (for aesthetics, so it's easy to find slice properties in the JSON output)
    this.replicas = replicas != null ? replicas : makeReplicas((Map<String,Object>)propMap.get(REPLICAS));
    propMap.put(REPLICAS, this.replicas);

    Map<String, Object> rules = (Map<String, Object>) propMap.get("routingRules");
    if (rules != null) {
      this.routingRules = new HashMap<>();
      for (Map.Entry<String, Object> entry : rules.entrySet()) {
        Object o = entry.getValue();
        if (o instanceof Map) {
          Map map = (Map) o;
          RoutingRule rule = new RoutingRule(entry.getKey(), map);
          routingRules.put(entry.getKey(), rule);
        } else {
          routingRules.put(entry.getKey(), (RoutingRule) o);
        }
      }
    } else {
      this.routingRules = null;
    }

    leader = findLeader();
  }


  private Map<String,Replica> makeReplicas(Map<String,Object> genericReplicas) {
    if (genericReplicas == null) return new HashMap<>(1);
    Map<String,Replica> result = new LinkedHashMap<>(genericReplicas.size());
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
   */
  public Map<String, Replica> getReplicasMap() {
    return replicas;
  }

  public Map<String,Replica> getReplicasCopy() {
    return new LinkedHashMap<>(replicas);
  }

  public Replica getLeader() {
    return leader;
  }

  public Replica getReplica(String replicaName) {
    return replicas.get(replicaName);
  }

  public DocRouter.Range getRange() {
    return range;
  }

  public State getState() {
    return state;
  }

  public String getParent() {
    return parent;
  }

  public Map<String, RoutingRule> getRoutingRules() {
    return routingRules;
  }

  @Override
  public String toString() {
    return name + ':' + JSONUtil.toJSON(propMap);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(propMap);
  }
}
