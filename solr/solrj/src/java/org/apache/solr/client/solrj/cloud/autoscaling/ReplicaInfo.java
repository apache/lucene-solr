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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.cloud.ZkStateReader.LEADER_PROP;


public class ReplicaInfo implements MapWriter {
  private final String name;
  private String core, collection, shard;
  private Replica.Type type;
  private String node;
  public final boolean isLeader;
  private final Map<String, Object> variables = new HashMap<>();

  public ReplicaInfo(String coll, String shard, Replica r, Map<String, Object> vals) {
    this.name = r.getName();
    this.core = r.getCoreName();
    this.collection = coll;
    this.shard = shard;
    this.type = r.getType();
    this.isLeader = r.getBool(LEADER_PROP, false);
    if (vals != null) {
      this.variables.putAll(vals);
    }
    this.node = r.getNodeName();
  }

  public ReplicaInfo(String name, String core, String coll, String shard, Replica.Type type, String node, Map<String, Object> vals) {
    if(vals==null) vals = Collections.emptyMap();
    this.name = name;
    if (vals != null) {
      this.variables.putAll(vals);
    }
    this.isLeader = "true".equals(String.valueOf(vals.getOrDefault(LEADER_PROP, "false")));
    this.collection = coll;
    this.shard = shard;
    this.type = type;
    this.core = core;
    this.node = node;
  }

  public Object clone() {
    return new ReplicaInfo(name, core, collection, shard, type, node, variables);
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put(name, (MapWriter) ew1 -> {
      for (Map.Entry<String, Object> e : variables.entrySet()) {
        ew1.put(e.getKey(), e.getValue());
      }
      if (core != null && !variables.containsKey(ZkStateReader.CORE_NAME_PROP)) {
        ew1.put(ZkStateReader.CORE_NAME_PROP, core);
      }
      if (shard != null && !variables.containsKey(ZkStateReader.SHARD_ID_PROP)) {
        ew1.put(ZkStateReader.SHARD_ID_PROP, shard);
      }
      if (collection != null && !variables.containsKey(ZkStateReader.COLLECTION_PROP)) {
        ew1.put(ZkStateReader.COLLECTION_PROP, collection);
      }
      if (node != null && !variables.containsKey(ZkStateReader.NODE_NAME_PROP)) {
        ew1.put(ZkStateReader.NODE_NAME_PROP, node);
      }
      if (type != null) ew1.put(ZkStateReader.REPLICA_TYPE, type.toString());
    });
  }

  public String getName() {
    return name;
  }

  public String getCore() {
    return core;
  }

  public String getCollection() {
    return collection;
  }

  public String getShard() {
    return shard;
  }

  public Replica.Type getType() {
    Object o = type == null ? variables.get(ZkStateReader.REPLICA_TYPE) : type;
    if (o == null) {
      return Replica.Type.NRT;
    } else if (o instanceof Replica.Type) {
      return (Replica.Type)o;
    } else {
      Replica.Type type = Replica.Type.get(String.valueOf(o).toUpperCase(Locale.ROOT));
      return type;
    }
  }

  public Replica.State getState() {
    if (variables.get(ZkStateReader.STATE_PROP) != null) {
      return Replica.State.getState((String) variables.get(ZkStateReader.STATE_PROP));
    } else {
      // default to ACTIVE
      variables.put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
      return Replica.State.ACTIVE;
    }
  }

  public Map<String, Object> getVariables() {
    return variables;
  }

  public Object getVariable(String name) {
    return variables.get(name);
  }

  public Object getVariable(String name, Object defValue) {
    Object o = variables.get(name);
    if (o != null) {
      return o;
    } else {
      return defValue;
    }
  }

  public boolean getBool(String name, boolean defValue) {
    Object o = getVariable(name, defValue);
    if (o instanceof Boolean) {
      return (Boolean)o;
    } else {
      return Boolean.parseBoolean(String.valueOf(o));
    }
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }

  public String getNode() {
    return node;
  }
}
