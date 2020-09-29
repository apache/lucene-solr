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
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.ConditionalMapWriter.NON_NULL_VAL;
import static org.apache.solr.common.ConditionalMapWriter.dedupeKeyPredicate;
import static org.apache.solr.common.cloud.ZkStateReader.LEADER_PROP;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class ReplicaInfo implements MapWriter {
  private final String name;
  private final String core, collection, shard;
  private final Replica.Type type;
  private final String node;
  public final boolean isLeader;
  private final Map<String, Object> variables = new HashMap<>();

  public ReplicaInfo(String coll, String shard, Replica r, Map<String, Object> vals) {
    this.name = r.getName();
    this.core = r.getCoreName();
    this.collection = coll;
    this.shard = shard;
    this.type = r.getType();
    this.node = r.getNodeName();
    boolean maybeLeader = r.getBool(LEADER_PROP, false);
    if (vals != null) {
      this.variables.putAll(vals);
      maybeLeader = "true".equals(String.valueOf(vals.getOrDefault(LEADER_PROP, maybeLeader)));
    }
    this.isLeader = maybeLeader;
    validate();
  }

  public ReplicaInfo(String name, String core, String coll, String shard, Replica.Type type, String node, Map<String, Object> vals) {
    if (vals == null) vals = Collections.emptyMap();
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
    validate();
  }

  @SuppressWarnings({"unchecked"})
  public ReplicaInfo(Map<String, Object> map) {
    this.name = map.keySet().iterator().next();
    @SuppressWarnings({"rawtypes"})Map details = (Map) map.get(name);
    details = Utils.getDeepCopy(details, 4);
    this.collection = (String) details.remove("collection");
    this.shard = (String) details.remove("shard");
    this.core = (String) details.remove("core");
    this.node = (String) details.remove("node_name");
    this.isLeader = Boolean.parseBoolean((String) details.getOrDefault("leader", "false"));
    details.remove("leader");
    type = Replica.Type.valueOf((String) details.getOrDefault("type", "NRT"));
    details.remove("type");
    this.variables.putAll(details);
    validate();
  }

  private final void validate() {
    Objects.requireNonNull(this.name, "'name' must not be null");
    Objects.requireNonNull(this.core, "'core' must not be null");
    Objects.requireNonNull(this.collection, "'collection' must not be null");
    Objects.requireNonNull(this.shard, "'shard' must not be null");
    Objects.requireNonNull(this.type, "'type' must not be null");
    Objects.requireNonNull(this.node, "'node' must not be null");
  }

  public Object clone() {
    return new ReplicaInfo(name, core, collection, shard, type, node, new HashMap<>(variables));
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    BiPredicate<CharSequence, Object> p = dedupeKeyPredicate(new HashSet<>())
        .and(NON_NULL_VAL);
    ew.put(name, (MapWriter) ew1 -> {
      ew1.put(ZkStateReader.CORE_NAME_PROP, core, p)
          .put(ZkStateReader.SHARD_ID_PROP, shard, p)
          .put(ZkStateReader.COLLECTION_PROP, collection, p)
          .put(ZkStateReader.NODE_NAME_PROP, node, p)
          .put(ZkStateReader.REPLICA_TYPE, type.toString(), p);
      for (Map.Entry<String, Object> e : variables.entrySet()) ew1.put(e.getKey(), e.getValue(), p);
    });
  }

  /** Replica "coreNode" name. */
  public String getName() {
    return name;
  }

  /** SolrCore name. */
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
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof ReplicaInfo)) {
      return false;
    }
    ReplicaInfo other = (ReplicaInfo)o;
    if (
        name.equals(other.name) &&
        collection.equals(other.collection) &&
        core.equals(other.core) &&
        isLeader == other.isLeader &&
        node.equals(other.node) &&
        shard.equals(other.shard) &&
        type == other.type &&
        variables.equals(other.variables)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, core, collection, shard, type);
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }

  public String getNode() {
    return node;
  }
}
