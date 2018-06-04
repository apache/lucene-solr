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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.params.CoreAdminParams.NODE;

/**
 * Each instance represents a node in the cluster
 */
public class Row implements MapWriter {
  public final String node;
  final Cell[] cells;
  //this holds the details of each replica in the node
  public Map<String, Map<String, List<ReplicaInfo>>> collectionVsShardVsReplicas;
  boolean anyValueMissing = false;
  boolean isLive = true;
  Policy.Session session;

  public Row(String node, List<Pair<String, Suggestion.ConditionType>> params, List<String> perReplicaAttributes, Policy.Session session) {
    this.session = session;
    collectionVsShardVsReplicas = session.nodeStateProvider.getReplicaInfo(node, perReplicaAttributes);
    if (collectionVsShardVsReplicas == null) collectionVsShardVsReplicas = new HashMap<>();
    this.node = node;
    cells = new Cell[params.size()];
    isLive = session.cloudManager.getClusterStateProvider().getLiveNodes().contains(node);
    List<String> paramNames = params.stream().map(Pair::first).collect(Collectors.toList());
    Map<String, Object> vals = isLive ? session.nodeStateProvider.getNodeValues(node, paramNames) : Collections.emptyMap();
    for (int i = 0; i < params.size(); i++) {
      Pair<String, Suggestion.ConditionType> pair = params.get(i);
      cells[i] = new Cell(i, pair.first(), Clause.validate(pair.first(), vals.get(pair.first()), false), null, pair.second(), this);
      if (NODE.equals(pair.first())) cells[i].val = node;
      if (cells[i].val == null) anyValueMissing = true;
    }
  }

  public Row(String node, Cell[] cells, boolean anyValueMissing, Map<String,
      Map<String, List<ReplicaInfo>>> collectionVsShardVsReplicas, boolean isLive, Policy.Session session) {
    this.session = session;
    this.node = node;
    this.isLive = isLive;
    this.cells = new Cell[cells.length];
    for (int i = 0; i < this.cells.length; i++) {
      this.cells[i] = cells[i].copy();
      this.cells[i].row = this;
    }
    this.anyValueMissing = anyValueMissing;
    this.collectionVsShardVsReplicas = collectionVsShardVsReplicas;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put(NODE, node);
    ew.put("replicas", collectionVsShardVsReplicas);
    ew.put("isLive", isLive);
    ew.put("attributes", Arrays.asList(cells));
  }

  Row copy(Policy.Session session) {
    return new Row(node, cells, anyValueMissing, Utils.getDeepCopy(collectionVsShardVsReplicas, 3), isLive, session);
  }

  Object getVal(String name) {
    for (Cell cell : cells) if (cell.name.equals(name)) return cell.val;
    return null;
  }

  Object getVal(String name, Object def) {
    for (Cell cell : cells)
      if (cell.name.equals(name)) {
        return cell.val == null ? def : cell.val;
      }
    return def;
  }

  @Override
  public String toString() {
    return node;
  }

  /**
   * this simulates adding a replica of a certain coll+shard to node. as a result of adding a replica ,
   * values of certain attributes will be modified, in this node as well as other nodes. Please note that
   * the state of the current session is kept intact while this operation is being performed
   *
   * @param coll  collection name
   * @param shard shard name
   * @param type  replica type
   */
  public Row addReplica(String coll, String shard, Replica.Type type) {
    Row row = session.copy().getNode(this.node);
    if (row == null) throw new RuntimeException("couldn't get a row");
    Map<String, List<ReplicaInfo>> c = row.collectionVsShardVsReplicas.computeIfAbsent(coll, k -> new HashMap<>());
    List<ReplicaInfo> replicas = c.computeIfAbsent(shard, k -> new ArrayList<>());
    String replicaname = "" + new Random().nextInt(1000) + 1000;
    ReplicaInfo ri = new ReplicaInfo(replicaname, replicaname, coll, shard, type, this.node,
        Utils.makeMap(ZkStateReader.REPLICA_TYPE, type != null ? type.toString() : Replica.Type.NRT.toString()));
    replicas.add(ri);
    for (Cell cell : row.cells) {
      cell.type.projectAddReplica(cell, ri);
    }
    return row;
  }


  public ReplicaInfo getReplica(String coll, String shard, Replica.Type type) {
    Map<String, List<ReplicaInfo>> c = collectionVsShardVsReplicas.get(coll);
    if (c == null) return null;
    List<ReplicaInfo> r = c.get(shard);
    if (r == null) return null;
    int idx = -1;
    for (int i = 0; i < r.size(); i++) {
      ReplicaInfo info = r.get(i);
      if (type == null || info.getType() == type) {
        idx = i;
        break;
      }
    }
    if (idx == -1) return null;
    return r.get(idx);
  }

  // this simulates removing a replica from a node
  public Pair<Row, ReplicaInfo> removeReplica(String coll, String shard, Replica.Type type) {
    Row row = session.copy().getNode(this.node);
    Map<String, List<ReplicaInfo>> c = row.collectionVsShardVsReplicas.get(coll);
    if (c == null) return null;
    List<ReplicaInfo> r = c.get(shard);
    if (r == null) return null;
    int idx = -1;
    for (int i = 0; i < r.size(); i++) {
      ReplicaInfo info = r.get(i);
      if (type == null || info.getType() == type) {
        idx = i;
        break;
      }
    }
    if (idx == -1) return null;
    ReplicaInfo removed = r.remove(idx);
    for (Cell cell : row.cells) {
      cell.type.projectRemoveReplica(cell, removed);
    }
    return new Pair(row, removed);

  }

  public Cell[] getCells() {
    return cells;
  }

  public void forEachReplica(Consumer<ReplicaInfo> consumer) {
    forEachReplica(collectionVsShardVsReplicas, consumer);
  }

  public static void forEachReplica(Map<String, Map<String, List<ReplicaInfo>>> collectionVsShardVsReplicas, Consumer<ReplicaInfo> consumer) {
    collectionVsShardVsReplicas.forEach((coll, shardVsReplicas) -> shardVsReplicas
        .forEach((shard, replicaInfos) -> {
          for (ReplicaInfo r : replicaInfos) consumer.accept(r);
        }));
  }
}
