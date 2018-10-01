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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CoreAdminParams.NODE;

/**
 * Each instance represents a node in the cluster
 */
public class Row implements MapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public final String node;
  final Cell[] cells;
  //this holds the details of each replica in the node
  public Map<String, Map<String, List<ReplicaInfo>>> collectionVsShardVsReplicas;
  boolean anyValueMissing = false;
  boolean isLive = true;
  Policy.Session session;

  public Row(String node, List<Pair<String, Variable.Type>> params, List<String> perReplicaAttributes, Policy.Session session) {
    this.session = session;
    collectionVsShardVsReplicas = session.nodeStateProvider.getReplicaInfo(node, perReplicaAttributes);
    if (collectionVsShardVsReplicas == null) collectionVsShardVsReplicas = new HashMap<>();
    this.node = node;
    cells = new Cell[params.size()];
    isLive = session.cloudManager.getClusterStateProvider().getLiveNodes().contains(node);
    List<String> paramNames = params.stream().map(Pair::first).collect(Collectors.toList());
    Map<String, Object> vals = isLive ? session.nodeStateProvider.getNodeValues(node, paramNames) : Collections.emptyMap();
    for (int i = 0; i < params.size(); i++) {
      Pair<String, Variable.Type> pair = params.get(i);
      cells[i] = new Cell(i, pair.first(), Clause.validate(pair.first(), vals.get(pair.first()), false), null, pair.second(), this);
      if (NODE.equals(pair.first())) cells[i].val = node;
      if (cells[i].val == null) anyValueMissing = true;
    }
  }

  public void forEachShard(String collection, BiConsumer<String, List<ReplicaInfo>> consumer) {
    collectionVsShardVsReplicas
        .getOrDefault(collection, Collections.emptyMap())
        .forEach(consumer);
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
    if (NODE.equals(name)) return this.node;
    for (Cell cell : cells) if (cell.name.equals(name)) return cell.val;
    return null;
  }

  public Object getVal(String name, Object def) {
    for (Cell cell : cells)
      if (cell.name.equals(name)) {
        return cell.val == null ? def : cell.val;
      }
    return def;
  }

  @Override
  public String toString() {
    return jsonStr();
  }

  public Row addReplica(String coll, String shard, Replica.Type type) {
    return addReplica(coll, shard, type, 0, true);
  }

  public Row addReplica(String coll, String shard, Replica.Type type, boolean strictMode) {
    return addReplica(coll, shard, type, 0, strictMode);
  }

  /**
   * this simulates adding a replica of a certain coll+shard to node. as a result of adding a replica ,
   * values of certain attributes will be modified, in this node as well as other nodes. Please note that
   * the state of the current session is kept intact while this operation is being performed
   *
   * @param coll           collection name
   * @param shard          shard name
   * @param type           replica type
   * @param recursionCount the number of times we have recursed to add more replicas
   * @param strictMode     whether suggester is operating in strict mode or not
   */
  Row addReplica(String coll, String shard, Replica.Type type, int recursionCount, boolean strictMode) {
    if (recursionCount > 3) {
      log.error("more than 3 levels of recursion ", new RuntimeException());
      return this;
    }
    List<OperationInfo> furtherOps = new LinkedList<>();
    Consumer<OperationInfo> opCollector = it -> furtherOps.add(it);
    Row row = null;
    row = session.copy().getNode(this.node);
    if (row == null) throw new RuntimeException("couldn't get a row");
    Map<String, List<ReplicaInfo>> c = row.collectionVsShardVsReplicas.computeIfAbsent(coll, k -> new HashMap<>());
    List<ReplicaInfo> replicas = c.computeIfAbsent(shard, k -> new ArrayList<>());
    String replicaname = "SYNTHETIC." + new Random().nextInt(1000) + 1000;
    ReplicaInfo ri = new ReplicaInfo(replicaname, replicaname, coll, shard, type, this.node,
        Utils.makeMap(ZkStateReader.REPLICA_TYPE, type != null ? type.toString() : Replica.Type.NRT.toString()));
    replicas.add(ri);
    for (Cell cell : row.cells) {
      cell.type.projectAddReplica(cell, ri, opCollector, strictMode);
    }
    for (OperationInfo op : furtherOps) {
      if (op.isAdd) {
        row = row.session.getNode(op.node).addReplica(op.coll, op.shard, op.type, recursionCount + 1, strictMode);
      } else {
        row.session.getNode(op.node).removeReplica(op.coll, op.shard, op.type, recursionCount + 1);
      }
    }

    return row;
  }


  static class OperationInfo {
    final String coll, shard, node, cellName;
    final boolean isAdd;// true =addReplica, false=removeReplica
    final Replica.Type type;


    OperationInfo(String coll, String shard, String node, String cellName, boolean isAdd, Replica.Type type) {
      this.coll = coll;
      this.shard = shard;
      this.node = node;
      this.cellName = cellName;
      this.isAdd = isAdd;
      this.type = type;
    }
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

  public Row removeReplica(String coll, String shard, Replica.Type type) {
    return removeReplica(coll, shard, type, 0);

  }

  // this simulates removing a replica from a node
  public Row removeReplica(String coll, String shard, Replica.Type type, int recursionCount) {
    if (recursionCount > 3) {
      log.error("more than 3 levels of recursion ", new RuntimeException());
      return this;
    }
    List<OperationInfo> furtherOps = new LinkedList<>();
    Consumer<OperationInfo> opCollector = it -> furtherOps.add(it);
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
      cell.type.projectRemoveReplica(cell, removed, opCollector);
    }
    return row;

  }

  public Cell[] getCells() {
    return cells;
  }

  public void forEachReplica(Consumer<ReplicaInfo> consumer) {
    forEachReplica(collectionVsShardVsReplicas, consumer);
  }

  public void forEachReplica(String coll, Consumer<ReplicaInfo> consumer) {
    collectionVsShardVsReplicas.getOrDefault(coll, Collections.emptyMap()).forEach((shard, replicaInfos) -> {
      for (ReplicaInfo replicaInfo : replicaInfos) {
        consumer.accept(replicaInfo);
      }
    });
  }

  public static void forEachReplica(Map<String, Map<String, List<ReplicaInfo>>> collectionVsShardVsReplicas, Consumer<ReplicaInfo> consumer) {
    collectionVsShardVsReplicas.forEach((coll, shardVsReplicas) -> shardVsReplicas
        .forEach((shard, replicaInfos) -> {
          for (int i = 0; i < replicaInfos.size(); i++) {
            ReplicaInfo r = replicaInfos.get(i);
            consumer.accept(r);
          }
        }));
  }
}
