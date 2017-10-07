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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.common.params.CoreAdminParams.NODE;


public class Row implements MapWriter {
  public final String node;
  final Cell[] cells;
  public Map<String, Map<String, List<ReplicaInfo>>> collectionVsShardVsReplicas;
  boolean anyValueMissing = false;
  boolean isLive = true;

  public Row(String node, List<String> params, ClusterDataProvider dataProvider) {
    collectionVsShardVsReplicas = dataProvider.getReplicaInfo(node, params);
    if (collectionVsShardVsReplicas == null) collectionVsShardVsReplicas = new HashMap<>();
    this.node = node;
    cells = new Cell[params.size()];
    isLive = dataProvider.getNodes().contains(node);
    Map<String, Object> vals = isLive ? dataProvider.getNodeValues(node, params) : Collections.emptyMap();
    for (int i = 0; i < params.size(); i++) {
      String s = params.get(i);
      cells[i] = new Cell(i, s, Clause.validate(s,vals.get(s), false));
      if (NODE.equals(s)) cells[i].val = node;
      if (cells[i].val == null) anyValueMissing = true;
    }
  }

  public Row(String node, Cell[] cells, boolean anyValueMissing, Map<String,
      Map<String, List<ReplicaInfo>>> collectionVsShardVsReplicas, boolean isLive) {
    this.node = node;
    this.isLive = isLive;
    this.cells = new Cell[cells.length];
    for (int i = 0; i < this.cells.length; i++) {
      this.cells[i] = cells[i].copy();

    }
    this.anyValueMissing = anyValueMissing;
    this.collectionVsShardVsReplicas = collectionVsShardVsReplicas;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put(node, (IteratorWriter) iw -> {
      iw.add((MapWriter) e -> e.put("replicas", collectionVsShardVsReplicas));
      for (Cell cell : cells) iw.add(cell);
    });
  }

  Row copy() {
    return new Row(node, cells, anyValueMissing, Utils.getDeepCopy(collectionVsShardVsReplicas, 3), isLive);
  }

  Object getVal(String name) {
    for (Cell cell : cells) if (cell.name.equals(name)) return cell.val;
    return null;
  }

  @Override
  public String toString() {
    return node;
  }

  // this adds a replica to the replica info
  public Row addReplica(String coll, String shard, Replica.Type type) {
    Row row = copy();
    Map<String, List<ReplicaInfo>> c = row.collectionVsShardVsReplicas.computeIfAbsent(coll, k -> new HashMap<>());
    List<ReplicaInfo> replicas = c.computeIfAbsent(shard, k -> new ArrayList<>());
    replicas.add(new ReplicaInfo("" + new Random().nextInt(1000) + 1000, coll, shard, type, new HashMap<>()));
    for (Cell cell : row.cells) {
      if (cell.name.equals("cores")) {
        cell.val = cell.val == null ? 0 : ((Number) cell.val).longValue() + 1;
      }
    }
    return row;

  }

  public Pair<Row, ReplicaInfo> removeReplica(String coll, String shard, Replica.Type type) {
    Row row = copy();
    Map<String, List<ReplicaInfo>> c = row.collectionVsShardVsReplicas.get(coll);
    if (c == null) return null;
    List<ReplicaInfo> r = c.get(shard);
    if (r == null) return null;
    int idx = -1;
    for (int i = 0; i < r.size(); i++) {
      ReplicaInfo info = r.get(i);
      if (type == null || info.type == type) {
        idx = i;
        break;
      }
    }
    if(idx == -1) return null;

    for (Cell cell : row.cells) {
      if (cell.name.equals("cores")) {
        cell.val = cell.val == null ? 0 : ((Number) cell.val).longValue() - 1;
      }
    }
    return new Pair(row, r.remove(idx));

  }

  public Cell[] getCells() {
    return cells;
  }
}
