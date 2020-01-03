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

package org.apache.solr.cloud;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;

public class ClusterStateMockUtil {

  private final static Pattern BLUEPRINT = Pattern.compile("([a-z])(\\d+)?(?:(['A','R','D','F']))?(\\*)?");

  protected static ZkStateReader buildClusterState(String string, String ... liveNodes) {
    return buildClusterState(string, 1, liveNodes);
  }

  protected static ZkStateReader buildClusterState(String string, int replicationFactor, String ... liveNodes) {
    return buildClusterState(string, replicationFactor, 10, liveNodes);
  }

  /**
   * This method lets you construct a complex ClusterState object by using simple strings of letters.
   *
   * c = collection, s = slice, r = replica, \d = node number (r2 means the replica is on node 2),
   * state = [A,R,D,F], * = replica to replace, binds to the left.
   *
   * For example:
   * csrr2rD*sr2csr
   *
   * Creates:
   *
   * 'csrr2rD*'
   * A collection, a shard, a replica on node 1 (the default) that is active (the default), a replica on node 2, and a replica on node 1
   * that has a state of down and is the replica we will be looking to put somewhere else (the *).
   *
   * 'sr2'
   * Then, another shard that has a replica on node 2.
   *
   * 'csr'
   * Then, another collection that has a shard with a single active replica on node 1.
   *
   * Result:
   *        {
   *         "collection2":{
   *           "maxShardsPerNode":"1",
   *           "replicationFactor":"1",
   *           "shards":{"slice1":{
   *               "state":"active",
   *               "replicas":{"replica5":{
   *                   "state":"active",
   *                   "node_name":"baseUrl1_",
   *                   "base_url":"http://baseUrl1"}}}}},
   *         "collection1":{
   *           "maxShardsPerNode":"1",
   *           "replicationFactor":"1",
   *           "shards":{
   *             "slice1":{
   *               "state":"active",
   *               "replicas":{
   *                 "replica3 (bad)":{
   *                   "state":"down",
   *                   "node_name":"baseUrl1_",
   *                   "base_url":"http://baseUrl1"},
   *                 "replica2":{
   *                   "state":"active",
   *                   "node_name":"baseUrl2_",
   *                   "base_url":"http://baseUrl2"},
   *                 "replica1":{
   *                   "state":"active",
   *                   "node_name":"baseUrl1_",
   *                   "base_url":"http://baseUrl1"}}},
   *             "slice2":{
   *               "state":"active",
   *               "replicas":{"replica4":{
   *                   "state":"active",
   *                   "node_name":"baseUrl2_",
   *                   "base_url":"http://baseUrl2"}}}}}}
   *
   */
  @SuppressWarnings("resource")
  protected static ZkStateReader buildClusterState(String clusterDescription, int replicationFactor, int maxShardsPerNode, String ... liveNodes) {
    Map<String,Slice> slices = null;
    Map<String,Replica> replicas = null;
    Map<String,Object> collectionProps = new HashMap<>();
    collectionProps.put(ZkStateReader.MAX_SHARDS_PER_NODE, Integer.toString(maxShardsPerNode));
    collectionProps.put(ZkStateReader.REPLICATION_FACTOR, Integer.toString(replicationFactor));
    Map<String,DocCollection> collectionStates = new HashMap<>();
    DocCollection docCollection = null;
    String collName = null;
    String sliceName = null;
    Slice slice = null;
    int replicaCount = 1;

    Matcher m = BLUEPRINT.matcher(clusterDescription);
    while (m.find()) {
      Replica replica;
      switch (m.group(1)) {
        case "c":
          slices = new HashMap<>();
          docCollection = new DocCollection(collName = "collection" + (collectionStates.size() + 1), slices, collectionProps, null);
          collectionStates.put(docCollection.getName(), docCollection);
          break;
        case "s":
          replicas = new HashMap<>();
          if(collName == null) collName = "collection" + (collectionStates.size() + 1);
          slice = new Slice(sliceName = "slice" + (slices.size() + 1), replicas, null,  collName);
          slices.put(slice.getName(), slice);
          break;
        case "r":
          Map<String,Object> replicaPropMap = new HashMap<>();
          String node;

          node = m.group(2);

          if (node == null || node.trim().length() == 0) {
            node = "1";
          }

          Replica.State state = Replica.State.ACTIVE;
          String stateCode = m.group(3);

          if (stateCode != null) {
            switch (stateCode.charAt(0)) {
              case 'S':
                state = Replica.State.ACTIVE;
                break;
              case 'R':
                state = Replica.State.RECOVERING;
                break;
              case 'D':
                state = Replica.State.DOWN;
                break;
              case 'F':
                state = Replica.State.RECOVERY_FAILED;
                break;
              default:
                throw new IllegalArgumentException(
                    "Unexpected state for replica: " + stateCode);
            }
          }

          String nodeName = "baseUrl" + node + "_";
          String replicaName = "replica" + replicaCount++;

          replicaPropMap.put(ZkStateReader.NODE_NAME_PROP, nodeName);
          replicaPropMap.put(ZkStateReader.BASE_URL_PROP, "http://baseUrl" + node);
          replicaPropMap.put(ZkStateReader.STATE_PROP, state.toString());
          if(collName == null) collName = "collection" + (collectionStates.size() + 1);
          if(sliceName == null) collName = "slice" + (slices.size() + 1);
          replica = new Replica(replicaName, replicaPropMap, collName, sliceName);

          replicas.put(replica.getName(), replica);
          break;
        default:
          break;
      }
    }

    ClusterState clusterState = new ClusterState(1, new HashSet<>(Arrays.asList(liveNodes)), collectionStates);
    MockZkStateReader reader = new MockZkStateReader(clusterState, collectionStates.keySet());

    String json;
    json = new String(Utils.toJSON(clusterState), StandardCharsets.UTF_8);
    System.err.println(json);

    return reader;
  }


}
