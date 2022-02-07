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
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;

/**
 * A utility class that can create mock ZkStateReader objects with custom ClusterState objects created
 * using a simple string based description. See {@link #buildClusterState(String, int, int, String...)} for
 * details on how the cluster state can be created.
 *
 * @lucene.experimental
 */
public class ClusterStateMockUtil {

  private final static Pattern BLUEPRINT = Pattern.compile("([a-z])(\\d+)?(?:(['A','R','D','F']))?(\\*)?");

  public static ZkStateReader buildClusterState(String clusterDescription, String ... liveNodes) {
    return buildClusterState(clusterDescription, 1, liveNodes);
  }

  public static ZkStateReader buildClusterState(String clusterDescription, int replicationFactor, String ... liveNodes) {
    return buildClusterState(clusterDescription, replicationFactor, 10, liveNodes);
  }

  /**
   * This method lets you construct a complex ClusterState object by using simple strings of letters.
   *
   * c = collection, s = slice, r = replica (nrt type, default), n = nrt replica, t = tlog replica, p = pull replica, \d = node number (r2 means the replica is on node 2),
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
  public static ZkStateReader buildClusterState(String clusterDescription, int replicationFactor, int maxShardsPerNode, String ... liveNodes) {
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
          docCollection = new DocCollection(collName = "collection" + (collectionStates.size() + 1), slices, collectionProps, DocRouter.DEFAULT);
          collectionStates.put(docCollection.getName(), docCollection);
          break;
        case "s":
          replicas = new HashMap<>();
          if(collName == null) collName = "collection" + (collectionStates.size() + 1);
          slice = new Slice(sliceName = "slice" + (slices.size() + 1), replicas, null,  collName);
          slices.put(slice.getName(), slice);

          // hack alert: the DocCollection constructor copies over active slices to its active slice map in the constructor
          // but here we construct the DocCollection before creating the slices which breaks code that calls DocCollection.getActiveSlices
          // so here we re-create doc collection with the latest slices map to workaround this problem
          // todo: a better fix would be to have a builder class for DocCollection that builds the final object once all the slices and replicas have been created.
          docCollection = docCollection.copyWithSlices(slices);
          collectionStates.put(docCollection.getName(), docCollection);
          break;
        case "r":
        case "n":
        case "t":
        case "p":
          String node = m.group(2);
          String replicaName = "replica" + replicaCount++;
          String stateCode = m.group(3);

          Map<String, Object> replicaPropMap = makeReplicaProps(sliceName, node, replicaName, stateCode, m.group(1));
          if (collName == null) collName = "collection" + (collectionStates.size() + 1);
          if (sliceName == null) collName = "slice" + (slices.size() + 1);

          // O(n^2) alert! but this is for mocks and testing so shouldn't be used for very large cluster states
          boolean leaderFound = false;
          for (Map.Entry<String, Replica> entry : replicas.entrySet()) {
            Replica value = entry.getValue();
            if ("true".equals(value.get(Slice.LEADER)))  {
              leaderFound = true;
              break;
            }
          }
          if (!leaderFound && !m.group(1).equals("p")) {
            replicaPropMap.put(Slice.LEADER, "true");
          }
          replica = new Replica(replicaName, replicaPropMap, collName, sliceName);
          replicas.put(replica.getName(), replica);

          // hack alert: re-create slice with existing data and new replicas map so that it updates its internal leader attribute
          slice = new Slice(slice.getName(), replicas, null, collName);
          slices.put(slice.getName(), slice);
          // we don't need to update doc collection again because we aren't adding a new slice or changing its state
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

  private static Map<String, Object> makeReplicaProps(String sliceName, String node, String replicaName, String stateCode, String replicaTypeCode) {
    if (node == null || node.trim().length() == 0) {
      node = "1";
    }

    Replica.State state = Replica.State.ACTIVE;
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

    Replica.Type replicaType = Replica.Type.NRT;
    switch (replicaTypeCode)  {
      case "t":
        replicaType = Replica.Type.TLOG;
        break;
      case "p":
        replicaType = Replica.Type.PULL;
        break;
    }

    Map<String,Object> replicaPropMap = new HashMap<>();
    int port = 8982 + Integer.parseInt(node);
    String nodeName = String.format(Locale.ROOT, "baseUrl%s:%d_", node, port);
    replicaPropMap.put(ZkStateReader.NODE_NAME_PROP, nodeName);
    replicaPropMap.put(ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName(nodeName, "http"));
    replicaPropMap.put(ZkStateReader.STATE_PROP, state.toString());
    replicaPropMap.put(ZkStateReader.CORE_NAME_PROP, sliceName + "_" + replicaName);
    replicaPropMap.put(ZkStateReader.REPLICA_TYPE, replicaType.name());
    return replicaPropMap;
  }


}
