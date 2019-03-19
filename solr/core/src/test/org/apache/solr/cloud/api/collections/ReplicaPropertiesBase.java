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
package org.apache.solr.cloud.api.collections;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// Collect useful operations for testing assigning properties to individual replicas
// Could probably expand this to do something creative with getting random slices
// and shards, but for now this will do.
public abstract class ReplicaPropertiesBase extends AbstractFullDistribZkTestBase {

  public static NamedList<Object> doPropertyAction(CloudSolrClient client, String... paramsIn) throws IOException, SolrServerException {
    assertTrue("paramsIn must be an even multiple of 2, it is: " + paramsIn.length, (paramsIn.length % 2) == 0);
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (int idx = 0; idx < paramsIn.length; idx += 2) {
      params.set(paramsIn[idx], paramsIn[idx + 1]);
    }
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    return client.request(request);
  }

  public static void verifyPropertyNotPresent(CloudSolrClient client, String collectionName, String replicaName,
                                String property)
      throws KeeperException, InterruptedException {
    ClusterState clusterState = null;
    Replica replica = null;
    for (int idx = 0; idx < 300; ++idx) {
      clusterState = client.getZkStateReader().getClusterState();
      final DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);
      replica = (docCollection == null) ? null : docCollection.getReplica(replicaName);
      if (replica == null) {
        fail("Could not find collection/replica pair! " + collectionName + "/" + replicaName);
      }
      if (StringUtils.isBlank(replica.getProperty(property))) return;
      Thread.sleep(100);
    }
    fail("Property " + property + " not set correctly for collection/replica pair: " +
        collectionName + "/" + replicaName + ". Replica props: " + replica.getProperties().toString() +
        ". Cluster state is " + clusterState.toString());

  }

  // The params are triplets,
  // collection
  // shard
  // replica
  public static void verifyPropertyVal(CloudSolrClient client, String collectionName,
                         String replicaName, String property, String val)
      throws InterruptedException, KeeperException {
    Replica replica = null;
    ClusterState clusterState = null;

    for (int idx = 0; idx < 300; ++idx) { // Keep trying while Overseer writes the ZK state for up to 30 seconds.
      clusterState = client.getZkStateReader().getClusterState();
      final DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);
      replica = (docCollection == null) ? null : docCollection.getReplica(replicaName);
      if (replica == null) {
        fail("Could not find collection/replica pair! " + collectionName + "/" + replicaName);
      }
      if (StringUtils.equals(val, replica.getProperty(property))) return;
      Thread.sleep(100);
    }

    fail("Property '" + property + "' with value " + replica.getProperty(property) +
        " not set correctly for collection/replica pair: " + collectionName + "/" + replicaName + " property map is " +
        replica.getProperties().toString() + ".");

  }

  // Verify that
  // 1> the property is only set once in all the replicas in a slice.
  // 2> the property is balanced evenly across all the nodes hosting collection
  public static void verifyUniqueAcrossCollection(CloudSolrClient client, String collectionName,
                                    String property) throws KeeperException, InterruptedException {
    verifyUnique(client, collectionName, property, true);
  }

  public static void verifyUniquePropertyWithinCollection(CloudSolrClient client, String collectionName,
                            String property) throws KeeperException, InterruptedException {
    verifyUnique(client, collectionName, property, false);
  }

  public static void verifyUnique(CloudSolrClient client, String collectionName, String property, boolean balanced)
      throws KeeperException, InterruptedException {

    DocCollection col = null;
    for (int idx = 0; idx < 300; ++idx) {
      ClusterState clusterState = client.getZkStateReader().getClusterState();

      col = clusterState.getCollection(collectionName);
      if (col == null) {
        fail("Could not find collection " + collectionName);
      }
      Map<String, Integer> counts = new HashMap<>();
      Set<String> uniqueNodes = new HashSet<>();
      boolean allSlicesHaveProp = true;
      boolean badSlice = false;
      for (Slice slice : col.getSlices()) {
        boolean thisSliceHasProp = false;
        int propCount = 0;
        for (Replica replica : slice.getReplicas()) {
          uniqueNodes.add(replica.getNodeName());
          String propVal = replica.getProperty(property);
          if (StringUtils.isNotBlank(propVal)) {
            ++propCount;
            if (counts.containsKey(replica.getNodeName()) == false) {
              counts.put(replica.getNodeName(), 0);
            }
            int count = counts.get(replica.getNodeName());
            thisSliceHasProp = true;
            counts.put(replica.getNodeName(), count + 1);
          }
        }
        badSlice = (propCount > 1) ? true : badSlice;
        allSlicesHaveProp = allSlicesHaveProp ? thisSliceHasProp : allSlicesHaveProp;
      }
      if (balanced == false && badSlice == false) {
        return;
      }
      if (allSlicesHaveProp && balanced) {
        // Check that the properties are evenly distributed.
        int minProps = col.getSlices().size() / uniqueNodes.size();
        int maxProps = minProps;

        if (col.getSlices().size() % uniqueNodes.size() > 0) {
          ++maxProps;
        }
        boolean doSleep = false;
        for (Map.Entry<String, Integer> ent : counts.entrySet()) {
          if (ent.getValue() != minProps && ent.getValue() != maxProps) {
            doSleep = true;
          }
        }

        if (doSleep == false) {
          assertTrue("We really shouldn't be calling this if there is no node with the property " + property,
              counts.size() > 0);
          return;
        }
      }
      Thread.sleep(100);
    }
    fail("Collection " + collectionName + " does not have roles evenly distributed. Collection is: " + col.toString());
  }

}
