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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.NumberUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for backward compatibility when users update from 6.x or 7.0 to 7.1,
 * then the counter of collection does not exist in Zk
 * TODO Remove in Solr 9.0
 */
public class AssignBackwardCompatibilityTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, 1, 4)
        .setMaxShardsPerNode(1000)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 4);
  }

  @Test
  public void test() throws IOException, SolrServerException, KeeperException, InterruptedException {
    Set<String> coreNames = new HashSet<>();
    Set<String> coreNodeNames = new HashSet<>();

    int numOperations = random().nextInt(15) + 15;
    int numLiveReplicas = 4;

    boolean clearedCounter = false;
    for (int i = 0; i < numOperations; i++) {
      if (log.isInfoEnabled()) {
        log.info("Collection counter={} i={}", getCounter(), i);
      }
      boolean deleteReplica = random().nextBoolean() && numLiveReplicas > 1;
      // No need to clear counter more than one time
      if (random().nextBoolean() && i > 5 && !clearedCounter) {
        log.info("Clear collection counter");
        // clear counter
        cluster.getZkClient().delete("/collections/"+COLLECTION+"/counter", -1, true);
        clearedCounter = true;
      }
      if (deleteReplica) {
        cluster.waitForActiveCollection(COLLECTION, 1, numLiveReplicas);
        DocCollection dc = getCollectionState(COLLECTION);
        Replica replica = getRandomReplica(dc.getSlice("shard1"), (r) -> r.getState() == Replica.State.ACTIVE);
        CollectionAdminRequest.deleteReplica(COLLECTION, "shard1", replica.getName()).process(cluster.getSolrClient());
        coreNames.remove(replica.getCoreName());
        numLiveReplicas--;
      } else {
        CollectionAdminResponse response = CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard1")
            .process(cluster.getSolrClient());
        assertTrue(response.isSuccess());
        String coreName = response.getCollectionCoresStatus()
            .keySet().iterator().next();
        assertFalse("Core name is not unique coreName=" + coreName + " " + coreNames, coreNames.contains(coreName));
        coreNames.add(coreName);
        numLiveReplicas++;
        cluster.waitForActiveCollection(COLLECTION, 1, numLiveReplicas);

        Replica newReplica = getCollectionState(COLLECTION).getReplicas().stream()
            .filter(r -> r.getCoreName().equals(coreName))
            .findAny().get();
        String coreNodeName = newReplica.getName();
        assertFalse("Core node name is not unique", coreNodeNames.contains(coreName));
        coreNodeNames.add(coreNodeName);
      }
    }
  }

  private int getCounter() throws KeeperException, InterruptedException {
    try {
      byte[] data = cluster.getZkClient().getData("/collections/"+COLLECTION+"/counter", null, new Stat(), true);
      int count = NumberUtils.bytesToInt(data);
      if (count < 0) throw new AssertionError("Found negative collection counter " + count);
      return count;
    } catch (KeeperException e) {
      return -1;
    }
  }
}
