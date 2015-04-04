package org.apache.solr.cloud;

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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SHARDS_PROP;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;

@Ignore("SOLR-6347")
public class DeleteLastCustomShardedReplicaTest extends AbstractFullDistribZkTestBase {
  private CloudSolrClient client;

  @BeforeClass
  public static void beforeThisClass2() throws Exception {

  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
    client = createCloudClient(null);
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    client.close();
  }

  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  public DeleteLastCustomShardedReplicaTest() {
    sliceCount = 2;
    checkCreatedVsState = false;
  }

  @Test
  @ShardsFixed(num = 2)
  public void test() throws Exception {
    int replicationFactor = 1;
    int maxShardsPerNode = 5;

    Map<String, Object> props = ZkNodeProps.makeMap(
        "router.name", ImplicitDocRouter.NAME,
        ZkStateReader.REPLICATION_FACTOR, replicationFactor,
        ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode,
        NUM_SLICES, 1,
        SHARDS_PROP,"a,b");

    Map<String,List<Integer>> collectionInfos = new HashMap<>();

    String collectionName = "customcollreplicadeletion";

    createCollection(collectionInfos, collectionName, props, client);

    waitForRecoveriesToFinish(collectionName, false);

    DocCollection testcoll = getCommonCloudSolrClient().getZkStateReader()
        .getClusterState().getCollection(collectionName);
    Replica replica = testcoll.getSlice("a").getReplicas().iterator().next();

    removeAndWaitForLastReplicaGone(collectionName, replica, "a");
  }

  protected void removeAndWaitForLastReplicaGone(String COLL_NAME, Replica replica, String shard)
      throws SolrServerException, IOException, InterruptedException {
    Map m = makeMap("collection", COLL_NAME, "action", DELETEREPLICA.toLower(), "shard",
        shard, "replica", replica.getName());
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    this.client.request(request);
    long endAt = System.currentTimeMillis() + 3000;
    boolean success = false;
    DocCollection testcoll = null;
    while (System.currentTimeMillis() < endAt) {
      testcoll = getCommonCloudSolrClient().getZkStateReader()
          .getClusterState().getCollection(COLL_NAME);
      // In case of a custom sharded collection, the last replica deletion would also lead to
      // the deletion of the slice.
      success = testcoll.getSlice(shard) == null;
      if (success) {
        log.info("replica cleaned up {}/{} core {}",
            shard + "/" + replica.getName(), replica.getStr("core"));
        log.info("current state {}", testcoll);
        break;
      }
      Thread.sleep(100);
    }
    assertTrue("Replica not cleaned up", success);
  }

}

