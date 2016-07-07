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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.SHARDS_PROP;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;

public class DeleteLastCustomShardedReplicaTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected String getSolrXml() {
    return "solr.xml";
  }

  public DeleteLastCustomShardedReplicaTest() {
    sliceCount = 2;
  }

  @Test
  @ShardsFixed(num = 2)
  public void test() throws Exception {
    try (CloudSolrClient client = createCloudClient(null))  {
      int replicationFactor = 1;
      int maxShardsPerNode = 5;

      Map<String, Object> props = Utils.makeMap(
          "router.name", ImplicitDocRouter.NAME,
          ZkStateReader.REPLICATION_FACTOR, replicationFactor,
          ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode,
          NUM_SLICES, 1,
          SHARDS_PROP, "a,b");

      Map<String,List<Integer>> collectionInfos = new HashMap<>();

      String collectionName = "customcollreplicadeletion";

      createCollection(collectionInfos, collectionName, props, client);

      waitForRecoveriesToFinish(collectionName, false);

      DocCollection testcoll = getCommonCloudSolrClient().getZkStateReader()
              .getClusterState().getCollection(collectionName);
      Replica replica = testcoll.getSlice("a").getReplicas().iterator().next();

      removeAndWaitForReplicaGone(client, collectionName, replica, "a", replicationFactor-1);
    }
  }

  protected void removeAndWaitForReplicaGone(CloudSolrClient client, String COLL_NAME, Replica replica, String shard,
      final int expectedNumReplicasRemaining)
      throws SolrServerException, IOException, InterruptedException {
    Map m = makeMap("collection", COLL_NAME, "action", DELETEREPLICA.toLower(), "shard",
        shard, "replica", replica.getName());
    SolrParams params = new MapSolrParams(m);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);
    TimeOut timeout = new TimeOut(3, TimeUnit.SECONDS);
    boolean success = false;
    DocCollection testcoll = null;
    while (! timeout.hasTimedOut()) {
      testcoll = getCommonCloudSolrClient().getZkStateReader()
          .getClusterState().getCollection(COLL_NAME);
      // As of SOLR-5209 the last replica deletion no longer leads to
      // the deletion of the slice.
      final Slice slice = testcoll.getSlice(shard);
      final int actualNumReplicasRemaining = (slice == null ? 0 : slice.getReplicas().size());
      success = (actualNumReplicasRemaining == expectedNumReplicasRemaining);
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

