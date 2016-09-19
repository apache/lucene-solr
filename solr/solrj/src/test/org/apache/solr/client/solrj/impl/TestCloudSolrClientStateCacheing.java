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

package org.apache.solr.client.solrj.impl;

import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCloudSolrClientStateCacheing extends SolrCloudTestCase {

  private final String id = "id";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();
  }

  @Test
  public void testCacheInvalidationOnLeaderChange() throws Exception {

    final String collectionName = "cacheInvalidation";

    try (CloudSolrClient solrClient = new CloudSolrClient.Builder()
        .withZkHost(cluster.getZkServer().getZkAddress())
        .sendDirectUpdatesToShardLeadersOnly()
        .build()) {

      CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
          .process(solrClient);

      // send one update that will populate the client's cluster state cache
      new UpdateRequest()
          .add(id, "0", "a_t", "hello1")
          .add(id, "2", "a_t", "hello2")
          .add(id, "3", "a_t", "hello2")
          .commit(solrClient, collectionName);

      // take down a leader node
      JettySolrRunner leaderJetty = cluster.getLeaderJetty(collectionName, "shard1");
      leaderJetty.stop();

      // wait for a new leader to be elected
      solrClient.waitForState(collectionName, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
          (n, c) -> DocCollection.isUpdateable(n, c, 2));

      // send another update - this should still succeed, even though the client's
      // cached leader will be incorrect
      new UpdateRequest()
          .add(id, "4", "a_t", "hello1")
          .add(id, "5", "a_t", "hello2")
          .add(id, "6", "a_t", "hello2")
          .commit(solrClient, collectionName);

    }

  }

}
