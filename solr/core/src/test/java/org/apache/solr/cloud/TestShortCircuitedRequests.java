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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class TestShortCircuitedRequests extends AbstractFullDistribZkTestBase {

  public TestShortCircuitedRequests() {
    schemaString = "schema15.xml";      // we need a string id
    super.sliceCount = 4;
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    waitForRecoveriesToFinish(false);
    assertEquals(4, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    index("id", "a!doc1");  // shard3
    index("id", "b!doc1");  // shard1
    index("id", "c!doc1");  // shard2
    index("id", "e!doc1");  // shard4
    commit();

    doQuery("a!doc1", "q", "*:*", ShardParams._ROUTE_, "a!"); // can go to any random node

    // query shard3 directly with _route_=a! so that we trigger the short circuited request path
    Replica shard3 = cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getLeader("shard3");
    String nodeName = shard3.getNodeName();
    SolrClient shard3Client = getClient(nodeName);
    QueryResponse response = shard3Client.query(new SolrQuery("*:*").add(ShardParams._ROUTE_, "a!").add(ShardParams.SHARDS_INFO, "true"));

    assertEquals("Could not find doc", 1, response.getResults().getNumFound());
    NamedList<?> sinfo = (NamedList<?>) response.getResponse().get(ShardParams.SHARDS_INFO);
    assertNotNull("missing shard info for short circuited request", sinfo);
  }
}
