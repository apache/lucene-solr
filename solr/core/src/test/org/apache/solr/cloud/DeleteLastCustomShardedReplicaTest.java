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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteLastCustomShardedReplicaTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void test() throws Exception {

    final String collectionName = "customcollreplicadeletion";

    CollectionAdminRequest.createCollectionWithImplicitRouter(collectionName, "conf", "a,b", 1)
        .setMaxShardsPerNode(5)
        .process(cluster.getSolrClient());

    DocCollection collectionState = getCollectionState(collectionName);
    Replica replica = getRandomReplica(collectionState.getSlice("a"));

    CollectionAdminRequest.deleteReplica(collectionName, "a", replica.getName())
        .process(cluster.getSolrClient());

    waitForState("Expected shard 'a' to have no replicas", collectionName, (n, c) -> {
      return c.getSlice("a") == null || c.getSlice("a").getReplicas().size() == 0;
    });

  }

}

