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
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDeleteCollectionOnDownNodes extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .addConfig("conf2", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void deleteCollectionWithDownNodes() throws Exception {

    CollectionAdminRequest.createCollection("halfdeletedcollection2", "conf", 4, 3)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    // stop a couple nodes
    cluster.stopJettySolrRunner(cluster.getRandomJetty(random()));
    cluster.stopJettySolrRunner(cluster.getRandomJetty(random()));

    // wait for leaders to settle out
    waitForState("Timed out waiting for leader elections", "halfdeletedcollection2", (n, c) -> {
      for (Slice slice : c) {
        if (slice.getLeader() == null)
          return false;
        if (slice.getLeader().isActive(n) == false)
          return false;
      }
      return true;
    });

    // delete the collection
    CollectionAdminRequest.deleteCollection("halfdeletedcollection2").process(cluster.getSolrClient());
    waitForState("Timed out waiting for collection to be deleted", "halfdeletedcollection2", (n, c) -> c == null);

    assertFalse("Still found collection that should be gone",
        cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("halfdeletedcollection2"));

  }
}
