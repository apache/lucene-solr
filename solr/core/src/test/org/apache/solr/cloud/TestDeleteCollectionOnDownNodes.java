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

import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDeleteCollectionOnDownNodes extends SolrCloudTestCase {

  @Before
  public void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .addConfig("conf2", configset("cloud-minimal"))
        .configure();
  }
  
  @After
  public void teardownCluster() throws Exception {
    shutdownCluster();
  }

  @Test
  public void deleteCollectionWithDownNodes() throws Exception {

    CollectionAdminRequest.createCollection("halfdeletedcollection2", "conf", 4, 3)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection("halfdeletedcollection2", 60, TimeUnit.SECONDS, 4, 12);
    
    // stop a couple nodes
    JettySolrRunner j1 = cluster.stopJettySolrRunner(cluster.getRandomJetty(random()));
    JettySolrRunner j2 = cluster.stopJettySolrRunner(cluster.getRandomJetty(random()));

    cluster.waitForJettyToStop(j1);
    cluster.waitForJettyToStop(j2);

    // delete the collection
    CollectionAdminRequest.deleteCollection("halfdeletedcollection2").process(cluster.getSolrClient());
    waitForState("Timed out waiting for collection to be deleted", "halfdeletedcollection2", (n, c) -> c == null);

    assertFalse("Still found collection that should be gone",
        cluster.getSolrClient().getZkStateReader().getClusterState().hasCollection("halfdeletedcollection2"));

  }
}
