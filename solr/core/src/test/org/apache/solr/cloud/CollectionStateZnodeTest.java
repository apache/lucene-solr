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

import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// MRM TODO: - speed this up - waits for zkwriter to see its own state after delete
public class CollectionStateZnodeTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4).formatZk(true)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }
  
  @After
  public void afterTest() throws Exception {

  }
  
  @Test
  public void testZkNodeLocation() throws Exception {

    String collectionName = "myExternColl";
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2).waitForFinalState(true)
        .process(cluster.getSolrClient());

    assertTrue("State Format 2 collection path does not exist",
        zkClient().exists(ZkStateReader.getCollectionPath(collectionName)));

    Stat stat = new Stat();
    zkClient().getData(ZkStateReader.getCollectionPath(collectionName), null, stat);

    // the state.json itself can be ahead of the local DocCollection version due to state updates filling it in
    try {
      cluster.getSolrClient().getZkStateReader().waitForState(collectionName, 3, TimeUnit.SECONDS, (liveNodes, collectionState) -> {
        if (collectionState == null) {
          return false;
        }
        if (collectionState.getZNodeVersion() != stat.getVersion() && !collectionState.getStateUpdates().get("_cs_ver_").equals(Integer.toString(stat.getVersion()))) {
          return false;
        }
        return true;
      });
    } catch (TimeoutException e) {
      fail("failed finding state in DocCollection that appears up to date with " + stat.getVersion());
    }


    // remove collection
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());

    assertFalse("collection state should not exist",
        zkClient().exists(ZkStateReader.getCollectionPath(collectionName)));

  }
}

