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

import java.io.File;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShardRoutingCustomTest extends AbstractFullDistribZkTestBase {

  String collection = DEFAULT_COLLECTION;  // enable this to be configurable (more work needs to be done)

  @BeforeClass
  public static void beforeShardHashingTest() throws Exception {
    useFactory(null);
  }

  public ShardRoutingCustomTest() {
    schemaString = "schema15.xml";      // we need a string id
    sliceCount = 0;
  }

  @Test
  public void test() throws Exception {
    boolean testFinished = false;
    try {
      doCustomSharding();

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayout();
      }
    }
  }

  private void doCustomSharding() throws Exception {
    printLayout();

  

    File jettyDir = createTempDir("jetty").toFile();
    jettyDir.mkdirs();
    setupJettySolrHome(jettyDir);
    JettySolrRunner j = createJetty(jettyDir, createTempDir().toFile().getAbsolutePath(), "shardA", "solrconfig.xml", null);
    j.start();
    assertEquals(0, CollectionAdminRequest
        .createCollection(DEFAULT_COLLECTION, "conf1", 1, 1)
        .setStateFormat(Integer.parseInt(getStateFormat()))
        .setCreateNodeSet("")
        .process(cloudClient).getStatus());
    assertTrue(CollectionAdminRequest
        .addReplicaToShard(collection,"shard1")
        .setNode(j.getNodeName())
        .setType(useTlogReplicas()? Replica.Type.TLOG: Replica.Type.NRT)
        .process(cloudClient).isSuccess());
    jettys.add(j);
    SolrClient client = createNewSolrClient(j.getLocalPort());
    clients.add(client);

    waitForActiveReplicaCount(cloudClient, DEFAULT_COLLECTION, 1);

    updateMappingsFromZk(this.jettys, this.clients);

    printLayout();
  }


}
