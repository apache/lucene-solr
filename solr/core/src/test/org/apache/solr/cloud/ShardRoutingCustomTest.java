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
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShardRoutingCustomTest extends SolrCloudBridgeTestCase {

  @BeforeClass
  public static void beforeShardHashingTest() throws Exception {
    System.setProperty("solr.suppressDefaultConfigBootstrap", "false");
    useFactory(null);
  }

  public ShardRoutingCustomTest() {
    schemaString = "schema15.xml";      // we need a string id
    solrconfigString = "solrconfig.xml";
    uploadSelectCollection1Config = true;
    createCollection1 = false;
    sliceCount = 0;
  }

  @Test
  public void test() throws Exception {
    doCustomSharding();
  }

  private void doCustomSharding() throws Exception {

    assertEquals(0, CollectionAdminRequest
        .createCollection(COLLECTION, "_default", 1, 1)
        .setCreateNodeSet(ZkStateReader.CREATE_NODE_SET_EMPTY)
        .process(cloudClient).getStatus());
    assertTrue(CollectionAdminRequest
        .addReplicaToShard(COLLECTION,"s1")
        .setNode(cluster.getJettySolrRunner(0).getNodeName())
        .setType(useTlogReplicas() ? Replica.Type.TLOG: Replica.Type.NRT)
        .process(cloudClient).isSuccess());
  }

  protected boolean useTlogReplicas() {
    return false;
    // MRM TODO: - a TLog replica can end up configured with no update log
    //return random().nextBoolean();
  }

}
