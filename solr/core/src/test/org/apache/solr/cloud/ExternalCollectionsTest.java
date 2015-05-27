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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class ExternalCollectionsTest extends AbstractFullDistribZkTestBase {
  private CloudSolrClient client;

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

  public ExternalCollectionsTest() {
  }


  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    testZkNodeLocation();
    testConfNameAndCollectionNameSame();
  }



  @Override
  protected String getStateFormat() {
    return "2";
  }

  private void testConfNameAndCollectionNameSame() throws Exception{
    // .system collection precreates the configset

    createCollection(".system", client, 2, 1);
    waitForRecoveriesToFinish(".system", false);
  }

  private void testZkNodeLocation() throws Exception{

    String collectionName = "myExternColl";

    createCollection(collectionName, client, 2, 2);

    waitForRecoveriesToFinish(collectionName, false);
    assertTrue("does not exist collection state externally",
        cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.getCollectionPath(collectionName), true));
    Stat stat = new Stat();
    byte[] data = cloudClient.getZkStateReader().getZkClient().getData(ZkStateReader.getCollectionPath(collectionName), null, stat, true);
    DocCollection c = ZkStateReader.getCollectionLive(cloudClient.getZkStateReader(), collectionName);
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    assertEquals("The zkversion of the nodes must be same zkver:" + stat.getVersion() , stat.getVersion(),clusterState.getCollection(collectionName).getZNodeVersion() );
    assertTrue("DocCllection#getStateFormat() must be > 1", cloudClient.getZkStateReader().getClusterState().getCollection(collectionName).getStateFormat() > 1);


    // remove collection
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    if (client == null) {
      client = createCloudClient(null);
    }

    client.request(request);

    checkForMissingCollection(collectionName);
    assertFalse("collection state should not exist externally", cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.getCollectionPath(collectionName), true));

  }
}

