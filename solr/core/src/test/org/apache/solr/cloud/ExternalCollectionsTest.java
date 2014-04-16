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

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.cloud.OverseerCollectionProcessor.MAX_SHARDS_PER_NODE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

public class ExternalCollectionsTest extends AbstractFullDistribZkTestBase {
  private CloudSolrServer client;

  @BeforeClass
  public static void beforeThisClass2() throws Exception {

  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
    System.setProperty("solr.xml.persist", "true");
    client = createCloudClient(null);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    client.shutdown();
  }

  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  public ExternalCollectionsTest() {
    fixShardCount = true;

    sliceCount = 2;
    shardCount = 4;

    checkCreatedVsState = false;
  }


  @Override
  public void doTest() throws Exception {
    testZkNodeLocation();
  }


  boolean externalColl = false;
  @Override
  public boolean useExternalCollections() {
    return externalColl;
  }

  private void testZkNodeLocation() throws Exception{
    externalColl=true;

    String collectionName = "myExternColl";

    createCollection(collectionName, client, 2, 2);

    waitForRecoveriesToFinish(collectionName, false);
    assertTrue("does not exist collection state externally", cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.getCollectionPath(collectionName), true));
    Stat stat = new Stat();
    cloudClient.getZkStateReader().getZkClient().getData(ZkStateReader.getCollectionPath(collectionName),null,stat,true);
    assertEquals("", cloudClient.getZkStateReader().getClusterState().getCollection(collectionName).getVersion(), stat.getVersion());
    assertTrue("DocCllection#isExternal() must be true", cloudClient.getZkStateReader().getClusterState().getCollection(collectionName).isExternal() );


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



