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

package org.apache.solr.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.handler.TestBlobHandler;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlobRepositoryCloudTest extends SolrCloudTestCase {

  public static final Path TEST_PATH = getFile("solr/configsets").toPath();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)  // only sharing *within* a node
        .addConfig("configname", TEST_PATH.resolve("resource-sharing"))
        .configure();
//    Thread.sleep(2000);
    HashMap<String, String> params = new HashMap<>();
    CollectionAdminRequest.createCollection(".system", null, 1, 1)
        .process(cluster.getSolrClient());
    // test component will fail if it cant' find a blob with this data by this name
    TestBlobHandler.postData(cluster.getSolrClient(), findLiveNodeURI(), "testResource", ByteBuffer.wrap("foo,bar\nbaz,bam".getBytes(StandardCharsets.UTF_8)));
    //    Thread.sleep(2000);
    // if these don't load we probably failed to post the blob above
    CollectionAdminRequest.createCollection("col1", "configname", 1, 1)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("col2", "configname", 1, 1)
        .process(cluster.getSolrClient());

    SolrInputDocument document = new SolrInputDocument();
    document.addField("id", "1");
    document.addField("text", "col1");
    CloudSolrClient solrClient = cluster.getSolrClient();
    solrClient.add("col1", document);
    solrClient.commit("col1");
    document = new SolrInputDocument();
    document.addField("id", "1");
    document.addField("text", "col2");
    solrClient.add("col2", document);
    solrClient.commit("col2");
    Thread.sleep(2000);

  }

  @Test
  public void test() throws Exception {
    // This test relies on the installation of ResourceSharingTestComponent which has 2 useful properties:
    // 1. it will fail to initialize if it doesn't find a 2 line CSV like foo,bar\nbaz,bam thus validating
    //    that we are properly pulling data from the blob store
    // 2. It replaces any q for a query request to /select with "text:<name>" where <name> is the name
    //    of the last collection to run a query. It does this by caching a shared resource of type
    //    ResourceSharingTestComponent.TestObject, and the following sequence is proof that either
    //    collection can tell if it was (or was not) the last collection to issue a query by 
    //    consulting the shared object
    assertLastQueryNotToCollection("col1");
    assertLastQueryNotToCollection("col2");
    assertLastQueryNotToCollection("col1");
    assertLastQueryToCollection("col1");
    assertLastQueryNotToCollection("col2");
    assertLastQueryToCollection("col2");
  }

  // TODO: move this up to parent class?
  private static String findLiveNodeURI() {
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    return zkStateReader.getBaseUrlForNodeName(zkStateReader.getClusterState().getCollection(".system").getSlices().iterator().next().getLeader().getNodeName());
  }

  private void assertLastQueryToCollection(String collection) throws SolrServerException, IOException {
    assertEquals(1, getSolrDocuments(collection).size());
  }

  private void assertLastQueryNotToCollection(String collection) throws SolrServerException, IOException {
    assertEquals(0, getSolrDocuments(collection).size());
  }

  private SolrDocumentList getSolrDocuments(String collection) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery("*:*");
    CloudSolrClient client = cluster.getSolrClient();
    QueryResponse resp1 = client.query(collection, query);
    return resp1.getResults();
  }


}
