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

package org.apache.solr.client.solrj.request;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestV2Request extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("config", getFile("solrj/solr/collection1/conf").toPath())
        .configure();
  }

  public void assertSuccess(SolrClient client, V2Request request) throws IOException, SolrServerException {
    NamedList<Object> res = client.request(request);
    assertTrue("The request failed", res.get("responseHeader").toString().contains("status=0"));
  }

  @Test
  public void testIsCollectionRequest() {
    assertFalse(new V2Request.Builder("/collections").build().isPerCollectionRequest());
    assertFalse(new V2Request.Builder("/collections/a/shards").build().isPerCollectionRequest());
    assertFalse(new V2Request.Builder("/collections/a/shards/").build().isPerCollectionRequest());
    assertTrue(new V2Request.Builder("/collections/a/update").build().isPerCollectionRequest());
    assertEquals("a", new V2Request.Builder("/collections/a/update").build().getCollection());
    assertTrue(new V2Request.Builder("/c/a/update").build().isPerCollectionRequest());
    assertTrue(new V2Request.Builder("/c/a/schema").build().isPerCollectionRequest());
    assertFalse(new V2Request.Builder("/c/a").build().isPerCollectionRequest());
  }

  @Test
  public void testHttpSolrClient() throws Exception {
    HttpSolrClient solrClient = new HttpSolrClient.Builder(
        cluster.getJettySolrRunner(0).getBaseUrl().toString()).build();
    doTest(solrClient);
    solrClient.close();
  }

  @Test
  public void testCloudSolrClient() throws Exception {
    doTest(cluster.getSolrClient());
  }

  private void doTest(SolrClient client) throws IOException, SolrServerException {
    assertSuccess(client, new V2Request.Builder("/collections")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{" +
            "  'create' : {" +
            "    'name' : 'test'," +
            "    'numShards' : 2," +
            "    'replicationFactor' : 2," +
            "    'config' : 'config'" +
            "  }" +
            "}").build());
    assertSuccess(client, new V2Request.Builder("/c").build());
    assertSuccess(client, new V2Request.Builder("/c/_introspect").build());


    assertSuccess(client, new V2Request.Builder("/c/test/config")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{'create-requesthandler' : { 'name' : '/x', 'class': 'org.apache.solr.handler.DumpRequestHandler' , 'startup' : 'lazy'}}")
        .build());

    assertSuccess(client, new V2Request.Builder("/c/test").withMethod(SolrRequest.METHOD.DELETE).build());
    NamedList<Object> res = client.request(new V2Request.Builder("/c").build());
    List collections = (List) res.get("collections");
    assertEquals(0, collections.size());

  }

}
