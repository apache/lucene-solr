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

import static java.util.Collections.singletonMap;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.core.TestDynamicLoading.getFileContent;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.handler.TestBlobHandler;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDynamicURP extends SolrCloudTestCase {


  private static final String COLLECTION = "testUrpColl";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    SolrZkClient zkClient = cluster.getSolrClient().getZkStateReader().getZkClient();
    String path = ZkStateReader.CONFIGS_ZKNODE + "/conf/solrconfig.xml";
    byte[] data = zkClient.getData(path, null, null, true);

    String solrconfigStr = new String(data, StandardCharsets.UTF_8);
    zkClient.setData(path, solrconfigStr.replace("</config>",
        "<updateRequestProcessorChain name=\"test_urp\" processor=\"testURP\" default=\"true\">\n" +
        "    <processor class=\"solr.RunUpdateProcessorFactory\"/>\n" +
        "  </updateRequestProcessorChain>\n" +
        "\n" +
        "  <updateProcessor class=\"runtimecode.TestURP\" name=\"testURP\" runtimeLib=\"true\"></updateProcessor>\n" +
        "</config>").getBytes(StandardCharsets.UTF_8), true );


    CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 1).process(cluster.getSolrClient());
    waitForState("", COLLECTION, clusterShape(3, 3));
  }



  @Test
  public void testUrp() throws Exception {

    ByteBuffer jar = getFileContent("runtimecode/runtimeurp.jar.bin");

    String blobName = "urptest";
    TestBlobHandler.postAndCheck(cluster.getSolrClient(), cluster.getRandomJetty(random()).getBaseUrl().toString(),
        blobName, jar, 1);

    new V2Request.Builder("/c/" + COLLECTION + "/config")
        .withPayload(singletonMap("add-runtimelib", (MapWriter) ew1 -> ew1
            .put("name", blobName)
            .put("version", "1")))
        .withMethod(POST)
        .build()
        .process(cluster.getSolrClient());
    TestSolrConfigHandler.testForResponseElement(null,
        cluster.getRandomJetty(random()).getBaseUrl().toString(),
        "/"+COLLECTION+"/config/overlay",
        cluster.getSolrClient(),
        Arrays.asList("overlay", "runtimeLib", blobName, "version")
        ,"1",10);

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "123");
    doc.addField("name_s", "Test URP");
    new UpdateRequest()
        .add(doc)
        .commit(cluster.getSolrClient(), COLLECTION);
    QueryResponse result = cluster.getSolrClient().query(COLLECTION, new SolrQuery("id:123"));
    assertEquals(1, result.getResults().getNumFound());
    Object time_s = result.getResults().get(0).getFirstValue("time_s");
    assertNotNull(time_s);



  }

}
