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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/** 
 * A very simple sanity check that Phrase Identification works across a cloud cluster
 * using distributed term stat collection.
 *
 * @see org.apache.solr.handler.component.PhrasesIdentificationComponentTest
 */
@Slow
public class TestCloudPhrasesIdentificationComponent extends SolrCloudTestCase {

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** One client per node */
  private static final ArrayList<HttpSolrClient> CLIENTS = new ArrayList<>(5);

  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {
    
    // multi replicas should not matter...
    final int repFactor = usually() ? 1 : 2;
    // ... but we definitely want to test multiple shards
    final int numShards = TestUtil.nextInt(random(), 1, (usually() ? 2 :3));
    final int numNodes = (numShards * repFactor);
   
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");
    
    configureCluster(numNodes).addConfig(configName, configDir).configure();
    
    Map<String, String> collectionProperties = new LinkedHashMap<>();
    collectionProperties.put("config", "solrconfig-phrases-identification.xml");
    collectionProperties.put("schema", "schema-phrases-identification.xml");
    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    waitForRecoveriesToFinish(CLOUD_CLIENT);

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      CLIENTS.add(getHttpSolrClient(jetty.getBaseUrl() + "/" + COLLECTION_NAME + "/"));
    }

    // index some docs...
    CLOUD_CLIENT.add
      (sdoc("id", "42",
            "title","Tale of the Brown Fox: was he lazy?",
            "body", "No. The quick brown fox was a very brown fox who liked to get into trouble."));
    CLOUD_CLIENT.add
      (sdoc("id", "43",
            "title","A fable in two acts",
            "body", "The brOwn fOx jumped. The lazy dog did not"));
    CLOUD_CLIENT.add
      (sdoc("id", "44",
            "title","Why the LazY dog was lazy",
            "body", "News flash: Lazy Dog was not actually lazy, it just seemd so compared to Fox"));
    CLOUD_CLIENT.add
      (sdoc("id", "45",
            "title","Why Are We Lazy?",
            "body", "Because we are. that's why"));
    CLOUD_CLIENT.commit();
  }

  @AfterClass
  private static void afterClass() throws Exception {
    if (null != CLOUD_CLIENT) {
      CLOUD_CLIENT.close();
      CLOUD_CLIENT = null;
    }
    for (HttpSolrClient client : CLIENTS) {
      client.close();
    }
    CLIENTS.clear();
  }

  public void testBasicPhrases() throws Exception {
    final String input = " did  a Quick    brown FOX perniciously jump over the lazy dog";
    final String expected = " did  a Quick    {brown FOX} perniciously jump over {the lazy dog}";
    
    // based on the documents indexed, these assertions should all pass regardless of
    // how many shards we have, or wether the request is done via /phrases or /select...
    for (String path : Arrays.asList("/select", "/phrases")) {
      // ... or if we muck with "q" and use the alternative phrases.q for the bits we care about...
      for (SolrParams p : Arrays.asList(params("q", input, "phrases", "true"),
                                        params("q", "*:*", "phrases.q", input, "phrases", "true"),
                                        params("q", "-*:*", "phrases.q", input, "phrases", "true"))) {
        final QueryRequest req = new QueryRequest(p);
        req.setPath(path);
        final QueryResponse rsp = req.process(getRandClient(random()));
        try {
          NamedList<Object> phrases = (NamedList<Object>) rsp.getResponse().get("phrases");
          assertEquals("input", input, phrases.get("input"));
          assertEquals("summary", expected, phrases.get("summary"));
          
          final List<NamedList<Object>> details = (List<NamedList<Object>>) phrases.get("details");
          assertNotNull("null details", details);
          assertEquals("num phrases found", 2, details.size());
          
          final NamedList<Object> lazy_dog = details.get(0);
          assertEquals("dog text", "the lazy dog", lazy_dog.get("text"));
          assertEquals("dog score", 0.166666D, ((Double)lazy_dog.get("score")).doubleValue(), 0.000001D);
          
          final NamedList<Object> brown_fox = details.get(1);
          assertEquals("fox text", "brown FOX", brown_fox.get("text"));
          assertEquals("fox score", 0.083333D, ((Double)brown_fox.get("score")).doubleValue(), 0.000001D);
          
        } catch (AssertionError e) {
          throw new AssertionError(e.getMessage() + " ::: " + path + " ==> " + rsp, e);
        }
      }
    }
  }

  public void testEmptyInput() throws Exception {
    // empty input shouldn't error, just produce empty results...
    for (String input : Arrays.asList("", "  ")) {
      for (SolrParams p : Arrays.asList(params("q", "*:*", "phrases.q", input, "phrases", "true"),
                                        params("q", "-*:*", "phrases.q", input, "phrases", "true"))) {
        final QueryRequest req = new QueryRequest(p);
        req.setPath("/phrases");
        final QueryResponse rsp = req.process(getRandClient(random()));
        try {
          NamedList<Object> phrases = (NamedList<Object>) rsp.getResponse().get("phrases");
          assertEquals("input", input, phrases.get("input"));
          assertEquals("summary", input, phrases.get("summary"));
          
          final List<NamedList<Object>> details = (List<NamedList<Object>>) phrases.get("details");
          assertNotNull("null details", details);
          assertEquals("num phrases found", 0, details.size());
          
        } catch (AssertionError e) {
          throw new AssertionError(e.getMessage() + " ==> " + rsp, e);
        }
      }
    }
  }

  /** 
   * returns a random SolrClient -- either a CloudSolrClient, or an HttpSolrClient pointed 
   * at a node in our cluster 
   */
  public static SolrClient getRandClient(Random rand) {
    int numClients = CLIENTS.size();
    int idx = TestUtil.nextInt(rand, 0, numClients);

    return (idx == numClients) ? CLOUD_CLIENT : CLIENTS.get(idx);
  }

  public static void waitForRecoveriesToFinish(CloudSolrClient client) throws Exception {
    assert null != client.getDefaultCollection();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(client.getDefaultCollection(),
                                                        client.getZkStateReader(),
                                                        true, true, 330);
  }

}
