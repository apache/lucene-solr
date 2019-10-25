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
package org.apache.solr.search.function;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;


/**
 *
 * @see TestSortByMinMaxFunction
 **/
public class SortByFunctionTest extends SolrCloudTestCase {

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";

  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** One client per node */
  private static ArrayList<HttpSolrClient> CLIENTS = new ArrayList<>(5);
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // multi replicas should not matter...
    final int repFactor = usually() ? 1 : 2;
    // ... but we definitely want to test multiple shards
    final int numShards = TestUtil.nextInt(random(), 1, (usually() ? 2 :3));
    final int numNodes = (numShards * repFactor);
   
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");
    
    configureCluster(numNodes).addConfig(configName, configDir).configure();
    
    Map<String, String> collectionProperties = new LinkedHashMap<>();
    collectionProperties.put("config", "solrconfig.xml");
    collectionProperties.put("schema", "schema.xml");
    collectionProperties.put("solr.test.sys.prop1", "unused");
    collectionProperties.put("solr.test.sys.prop2", "unused");
    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    waitForRecoveriesToFinish(CLOUD_CLIENT);

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      CLIENTS.add(getHttpSolrClient(jetty.getBaseUrl() + "/" + COLLECTION_NAME + "/"));
    }
  }
  
  @AfterClass
  private static void afterClass() throws Exception {
    CLOUD_CLIENT.close(); CLOUD_CLIENT = null;
    for (HttpSolrClient client : CLIENTS) {
      client.close();
    }
    CLIENTS = null;
  }

  @Before
  public void clearCollection() throws Exception {
    assertNull(CLOUD_CLIENT.deleteByQuery("*:*").getException());
    assertNull(CLOUD_CLIENT.commit().getException());
  }

  /** macro */
  public void assertAdd(SolrInputDocument doc) throws Exception {
    assertNull(getRandClient(random()).add(doc).getException());
  }
  /** macro */
  public void assertCommit() throws Exception {
    assertNull(getRandClient(random()).commit().getException());
  }

  /** 
   * given query params and a list of uniqueKey values, asserts that documents 
   * returned by the request match the list of ids and are in the specified order.
   */
  public void assertSortedResults(SolrParams p, String... ids) throws Exception {
    QueryResponse rsp = getRandClient(random()).query(p);
    assertNull(rsp.getException());
    SolrDocumentList results = rsp.getResults();
    assertEquals(ids.length, results.size());
    int i = 0;
    for (String id : ids) {
      assertEquals("#" + i, id, results.get(i).getFieldValue("id"));
      i++;
    }
  }
  
  public void test() throws Exception {
    assertAdd(sdoc("id", "1", "x_td1", "0", "y_td1", "2", "w_td1", "25", "z_td1", "5", "f_t", "ipod"));
    assertAdd(sdoc("id", "2", "x_td1", "2", "y_td1", "2", "w_td1", "15", "z_td1", "5", "f_t", "ipod ipod ipod ipod ipod"));
    assertAdd(sdoc("id", "3", "x_td1", "3", "y_td1", "2", "w_td1", "55", "z_td1", "5", "f_t", "ipod ipod ipod ipod ipod ipod ipod ipod ipod"));
    assertAdd(sdoc("id", "4", "x_td1", "4", "y_td1", "2", "w_td1", "45", "z_td1", "5", "f_t", "ipod ipod ipod ipod ipod ipod ipod"));
    assertCommit();

    assertSortedResults(params("fl", "id,score", "q", "f_t:ipod", "sort", "score desc"),
                        "1", "2", "3", "4");

    assertSortedResults(params("fl", "*,score", "q", "*:*", "sort", "sum(x_td1, y_td1) desc"),
                        "4", "3", "2", "1");

    assertSortedResults(params("fl", "*,score", "q", "*:*", "sort", "sum(x_td1, y_td1) asc"),
                        "1", "2", "3", "4");

    //the function is equal, w_td1 separates
    assertSortedResults(params("q", "*:*", "fl", "id", "sort", "sum(z_td1, y_td1) asc, w_td1 asc"),
                        "2", "1", "4", "3");

  }
  
  public void testSortJoinDocFreq() throws Exception
  {
    assertAdd(sdoc("id", "4", "id_s1", "D", "links_mfacet", "A", "links_mfacet", "B", "links_mfacet", "C" ) );
    assertAdd(sdoc("id", "3", "id_s1", "C", "links_mfacet", "A", "links_mfacet", "B" ) );
    assertCommit(); // Make sure it uses two readers
    assertAdd(sdoc("id", "2", "id_s1", "B", "links_mfacet", "A" ) );
    assertAdd(sdoc("id", "1", "id_s1", "A"  ) );
    assertCommit();

    assertSortedResults(params("q", "links_mfacet:B", "fl", "id", "sort", "id asc"),
                        "3", "4");
    
    assertSortedResults(params("q", "*:*", "fl", "id", "sort", "joindf(id_s1, links_mfacet) desc"),
                        "1", "2", "3", "4");

    assertSortedResults(params("q", "*:*", "fl", "id", "sort", "joindf(id_s1, links_mfacet) asc"),
                        "4", "3", "2", "1");
  }

  // nocommit: test explicit "sort=field(multivalued_str_field,min|max) asc|desc"
  // nocommit: test implicit "sort=multivalued_str_field asc|desc"
  
  /**
   * The sort clauses to test in <code>testFieldSortSpecifiedAsFunction</code>.
   *
   * @see #testFieldSortSpecifiedAsFunction
   */
  protected String[] getFieldFunctionClausesToTest() {
    return new String[] { "primary_tl1", "field(primary_tl1)" };
  }

  /**
   * Sort by function normally compares the double value, but if a function is specified that identifies
   * a single field, we should use the underlying field's SortField to save of a lot of type converstion 
   * (and RAM), and keep the sort precision as high as possible
   *
   * @see #getFieldFunctionClausesToTest
   */
  public void testFieldSortSpecifiedAsFunction() throws Exception {
    final long A = Long.MIN_VALUE;
    final long B = A + 1L;
    final long C = B + 1L;
    
    final long Z = Long.MAX_VALUE;
    final long Y = Z - 1L;
    final long X = Y - 1L;
    
    // test is predicated on the idea that if long -> double converstion is happening under the hood
    // then we lose precision in sorting; so lets sanity check that our JVM isn't doing something wacky
    // in converstion that violates the principle of the test
    
    assertEquals("WTF? small longs cast to double aren't equivalent?",
                 (double)A, (double)B, 0.0D);
    assertEquals("WTF? small longs cast to double aren't equivalent?",
                 (double)A, (double)C, 0.0D);
    
    assertEquals("WTF? big longs cast to double aren't equivalent?",
                 (double)Z, (double)Y, 0.0D);
    assertEquals("WTF? big longs cast to double aren't equivalent?",
                 (double)Z, (double)X, 0.0D);
    
    for (int i = 1; i <= 3; i++) {
      // inconsistent id structure to ensure lexigraphic/numeric id sort isn't a factor
      assertAdd(sdoc("id", "99" + i, "primary_tl1", X, "secondary_tl1", i,
                     "multi_l_dv", X, "multi_l_dv", A));
      assertAdd(sdoc("id", i + "55", "primary_tl1", Y, "secondary_tl1", i,
                     "multi_l_dv", Y, "multi_l_dv", B));
      assertAdd(sdoc("id", "6" + i + "6", "primary_tl1", Z, "secondary_tl1", i,
                     "multi_l_dv", Z, "multi_l_dv", C));
    }
    assertCommit();

    // all of these sorts should result in the exact same order
    // min/max of a field is tested in TestSortByMinMaxFunction
    for (String primarySort : getFieldFunctionClausesToTest()) {
      assertSortedResults(params("q", "*:*",
                                 "sort", primarySort + " asc, secondary_tl1 asc"),
                          "991", "992", "993",
                          "155", "255", "355",
                          "616", "626", "636");
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
