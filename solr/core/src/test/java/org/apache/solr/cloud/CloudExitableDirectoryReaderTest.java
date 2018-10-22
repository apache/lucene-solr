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

import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/**
* Distributed test for {@link org.apache.lucene.index.ExitableDirectoryReader} 
*/
public class CloudExitableDirectoryReaderTest extends SolrCloudTestCase {

  private static final int NUM_DOCS_PER_TYPE = 20;
  private static final String sleep = "2";

  private static final String COLLECTION = "exitable";
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("exitable-directory").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 2, 1));
  }

  @Test
  public void test() throws Exception {
    indexDocs();
    doTimeoutTests();
  }

  public void indexDocs() throws Exception {
    int counter = 1;
    UpdateRequest req = new UpdateRequest();

    for(; (counter % NUM_DOCS_PER_TYPE) != 0; counter++ )
      req.add(sdoc("id", Integer.toString(counter), "name", "a" + counter));

    counter++;
    for(; (counter % NUM_DOCS_PER_TYPE) != 0; counter++ )
      req.add(sdoc("id", Integer.toString(counter), "name", "b" + counter));

    counter++;
    for(; counter % NUM_DOCS_PER_TYPE != 0; counter++ )
      req.add(sdoc("id", Integer.toString(counter), "name", "dummy term doc" + counter));

    req.commit(cluster.getSolrClient(), COLLECTION);
  }

  public void doTimeoutTests() throws Exception {
    assertPartialResults(params("q", "name:a*", "timeAllowed", "1", "sleep", sleep));

    /*
    query rewriting for NUM_DOCS_PER_TYPE terms should take less 
    time than this. Keeping it at 5 because the delaying search component delays all requests 
    by at 1 second.
     */
    int fiveSeconds = 5000;
    
    Integer timeAllowed = TestUtil.nextInt(random(), fiveSeconds, Integer.MAX_VALUE);
    assertSuccess(params("q", "name:a*", "timeAllowed", timeAllowed.toString()));

    assertPartialResults(params("q", "name:a*", "timeAllowed", "1", "sleep", sleep));

    timeAllowed = TestUtil.nextInt(random(), fiveSeconds, Integer.MAX_VALUE);
    assertSuccess(params("q", "name:b*", "timeAllowed",timeAllowed.toString()));

    // negative timeAllowed should disable timeouts
    timeAllowed = TestUtil.nextInt(random(), Integer.MIN_VALUE, -1); 
    assertSuccess(params("q", "name:b*", "timeAllowed",timeAllowed.toString()));

    assertSuccess(params("q","name:b*")); // no time limitation
  }

  /**
   * execute a request, verify that we get an expected error
   */
  public void assertPartialResults(ModifiableSolrParams p) throws Exception {
      QueryResponse rsp = cluster.getSolrClient().query(COLLECTION, p);
      assertEquals(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY+" were expected",
          true, rsp.getHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
  }
  
  public void assertSuccess(ModifiableSolrParams p) throws Exception {
    QueryResponse response = cluster.getSolrClient().query(COLLECTION, p);
    assertEquals("Wrong #docs in response", NUM_DOCS_PER_TYPE - 1, response.getResults().getNumFound());
  }
}

